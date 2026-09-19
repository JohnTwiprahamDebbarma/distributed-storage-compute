"""
Core Raft consensus engine

Implements leader election, log replication, and snapshotting. This module is pure Raft logic; the gRPC plumbing lives in raft_server.py.
"""

import json
import logging
import os
import random
import threading
import time

logger = logging.getLogger("raft")

# Constants
ELECTION_TIMEOUT_MIN = 2.0   # seconds  (longer for local testing visibility)
ELECTION_TIMEOUT_MAX = 4.0
HEARTBEAT_INTERVAL   = 0.5   # seconds


class RaftNode:
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"

    def __init__(self, node_id: int, peers: dict, persist_dir: str,
                 apply_fn=None):
        self.node_id= node_id
        self.peers= peers          # {id: "host:port"}
        self.persist_dir = persist_dir
        self.apply_fn= apply_fn

        os.makedirs(persist_dir, exist_ok=True)

        # Persistent state (must survive crashes)
        self.current_term = 0
        self.voted_for = None
        self.log = []   # list of dicts with keys matching LogEntry

        # Volatile state
        self.commit_index = 0   # highest log index known to be committed
        self.last_applied = 0   # highest log index applied to state machine

        self.state= self.FOLLOWER
        self.leader_id = None

        # Leader-only volatile state
        self.next_index = {}   # peer_id -> next index to send
        self.match_index = {}   # peer_id -> highest index replicated

        # Concurrency
        self.lock= threading.RLock()
        self._election_timer = None
        self._hb_thread = None
        self._applier_thread = None
        self._apply_cv = threading.Condition(self.lock)

        # Waiting client RPC futures
        # log_index -> waiter dict {term, client_id, seq_num, event, result}
        # Each waiter records the identity of the entry the caller appended, so a
        # commit at the same index for a DIFFERENT entry (after this node loses
        # leadership and a new leader overwrites the slot) is never mistaken for
        # the caller's own write. See _fail_all_waiters and the applier.
        self._commit_waiters = {}

        # gRPC stubs (populated lazily by raft_server.py)
        self.peer_stubs = {}   # {peer_id: RaftServiceStub}

        # Fault injection: while isolated this node neither sends nor accepts
        # Raft traffic, as if its network cable were pulled (see set_isolated)
        self.isolated = False
        self._heal_timer = None

        # Restore durable state then start timers
        self._load_persistent_state()
        self._start_applier()
        self._reset_election_timer()

    # Public API
    def is_leader(self) -> bool:
        with self.lock:
            return self.state == self.LEADER

    def get_leader_info(self):
        """Returns (leader_id, leader_address_string) or (None, None)."""
        with self.lock:
            lid = self.leader_id
        if lid is None:
            return None, None
        if lid == self.node_id:
            return lid, None   # caller knows own address
        addr = self.peers.get(lid)
        return lid, addr

    def status(self) -> dict:
        """A consistent snapshot of this node's Raft state (dashboards, tests)."""
        with self.lock:
            return {
                "node_id": self.node_id,
                "state": self.state,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "last_log_index": self._last_log_index(),
                "last_log_term": self._last_log_term(),
                "isolated": self.isolated,
                "match_index": dict(self.match_index) if self.state == self.LEADER else {},
            }

    def set_isolated(self, isolated: bool, heal_after: float = 0.0):
        """Cut this node off from the cluster (fault injection), or reconnect it.

        While isolated the node drops every outgoing Raft RPC, and the gRPC layer
        rejects incoming ones, so the rest of the cluster sees it as crashed even
        though it keeps running. heal_after > 0 reconnects it automatically.
        """
        with self.lock:
            self.isolated = isolated
            if self._heal_timer is not None:
                self._heal_timer.cancel()
                self._heal_timer = None
            if isolated and heal_after > 0:
                self._heal_timer = threading.Timer(heal_after, self.set_isolated, args=(False,))
                self._heal_timer.daemon = True
                self._heal_timer.start()
        logger.info(f"[{self.node_id}] {'ISOLATED' if isolated else 'Reconnected'}")

    def append_entry(self, op_type: str, filename: str, data: bytes,
                     client_id: str, seq_num: int, timeout: float = 10.0):
        with self.lock:
            if self.state != self.LEADER:
                raise NotLeaderError(self.leader_id,self.peers.get(self.leader_id))

            # Build log entry
            idx = self._last_log_index() + 1
            entry = {
                "term":self.current_term,
                "index": idx,
                "op_type": op_type,
                "filename":filename,
                "data": data.hex() if isinstance(data, bytes) else "",
                "client_id": client_id,
                "seq_num": seq_num,
            }
            self.log.append(entry)
            self._persist_log()

            # Update own match/next
            self.match_index[self.node_id] = idx
            self.next_index[self.node_id] = idx + 1

            waiter = {
                "term": self.current_term,
                "client_id": client_id,
                "seq_num": seq_num,
                "event": threading.Event(),
                "result": None,
            }
            self._commit_waiters[idx] = waiter

        logger.info(f"[{self.node_id}] Appended entry idx={idx} op={op_type} "f"file={filename}")

        # Trigger immediate replication to all peers
        self._send_append_entries_all()

        # Wait for THIS entry to commit. The waiter is resolved either by the
        # applier (when our own entry commits at idx) or by _fail_all_waiters
        # (when we lose leadership). We hold `waiter` by reference, so the result
        # reaches us even after it is removed from the pending map.
        if not waiter["event"].wait(timeout=timeout):
            with self.lock:
                if self._commit_waiters.get(idx) is waiter:
                    self._commit_waiters.pop(idx, None)
            raise TimeoutError(f"Entry {idx} not committed within {timeout}s")

        result = waiter["result"]
        if isinstance(result, Exception):
            raise result
        return result

    # RPC handlers (called by gRPC servicer in raft_server.py)
    def handle_request_vote(self, args: dict) -> dict:
        with self.lock:
            term = args["term"]
            candidate_id = args["candidate_id"]
            last_log_idx = args["last_log_index"]
            last_log_term= args["last_log_term"]

            # If we see a higher term, revert to follower
            if term > self.current_term:
                self._become_follower(term)

            vote_granted = False
            if (term >= self.current_term and
                    (self.voted_for is None or self.voted_for == candidate_id) and
                    self._candidate_log_up_to_date(last_log_idx, last_log_term)):
                vote_granted = True
                self.voted_for = candidate_id
                self._persist_hard_state()
                self._reset_election_timer()   # reset on granting vote

            logger.info(f"[{self.node_id}] RequestVote from {candidate_id} "
                        f"term={term} -> granted={vote_granted}")
            return {"term": self.current_term, "vote_granted": vote_granted}

    def handle_append_entries(self, args: dict) -> dict:
        with self.lock:
            term= args["term"]
            leader_id = args["leader_id"]
            prev_idx= args["prev_log_index"]
            prev_term = args["prev_log_term"]
            entries = args["entries"]
            ldr_commit = args["leader_commit"]

            conflict_index = 0
            conflict_term = 0

            # Reject stale leaders
            if term < self.current_term:
                return {"term": self.current_term, "success": False,
                        "conflict_index": 0, "conflict_term": 0}

            # Valid leader - reset timer and update state
            if term > self.current_term:
                self._become_follower(term)
            elif self.state == self.CANDIDATE:
                self._become_follower(term)

            self.leader_id = leader_id
            self._reset_election_timer()

            # Check prev log consistency
            if prev_idx > 0:
                if self._last_log_index() < prev_idx:
                    # We are missing entries
                    conflict_index = self._last_log_index() + 1
                    return {"term": self.current_term, "success": False,"conflict_index": conflict_index,"conflict_term": 0}

                existing_term = self.log[prev_idx - 1]["term"]
                if existing_term != prev_term:
                    # Conflict: find first index of conflicting term
                    conflict_term = existing_term
                    ci = prev_idx
                    while ci > 1 and self.log[ci - 2]["term"] == conflict_term:
                        ci -= 1
                    conflict_index = ci
                    return {"term": self.current_term, "success": False,
                            "conflict_index": conflict_index,
                            "conflict_term": conflict_term}

            # Append new entries, deleting any conflicting ones
            insert_at = prev_idx   # 0-based log position
            for entry in entries:
                pos = insert_at   # position in log list
                if pos < len(self.log):
                    if self.log[pos]["term"] != entry["term"]:
                        # Conflict: truncate from here
                        self.log = self.log[:pos]
                    else:
                        insert_at += 1
                        continue   # already have this entry
                self.log.append(entry)
                insert_at += 1

            if entries:
                self._persist_log()

            # Update commit index
            if ldr_commit > self.commit_index:
                self.commit_index = min(ldr_commit, self._last_log_index())
                self._apply_cv.notify_all()

            return {"term": self.current_term, "success": True,
                    "conflict_index": 0, "conflict_term": 0}

    def handle_install_snapshot(self, args: dict) -> dict:
        with self.lock:
            term = args["term"]
            if term > self.current_term:
                self._become_follower(term)
            self.leader_id = args["leader_id"]
            self._reset_election_timer()

            snapshot_data = args["snapshot_data"]

        # Apply snapshot outside lock
        if self.apply_fn:
            try:
                self.apply_fn({"op_type": "_snapshot",
                               "snapshot_data": snapshot_data})
            except Exception as e:
                logger.error(f"[{self.node_id}] Snapshot apply failed: {e}")

        with self.lock:
            last_idx = args["last_included_index"]
            last_term = args["last_included_term"]
            # Discard log entries covered by snapshot
            if last_idx <= self._last_log_index():
                self.log = self.log[last_idx:]
            else:
                self.log = [{"term": last_term, "index": last_idx,
                             "op_type": "noop", "filename": "", "data": "",
                             "client_id": "", "seq_num": 0}]
            self.commit_index = max(self.commit_index, last_idx)
            self.last_applied = max(self.last_applied, last_idx)
            self._persist_log()
            return {"term": self.current_term}

    # Election
    def _reset_election_timer(self):
        #Restart the randomised election timeout.
        if self._election_timer is not None:
            self._election_timer.cancel()
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self._election_timer = threading.Timer(timeout, self._start_election)
        self._election_timer.daemon = True
        self._election_timer.start()

    def _start_election(self):
        # Transitionto CANDIDATE and request votes.
        with self.lock:
            if self.state == self.LEADER:
                return
            self.state = self.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id= None
            self._persist_hard_state()
            term = self.current_term
            last_log_idx = self._last_log_index()
            last_log_term = self._last_log_term()
            votes_needed = (len(self.peers) + 1) // 2 + 1  # majority

        logger.info(f"[{self.node_id}] Starting election for term {term} "f"(need {votes_needed} votes)")

        votes = [1]   # vote for self
        vote_lk = threading.Lock()
        won_ev = threading.Event()

        def request_vote_from(peer_id, stub):
            nonlocal votes
            if self.isolated:
                return   # fault injection: the request never leaves this node
            try:
                import raft_pb2
                req = raft_pb2.RequestVoteArgs(
                    term=term,
                    candidate_id=self.node_id,
                    last_log_index=last_log_idx,
                    last_log_term=last_log_term,
                )
                reply = stub.RequestVote(req, timeout=2.0)
                with self.lock:
                    if reply.term > self.current_term:
                        self._become_follower(reply.term)
                        return
                if reply.vote_granted:
                    with vote_lk:
                        votes[0] += 1
                        if votes[0] >= votes_needed:
                            won_ev.set()
            except Exception as e:
                logger.debug(f"[{self.node_id}] Vote req to {peer_id} failed: {e}")

        threads = []
        for pid, stub in self.peer_stubs.items():
            t = threading.Thread(target=request_vote_from,
                                 args=(pid, stub), daemon=True)
            t.start()
            threads.append(t)

        # Wait up to election timeout for votes
        won_ev.wait(timeout=ELECTION_TIMEOUT_MIN)

        with self.lock:
            if self.state != self.CANDIDATE or self.current_term != term:
                return   # something changed (e.g. saw higher term)
            with vote_lk:
                if votes[0] >= votes_needed:
                    self._become_leader()
                else:
                    logger.info(f"[{self.node_id}] Lost election term={term} "
                                f"votes={votes[0]}/{votes_needed}")
                    self._reset_election_timer()

    # Leader behaviour
    def _become_leader(self):
        #Must be called with self.lock held.
        self.state     = self.LEADER
        self.leader_id = self.node_id
        last_idx       = self._last_log_index()
        for pid in self.peers:
            self.next_index[pid]  = last_idx + 1
            self.match_index[pid] = 0
        self.match_index[self.node_id] = last_idx

        # Append a no-op to commit entries from previous terms
        noop = {"term": self.current_term, "index": last_idx + 1,
                "op_type": "noop", "filename": "", "data": "",
                "client_id": "", "seq_num": 0}
        self.log.append(noop)
        self._persist_log()
        self.match_index[self.node_id] = last_idx + 1
        self.next_index[self.node_id] = last_idx + 2

        logger.info(f"[{self.node_id}] *** BECAME LEADER term={self.current_term} ***")

        if self._election_timer:
            self._election_timer.cancel()
        self._start_heartbeat()

    def _start_heartbeat(self):
        def hb_loop():
            while True:
                with self.lock:
                    if self.state != self.LEADER:
                        return
                self._send_append_entries_all()
                time.sleep(HEARTBEAT_INTERVAL)

        t = threading.Thread(target=hb_loop, daemon=True, name="raft-hb")
        t.start()
        self._hb_thread = t

    def _send_append_entries_all(self):
        for pid, stub in self.peer_stubs.items():
            t = threading.Thread(target=self._send_append_entries_to,
                                 args=(pid, stub), daemon=True)
            t.start()

    def _send_append_entries_to(self, peer_id: int, stub):
        with self.lock:
            # An isolated leader keeps believing it leads, but its heartbeats and
            # entries never arrive, so the followers time out and elect a new one.
            if self.state != self.LEADER or self.isolated:
                return
            next_idx  = self.next_index.get(peer_id, 1)
            prev_idx  = next_idx - 1
            prev_term = self.log[prev_idx - 1]["term"] if prev_idx > 0 else 0
            entries= self.log[prev_idx:]    # everything from next_idx
            commit = self.commit_index
            term = self.current_term

        import raft_pb2

        # Build protobuf entries
        pb_entries = []
        for e in entries:
            raw = bytes.fromhex(e["data"]) if e.get("data") else b""
            pb_entries.append(raft_pb2.LogEntry(
                term=e["term"], index=e["index"],
                op_type=e["op_type"], filename=e.get("filename", ""),
                data=raw,
                client_id=e.get("client_id", ""), seq_num=e.get("seq_num", 0),
            ))

        req = raft_pb2.AppendEntriesArgs(
            term=term, leader_id=self.node_id,
            prev_log_index=prev_idx, prev_log_term=prev_term,
            entries=pb_entries, leader_commit=commit,
        )

        try:
            reply = stub.AppendEntries(req, timeout=2.0)
        except Exception as e:
            logger.debug(f"[{self.node_id}] AE to {peer_id} failed: {e}")
            return

        with self.lock:
            if reply.term > self.current_term:
                self._become_follower(reply.term)
                return
            if self.state != self.LEADER:
                return

            if reply.success:
                new_match = prev_idx + len(entries)
                if new_match > self.match_index.get(peer_id, 0):
                    self.match_index[peer_id] = new_match
                    self.next_index[peer_id] = new_match + 1
                self._advance_commit_index()
            else:
                # Back up next_index using conflict hint
                ci = reply.conflict_index
                ct = reply.conflict_term
                if ct > 0:
                    # Find last entry in our log with conflict_term
                    found = 0
                    for i in range(len(self.log) - 1, -1, -1):
                        if self.log[i]["term"] == ct:
                            found = i + 1
                            break
                    self.next_index[peer_id] = found + 1 if found else ci
                else:
                    self.next_index[peer_id] = max(1, ci)

    def _advance_commit_index(self):
        """
        Check if any new log index can be committed (majority replicated).
        Must be called with self.lock held.
        """
        n = len(self.peers) + 1  # total cluster size
        majority = n // 2 + 1

        for idx in range(self._last_log_index(), self.commit_index, -1):
            count = sum(1 for mid in self.match_index.values() if mid >= idx)
            if (count >= majority and
                    idx <= len(self.log) and
                    self.log[idx - 1]["term"] == self.current_term):
                self.commit_index = idx
                self._apply_cv.notify_all()
                logger.info(f"[{self.node_id}] Committed up to index={idx}")
                break

    # Follower / term helpers
    def _become_follower(self, term: int):
        """Must be called with self.lock held."""
        self.state= self.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self._persist_hard_state()
        self._reset_election_timer()
        # Once we are no longer leader we can neither guarantee nor observe the
        # commit of entries we appended, so any pending client calls must fail and
        # retry against the new leader rather than be told (falsely) they succeeded.
        self._fail_all_waiters(NotLeaderError(None, None))

    def _fail_all_waiters(self, exc: Exception):
        """Resolve every pending client waiter with `exc` (call with self.lock held).

        Invoked when we stop being leader: entries we appended may be overwritten
        by a future leader, so we must not leave a client blocked on -- or later
        signal success for -- a log slot that no longer holds its entry.
        """
        for waiter in self._commit_waiters.values():
            if waiter["result"] is None:
                waiter["result"] = exc
            waiter["event"].set()
        self._commit_waiters.clear()

    def _candidate_log_up_to_date(self, cand_last_idx: int,cand_last_term: int) -> bool:
        my_last_term = self._last_log_term()
        my_last_idx = self._last_log_index()
        if cand_last_term != my_last_term:
            return cand_last_term > my_last_term
        return cand_last_idx >= my_last_idx

    # Log helpers
    def _last_log_index(self) -> int:
        return self.log[-1]["index"] if self.log else 0

    def _last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else 0

    # Applier thread - applies committed entries to the state machine
    def _start_applier(self):
        def applier_loop():
            while True:
                with self._apply_cv:
                    while self.last_applied >= self.commit_index:
                        self._apply_cv.wait()
                    to_apply = []
                    while self.last_applied < self.commit_index:
                        self.last_applied += 1
                        idx = self.last_applied
                        if 1 <= idx <= len(self.log):
                            to_apply.append((idx, self.log[idx - 1].copy()))

                for idx, entry in to_apply:
                    result = None
                    if entry["op_type"] != "noop" and self.apply_fn:
                        try:
                            # Convert hex-encoded data back to bytes
                            entry["data"] = (bytes.fromhex(entry["data"]) if entry.get("data") else b"")
                            result = self.apply_fn(entry)
                        except Exception as e:
                            logger.error(f"[{self.node_id}] Apply failed "f"idx={idx}: {e}")
                            result = e

                    with self.lock:
                        waiter = self._commit_waiters.get(idx)
                        if waiter is not None:
                            # Only hand the result back to the caller if the entry
                            # that actually committed at this index is the one they
                            # appended. A mismatch means a new leader overwrote the
                            # slot, so the caller's write was lost -> tell it to retry.
                            same = (waiter["term"] == entry["term"] and
                                    waiter["client_id"] == entry.get("client_id", "") and
                                    waiter["seq_num"] == entry.get("seq_num", 0))
                            waiter["result"] = result if same else NotLeaderError(
                                self.leader_id, self.peers.get(self.leader_id))
                            waiter["event"].set()
                            self._commit_waiters.pop(idx, None)

        t = threading.Thread(target=applier_loop, daemon=True, name="raft-apply")
        t.start()
        self._applier_thread = t

    # Persistence
    def _hard_state_path(self):
        return os.path.join(self.persist_dir, f"node{self.node_id}_hard_state.json")

    def _log_path(self):
        return os.path.join(self.persist_dir, f"node{self.node_id}_log.json")

    def _persist_hard_state(self):
        # Persist current_term and voted_for (called with lock held).
        state = {"current_term": self.current_term, "voted_for": self.voted_for}
        tmp = self._hard_state_path() + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, self._hard_state_path())

    def _persist_log(self):
        tmp = self._log_path() + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.log, f)
        os.replace(tmp, self._log_path())

    def _load_persistent_state(self):
        hs_path  = self._hard_state_path()
        log_path = self._log_path()
        if os.path.exists(hs_path):
            with open(hs_path) as f:
                hs = json.load(f)
            self.current_term = hs.get("current_term", 0)
            self.voted_for    = hs.get("voted_for")
            logger.info(f"[{self.node_id}] Restored hard state: "
                        f"term={self.current_term} voted_for={self.voted_for}")
        if os.path.exists(log_path):
            with open(log_path) as f:
                self.log = json.load(f)
            logger.info(f"[{self.node_id}] Restored log: {len(self.log)} entries")


# Custom exceptions
class NotLeaderError(Exception):
    def __init__(self, leader_id=None, leader_address=None):
        self.leader_id      = leader_id
        self.leader_address = leader_address
        super().__init__(f"Not the leader. Leader is node {leader_id} at {leader_address}")
