"""
End-to-end tests of the web gateway against a live 3-node cluster, through the
real gRPC client: reads and writes with ETags, conditional writes, Idempotency-Key
retries, and a leader failover that HTTP clients never see.

Start a cluster from fresh state first:
    cd ../part2_3_replication && rm -rf raft_state_node* && ./start_kv_cluster.sh
    cd ../part6_web && python test_gateway_live.py
"""

import sys
import time

from fastapi.testclient import TestClient

from gateway.main import create_app


def check(label, got, expected):
    if got == expected:
        print(f"  [PASS] {label}")
    else:
        print(f"  [FAIL] {label}\n         expected: {expected!r}\n         got:      {got!r}")
        sys.exit(1)


def wait_until_ready(client, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.get("/readyz").status_code == 200:
            return True
        time.sleep(0.3)
    return False


def main():
    with TestClient(create_app()) as c:
        print("== Readiness ==")
        check("the gateway finds a leader", wait_until_ready(c), True)

        print("== Write and read, with ETags ==")
        r = c.put("/v1/kv/city", json={"value": "Kolkata"})
        check("PUT returns 200", r.status_code, 200)
        v = r.json()["version"]
        r = c.get("/v1/kv/city")
        check("GET returns the value", r.json()["value"], "Kolkata")
        check("GET sends the version as the ETag", r.headers["etag"], f'"{v}"')
        r = c.get("/v1/kv/city", headers={"If-None-Match": f'"{v}"'})
        check("an unchanged key answers 304", r.status_code, 304)
        r = c.get("/v1/kv/city", params={"consistency": "linearizable"})
        check("a linearizable read works", r.json()["value"], "Kolkata")

        print("== Conditional writes ==")
        r = c.put("/v1/kv/city", json={"value": "Delhi"}, headers={"If-Match": f'"{v}"'})
        check("If-Match at the current version writes", r.status_code, 200)
        r = c.put("/v1/kv/city", json={"value": "Pune"}, headers={"If-Match": f'"{v}"'})
        check("If-Match at a stale version is 412", r.status_code, 412)
        check("the 412 reports the current version", r.json()["current_version"], v + 1)
        r = c.put("/v1/kv/fresh", json={"value": "1"}, headers={"If-None-Match": "*"})
        check("If-None-Match: * creates the key (201)", r.status_code, 201)
        r = c.put("/v1/kv/fresh", json={"value": "2"}, headers={"If-None-Match": "*"})
        check("...but only once (412)", r.status_code, 412)

        print("== Delete ==")
        check("DELETE returns 204", c.delete("/v1/kv/fresh").status_code, 204)
        check("DELETE again returns 404", c.delete("/v1/kv/fresh").status_code, 404)

        print("== Idempotency-Key ==")
        key = {"Idempotency-Key": "order-42"}
        v1 = c.put("/v1/kv/order", json={"value": "paid"}, headers=key).json()["version"]
        v2 = c.put("/v1/kv/order", json={"value": "paid"}, headers=key).json()["version"]
        check("a retry returns the original version", v2, v1)

        print("== Cluster status ==")
        s = c.get("/v1/cluster").json()
        old_leader = s["leader_id"]
        check("a leader is reported", old_leader is not None, True)
        check("every node is reachable", all(n["reachable"] for n in s["nodes"]), True)

        print("== Leader failover, invisible to HTTP clients ==")
        r = c.put(f"/v1/cluster/nodes/{old_leader}/isolation",
                  json={"isolated": True, "heal_after_seconds": 60})
        check(f"isolate the leader (node {old_leader})", r.json()["isolated"], True)
        started = time.monotonic()
        r = c.put("/v1/kv/during-failover", json={"value": "ok"})
        check("a write sent during the election succeeds", r.status_code, 200)
        print(f"         (took {time.monotonic() - started:.1f}s, including the election)")
        r = c.put("/v1/kv/order", json={"value": "paid"}, headers=key)
        check("the Idempotency-Key retry on the NEW leader returns the original version",
              r.json()["version"], v1)
        s = c.get("/v1/cluster").json()
        check("another node now leads", s["leader_id"] not in (None, old_leader), True)
        c.put(f"/v1/cluster/nodes/{old_leader}/isolation", json={"isolated": False})
        time.sleep(3)
        s = c.get("/v1/cluster").json()
        check("the healed node rejoins as a follower",
              s["nodes"][old_leader]["state"], "follower")
        check("data written during the failover is readable",
              c.get("/v1/kv/during-failover").json()["value"], "ok")

        print("== Losing the majority ==")
        check("the cluster is ready again", wait_until_ready(c), True)
        s = c.get("/v1/cluster").json()
        followers = [n["node_id"] for n in s["nodes"] if n["node_id"] != s["leader_id"]]
        for node_id in followers:
            c.put(f"/v1/cluster/nodes/{node_id}/isolation",
                  json={"isolated": True, "heal_after_seconds": 60})
        key = {"Idempotency-Key": "no-majority-1"}
        started = time.monotonic()
        r = c.put("/v1/kv/no-majority", json={"value": "x"}, headers=key)
        # The leader appended the write but can't commit it without a majority,
        # so the gateway can't say whether it will be applied: 504, not 503.
        check("with both followers cut off, a write is 504 (outcome unknown)",
              r.status_code, 504)
        print(f"         (gave up after {time.monotonic() - started:.1f}s)")
        for node_id in followers:
            c.put(f"/v1/cluster/nodes/{node_id}/isolation", json={"isolated": False})
        check("the cluster recovers once they reconnect", wait_until_ready(c), True)
        r = c.put("/v1/kv/no-majority", json={"value": "x"}, headers=key)
        check("retrying with the same Idempotency-Key succeeds", r.status_code, 200)
        check("...and the write is applied exactly once",
              c.get("/v1/kv/no-majority").json()["version"], 1)

        print("== Live status stream ==")
        with c.websocket_connect("/v1/cluster/stream") as ws:
            check("the stream reports all three nodes", len(ws.receive_json()["nodes"]), 3)

    print("\n== All gateway live tests passed ==")


if __name__ == "__main__":
    main()
