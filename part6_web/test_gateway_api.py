"""
Hermetic tests for the web gateway: the real routes and service on top of a fake
cluster that applies writes to the real KVStateMachine in memory. No gRPC, no
generated stubs, no running cluster.

    python3 test_gateway_api.py
"""

import base64
import json
import sys
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parent.parent / "part2_3_replication"))
from kv_state_machine import KVStateMachine  # noqa: E402

from gateway.config import Settings  # noqa: E402
from gateway.domain import ClusterUnavailable, NodeStatus, WriteOutcomeUnknown  # noqa: E402
from gateway.main import create_app  # noqa: E402


class FakeCluster:
    """Stands in for RaftCluster: same methods, applied to one in-memory state machine."""
    node_count = 3

    def __init__(self):
        self.sm = KVStateMachine()
        self.fail_next = None     # an exception the next call raises
        self.writes = []          # (client_id, seq_num) of every write that reached the store
        self.isolated = {}
        self.statuses = None      # set to override node_statuses()

    def _maybe_fail(self):
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc

    def _apply(self, op, key, data, client_id, seq_num):
        self._maybe_fail()
        self.writes.append((client_id, seq_num))
        return self.sm.apply({"op_type": op, "filename": key, "data": data,
                              "client_id": client_id, "seq_num": seq_num})

    def get(self, key, linearizable=False):
        self._maybe_fail()
        value, version, found = self.sm.get(key)
        return found, value or b"", version

    def put(self, key, value, client_id, seq_num):
        return self._apply("put", key, value, client_id, seq_num)["version"]

    def cas(self, key, new_value, client_id, seq_num, *, expected_version=0,
            expect_absent=False):
        cmd = json.dumps({"new": base64.b64encode(new_value).decode(),
                          "expect_absent": expect_absent,
                          "expected_version": expected_version}).encode()
        result = self._apply("cas", key, cmd, client_id, seq_num)
        return result["swapped"], result["version"]

    def delete(self, key, client_id, seq_num):
        return self._apply("delete", key, b"", client_id, seq_num)["existed"]

    def node_statuses(self):
        self._maybe_fail()
        if self.statuses is not None:
            return self.statuses
        return [NodeStatus(node_id=i, address=f"node{i}:50051", reachable=True,
                           state="leader" if i == 0 else "follower", term=3, leader_id=0,
                           isolated=self.isolated.get(i, False))
                for i in range(self.node_count)]

    def set_isolated(self, node_id, isolated, heal_after=0.0):
        self.isolated[node_id] = isolated
        return isolated


class GatewayTestCase(unittest.TestCase):
    def setUp(self):
        self.cluster = FakeCluster()
        app = create_app(Settings(max_value_bytes=64), cluster_client=self.cluster)
        self.client = TestClient(app)
        self.client.__enter__()      # runs the app's lifespan (startup)
        self.addCleanup(self.client.__exit__, None, None, None)

    def put(self, key, value, **headers):
        return self.client.put(f"/v1/kv/{key}", json={"value": value}, headers=headers)

    def assertProblem(self, response, status):
        self.assertEqual(response.status_code, status, response.text)
        self.assertEqual(response.headers["content-type"], "application/problem+json")
        body = response.json()
        self.assertEqual(body["status"], status)
        return body


class KeyTest(GatewayTestCase):

    def test_put_then_get_returns_value_version_and_etag(self):
        r = self.put("city", "Kolkata")
        self.assertEqual((r.status_code, r.json()), (200, {"key": "city", "version": 1}))
        self.assertEqual(r.headers["etag"], '"1"')

        r = self.client.get("/v1/kv/city")
        self.assertEqual(r.json(), {"key": "city", "value": "Kolkata",
                                    "encoding": "utf-8", "version": 1})
        self.assertEqual(r.headers["etag"], '"1"')
        self.assertEqual(r.headers["cache-control"], "no-cache")

    def test_missing_key_is_404(self):
        body = self.assertProblem(self.client.get("/v1/kv/nope"), 404)
        self.assertEqual(body["title"], "Not Found")

    def test_keys_may_contain_slashes(self):
        self.put("ml/model", "weights")
        self.assertEqual(self.client.get("/v1/kv/ml/model").json()["value"], "weights")

    def test_binary_values_come_back_as_base64(self):
        self.cluster.sm.apply({"op_type": "put", "filename": "raw", "data": b"\xff\x00"})
        body = self.client.get("/v1/kv/raw").json()
        self.assertEqual((body["encoding"], body["value"]), ("base64", "/wA="))

    def test_value_over_the_limit_is_413(self):
        self.assertProblem(self.put("big", "x" * 65), 413)
        self.assertEqual(self.cluster.writes, [])      # rejected before the store

    def test_delete(self):
        self.put("k", "v")
        self.assertEqual(self.client.delete("/v1/kv/k").status_code, 204)
        self.assertProblem(self.client.delete("/v1/kv/k"), 404)
        self.assertProblem(self.client.get("/v1/kv/k"), 404)


class ConditionalRequestTest(GatewayTestCase):

    def test_get_with_current_etag_is_304(self):
        self.put("k", "v1")
        r = self.client.get("/v1/kv/k", headers={"If-None-Match": '"1"'})
        self.assertEqual((r.status_code, r.content), (304, b""))
        self.assertEqual(r.headers["etag"], '"1"')

        self.put("k", "v2")                                  # now at version 2
        r = self.client.get("/v1/kv/k", headers={"If-None-Match": '"1"'})
        self.assertEqual((r.status_code, r.json()["value"]), (200, "v2"))

    def test_if_match_writes_only_at_the_expected_version(self):
        self.put("k", "v1")
        r = self.put("k", "v2", **{"If-Match": '"1"'})
        self.assertEqual((r.status_code, r.json()["version"]), (200, 2))

        stale = self.put("k", "v3", **{"If-Match": '"1"'})   # someone else wrote first
        body = self.assertProblem(stale, 412)
        self.assertEqual(body["current_version"], 2)
        self.assertEqual(stale.headers["etag"], '"2"')
        self.assertEqual(self.client.get("/v1/kv/k").json()["value"], "v2")

    def test_if_match_on_a_missing_key_is_412(self):
        self.assertProblem(self.put("absent", "v", **{"If-Match": '"1"'}), 412)

    def test_if_none_match_star_creates_only_once(self):
        r = self.put("k", "first", **{"If-None-Match": "*"})
        self.assertEqual(r.status_code, 201)
        self.assertEqual((r.headers["location"], r.headers["etag"]), ("/v1/kv/k", '"1"'))

        body = self.assertProblem(self.put("k", "second", **{"If-None-Match": "*"}), 412)
        self.assertEqual(body["current_version"], 1)

    def test_malformed_or_unsupported_conditions_are_400(self):
        self.put("k", "v")
        for headers in ({"If-Match": "*"}, {"If-Match": 'W/"1"'}, {"If-Match": "abc"},
                        {"If-None-Match": '"1"'},
                        {"If-Match": '"1"', "If-None-Match": "*"}):
            with self.subTest(headers=headers):
                self.assertProblem(self.put("k", "x", **headers), 400)
        self.assertProblem(self.client.delete("/v1/kv/k", headers={"If-Match": '"1"'}), 400)
        self.assertEqual(self.client.get("/v1/kv/k").json()["version"], 1)   # untouched


class IdempotencyTest(GatewayTestCase):

    def test_retry_with_the_same_key_is_applied_once(self):
        first = self.put("order", "paid", **{"Idempotency-Key": "order-42"})
        retry = self.put("order", "paid", **{"Idempotency-Key": "order-42"})
        self.assertEqual(retry.json()["version"], first.json()["version"])
        self.assertEqual(self.client.get("/v1/kv/order").json()["version"], 1)
        self.assertEqual(self.cluster.writes[0], self.cluster.writes[1])   # same identity

    def test_reusing_a_key_for_a_different_request_makes_a_new_one(self):
        self.put("order", "paid", **{"Idempotency-Key": "order-42"})
        r = self.put("order", "refunded", **{"Idempotency-Key": "order-42"})
        self.assertEqual(r.json()["version"], 2)

    def test_retried_create_returns_its_original_201(self):
        headers = {"If-None-Match": "*", "Idempotency-Key": "signup-7"}
        self.assertEqual(self.put("user", "asha", **headers).status_code, 201)
        # Without the key's record this retry would be 412, since the key now exists.
        self.assertEqual(self.put("user", "asha", **headers).status_code, 201)

    def test_requests_without_a_key_each_get_their_own_identity(self):
        self.put("k", "a")
        self.put("k", "a")
        (id1, seq1), (id2, seq2) = self.cluster.writes
        self.assertNotEqual(id1, id2)
        self.assertEqual((seq1, seq2), (1, 1))
        self.assertEqual(self.client.get("/v1/kv/k").json()["version"], 2)


class ErrorTest(GatewayTestCase):

    def test_no_leader_is_503_with_retry_after(self):
        self.cluster.fail_next = ClusterUnavailable("no leader reachable within 8s")
        r = self.client.get("/v1/kv/k")
        self.assertProblem(r, 503)
        self.assertEqual(r.headers["retry-after"], "1")

    def test_unconfirmed_write_is_504_with_retry_advice(self):
        self.cluster.fail_next = WriteOutcomeUnknown("write not confirmed within 8s")
        body = self.assertProblem(self.put("k", "v"), 504)
        self.assertIn("Idempotency-Key", body["hint"])

    def test_invalid_body_is_422_problem(self):
        body = self.assertProblem(self.client.put("/v1/kv/k", json={}), 422)
        self.assertEqual(body["errors"][0]["loc"], ["body", "value"])

    def test_unknown_route_is_404_problem(self):
        self.assertProblem(self.client.get("/v2/anything"), 404)


class ClusterTest(GatewayTestCase):

    def test_status_reports_the_leader_and_every_node(self):
        body = self.client.get("/v1/cluster").json()
        self.assertEqual((body["leader_id"], body["term"], len(body["nodes"])), (0, 3, 3))

    def test_an_isolated_old_leader_is_not_reported_as_leader(self):
        self.cluster.statuses = [
            NodeStatus(0, "n0", True, state="leader", term=3, isolated=True),
            NodeStatus(1, "n1", True, state="leader", term=4, leader_id=1),
            NodeStatus(2, "n2", False)]
        body = self.client.get("/v1/cluster").json()
        self.assertEqual((body["leader_id"], body["term"]), (1, 4))

    def test_isolation(self):
        r = self.client.put("/v1/cluster/nodes/1/isolation",
                            json={"isolated": True, "heal_after_seconds": 30})
        self.assertEqual(r.json(), {"node_id": 1, "isolated": True})
        self.assertTrue(self.cluster.isolated[1])
        r = self.client.put("/v1/cluster/nodes/1/isolation", json={"isolated": False})
        self.assertEqual(r.json(), {"node_id": 1, "isolated": False})

    def test_isolation_must_heal_and_needs_a_real_node(self):
        self.assertProblem(self.client.put("/v1/cluster/nodes/1/isolation",
                                           json={"isolated": True, "heal_after_seconds": 1}),
                           422)   # under the 5 s minimum: a node can't be cut off for good
        self.assertProblem(self.client.put("/v1/cluster/nodes/7/isolation",
                                           json={"isolated": True}), 404)

    def test_websocket_streams_status(self):
        with self.client.websocket_connect("/v1/cluster/stream") as ws:
            first, second = ws.receive_json(), ws.receive_json()
        self.assertEqual((len(first["nodes"]), second["leader_id"]), (3, 0))

    def test_health_and_readiness(self):
        self.assertEqual(self.client.get("/healthz").json(), {"status": "ok"})
        self.assertEqual(self.client.get("/readyz").json()["leader_id"], 0)
        self.cluster.statuses = [NodeStatus(i, f"n{i}", True, state="candidate", term=5)
                                 for i in range(3)]
        self.assertProblem(self.client.get("/readyz"), 503)


class CorsTest(GatewayTestCase):

    def test_dashboard_origin_may_send_conditional_writes(self):
        r = self.client.options("/v1/kv/k", headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "content-type,if-match,idempotency-key"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["access-control-allow-origin"], "http://localhost:5173")

    def test_etag_is_readable_from_javascript(self):
        self.put("k", "v")
        r = self.client.get("/v1/kv/k", headers={"Origin": "http://localhost:5173"})
        self.assertIn("ETag", r.headers["access-control-expose-headers"])

    def test_other_origins_are_not_allowed(self):
        r = self.client.get("/v1/kv/k", headers={"Origin": "https://evil.example"})
        self.assertNotIn("access-control-allow-origin", r.headers)


if __name__ == "__main__":
    unittest.main(verbosity=2)
