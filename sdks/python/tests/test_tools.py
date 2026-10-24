import json
import unittest

from idemantis import ToolLedger, ToolRejected, ToolServer, ToolUnavailable, plan, step
from idemantis import fields, from_input, from_output, literal, observation_id_for

CONTEXT = {
    "adapter_instance_id": "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    "run_id": "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    "command_id": "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    "attempt_id": "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    "effect_id": "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    "ownership_epoch": 1,
    "deadline": "2030-01-01T00:00:00.000Z",
    "source_id": "test",
}


def call(server, action, effect_id="eff_01ARZ3NDEKTSV4RRFFQ69G5FAV", token=None, value=None):
    context = dict(CONTEXT, effect_id=effect_id)
    body = {"protocol_version": 1, "context": context}
    body["input" if action == "issue" else "subject"] = value if value is not None else (
        {"subject": "a"} if action == "issue" else "create"
    )
    headers = {"authorization": f"Bearer {token}"} if token else {}
    return server.handle(
        "POST", f"/operations/crm.create/{action}", headers, json.dumps(body).encode()
    )


class ToolServerTest(unittest.TestCase):
    def setUp(self):
        self.applied = []
        self.ledger = ToolLedger()
        self.server = ToolServer(adapter="py-tools", version="1.0.0", ledger=self.ledger)
        self.behavior = None

        def lookup(value, context):
            matches = [item for item in self.applied if item == context.idempotency_key]
            return {"ticket": "T-1"} if matches else False

        @self.server.tool(
            "crm.create",
            input_schema={"type": "object", "properties": {"subject": {"type": "string"}}},
            output_schema={"type": "object", "properties": {"ticket": {"type": "string"}}},
            lookup=lookup,
        )
        def create(value, context):
            self.applied.append(context.idempotency_key)
            if self.behavior:
                self.behavior()
            return {"ticket": f"T-{len(self.applied)}"}

    def tearDown(self):
        self.ledger.close()

    def test_manifest_describes_retry_safe_reconcilable_tools(self):
        status, manifest = self.server.handle("GET", "/manifest", {}, b"")
        operation = manifest["manifest"]["operations"][0]
        self.assertEqual(status, 200)
        self.assertEqual(manifest["protocol_version"], 1)
        self.assertTrue(manifest["manifest"]["adapter_package_digest"].startswith("sha256:"))
        self.assertEqual(operation["capabilities"]["duplicate_safety"], "keyed_uniqueness")
        self.assertEqual(operation["capabilities"]["reconciliation"], "authoritative")

    def test_repeated_issue_is_answered_from_the_ledger(self):
        first = call(self.server, "issue")
        second = call(self.server, "issue")
        self.assertEqual(first, (200, {"result": "completed", "output": {"ticket": "T-1"}}))
        self.assertEqual(second[1]["evidence"], {"deduplicated": True})
        self.assertEqual(len(self.applied), 1)
        self.assertEqual(call(self.server, "reconcile")[1]["result"], "confirmed_completed")
        self.assertEqual(
            call(self.server, "reconcile", "eff_01ARZ3NDEKTSV4RRFFQ69G5FAW")[1]["result"],
            "confirmed_not_applied",
        )

    def test_lost_response_is_recovered_by_lookup(self):
        def lose():
            raise ConnectionError("reset after create")

        self.behavior = lose
        self.assertEqual(call(self.server, "issue")[1]["result"], "uncertain")
        self.assertEqual(call(self.server, "issue")[1]["result"], "uncertain")
        recovered = call(self.server, "reconcile")[1]
        self.assertEqual(recovered, {"result": "confirmed_completed", "output": {"ticket": "T-1"}})
        self.assertEqual(len(self.applied), 1)

    def test_transient_and_rejected_failures(self):
        def unavailable():
            raise ToolUnavailable("rate limited")

        self.behavior = unavailable
        self.assertEqual(call(self.server, "issue")[1]["result"], "failed")
        self.assertEqual(self.ledger.count(), 0)

        def rejected():
            raise ToolRejected("not allowed")

        self.behavior = rejected
        self.assertEqual(call(self.server, "issue")[1]["result"], "rejected")
        self.assertEqual(call(self.server, "issue")[1]["result"], "rejected")
        self.assertEqual(call(self.server, "reconcile")[1]["result"], "confirmed_not_applied")

    def test_rejects_bad_requests_and_credentials(self):
        guarded = ToolServer(adapter="py-tools", version="1.0.0", ledger=self.ledger, token="s3cret")
        self.assertEqual(guarded.handle("GET", "/manifest", {}, b"")[0], 401)
        self.assertEqual(
            guarded.handle("GET", "/manifest", {"authorization": "Bearer s3cret"}, b"")[0], 200
        )
        self.assertEqual(self.server.handle("POST", "/operations/crm.create/issue", {}, b"{")[0], 400)
        self.assertEqual(self.server.handle("POST", "/operations/missing/issue", {}, b"{}")[0], 404)
        self.assertEqual(self.server.handle("POST", "/operations/crm.create/fence", {}, b"{}")[0], 404)
        self.assertEqual(self.server.handle("GET", "/operations/crm.create/issue", {}, b"")[0], 405)
        with self.assertRaises(ValueError):
            self.server.tool("crm.create", {}, {})


class InflightReconcileTest(unittest.TestCase):
    def test_reconcile_waits_for_the_running_call(self):
        import threading
        import time

        ledger = ToolLedger()
        server = ToolServer(adapter="py-tools", version="1.0.0", ledger=ledger)
        gate = threading.Event()

        @server.tool("crm.create", input_schema={"type": "object"}, output_schema={"type": "object"})
        def slow(value, context):
            gate.wait(5)
            return {"ticket": "T-9"}

        issued = []
        worker = threading.Thread(target=lambda: issued.append(call(server, "issue")))
        worker.start()
        time.sleep(0.05)
        threading.Timer(0.1, gate.set).start()
        reconciled = call(server, "reconcile")
        worker.join(5)
        ledger.close()
        self.assertEqual(reconciled[1], {"result": "confirmed_completed", "output": {"ticket": "T-9"}})
        self.assertEqual(issued[0][1]["result"], "completed")


class PlanBuilderTest(unittest.TestCase):
    def test_builds_the_plan_document_the_runner_accepts(self):
        document = plan(
            "agent.brief",
            "1.0.0",
            {"type": "object"},
            [
                step("search", "web.search", fields(query=from_input("query"))),
                step("notify", "chat.notify", fields(text=from_output("search", "url"), tag=literal("x"))),
            ],
            attempts=3,
        )
        self.assertEqual(document["steps"][1]["input"]["fields"]["text"]["kind"], "output")
        self.assertEqual(document["attempts"], 3)
        self.assertEqual(observation_id_for("a"), observation_id_for("a"))
        with self.assertRaises(ValueError):
            from_input()


if __name__ == "__main__":
    unittest.main()
