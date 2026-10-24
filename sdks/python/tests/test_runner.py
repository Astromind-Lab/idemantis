import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from idemantis import (
    IdemantisApiError,
    IdemantisClient,
    ToolLedger,
    ToolServer,
    fields,
    from_input,
    from_output,
    observation_id_for,
    plan,
    step,
)

ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / "apps" / "runner" / "dist" / "cli.js"
INSTANCE = "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV"


@unittest.skipUnless(CLI.exists() and shutil.which("node"), "built runner is required")
class RunnerIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="idemantis-python-")
        self.sent = []
        self.lose_first = False
        self.ledger = ToolLedger(os.path.join(self.directory, "tools.sqlite"))
        self.tools = ToolServer(adapter="py-tools", version="1.0.0", ledger=self.ledger, token="tool-secret")
        schema = {"type": "object", "properties": {"ticket": {"type": "string"}}, "required": ["ticket"]}

        @self.tools.tool(
            "crm.open",
            input_schema={"type": "object", "properties": {"subject": {"type": "string"}}, "required": ["subject"]},
            output_schema=schema,
        )
        def open_ticket(value, context):
            return {"ticket": f"T-{value['subject']}"}

        def lookup(value, context):
            return {"sent": True} if context.idempotency_key in self.sent else False

        @self.tools.tool(
            "mail.send",
            input_schema={"type": "object", "properties": {"ticket": {"type": "string"}}, "required": ["ticket"]},
            output_schema={"type": "object", "properties": {"sent": {"type": "boolean"}}, "required": ["sent"]},
            lookup=lookup,
        )
        def send(value, context):
            self.sent.append(context.idempotency_key)
            if self.lose_first and len(self.sent) == 1:
                raise TimeoutError("response lost after the mail was sent")
            return {"sent": True}

        url = self.tools.serve()
        state = os.path.join(self.directory, "runner")
        environment = dict(os.environ, IDEMANTIS_ADAPTER_TOKEN="tool-secret")
        self.runner = subprocess.Popen(
            ["node", str(CLI), "dev", "--port", "0", "--state-dir", state, "--adapter", f"{INSTANCE}={url}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            text=True,
        )
        origin = None
        for line in self.runner.stdout:
            found = re.match(r"Runner ready: (\S+)", line)
            if found:
                origin = found.group(1)
            if line.startswith("Journal:"):
                break
        self.assertIsNotNone(origin)
        created = subprocess.run(
            ["node", str(CLI), "keys", "create", "--name", "python-agent", "--scopes", "view,operate,define", "--state-dir", state],
            capture_output=True,
            text=True,
            check=True,
        )
        self.client = IdemantisClient(origin, json.loads(created.stdout)["token"])

    def tearDown(self):
        self.runner.terminate()
        self.runner.wait(timeout=10)
        self.tools.close()
        self.ledger.close()
        shutil.rmtree(self.directory, ignore_errors=True)

    def register(self):
        document = plan(
            "support.followup",
            "1.0.0",
            {"type": "object", "properties": {"subject": {"type": "string"}}, "required": ["subject"]},
            [
                step("open", "crm.open", fields(subject=from_input("subject"))),
                step("notify", "mail.send", fields(ticket=from_output("open", "ticket"))),
            ],
            backoff_ms=10,
        )
        return self.client.register_plan(document)["definition_digest"]

    def test_runs_a_python_plan_through_python_tools(self):
        self.assertEqual(self.client.health()["status"], "ready")
        self.assertEqual(sorted(item["key"] for item in self.client.operations()), ["crm.open", "mail.send"])
        digest = self.register()
        run_id = self.client.launch(digest, {"subject": "printer"})["summary"]["run_id"]
        context = self.client.wait_for(run_id, timeout=10, interval=0.05)
        self.assertEqual(context["terminal_outcome"], "completed")
        self.assertEqual(
            [effect["output"] for effect in context["effects"]],
            [{"ticket": "T-printer"}, {"sent": True}],
        )
        self.assertEqual(len(self.sent), 1)

    def test_recovers_a_lost_response_without_sending_twice(self):
        self.lose_first = True
        digest = self.register()
        run_id = self.client.launch(digest, {"subject": "router"})["summary"]["run_id"]
        context = self.client.wait_for(run_id, timeout=10, interval=0.05)
        self.assertEqual(context["terminal_outcome"], "completed")
        self.assertEqual(len(self.sent), 1)

    def test_reports_api_errors(self):
        with self.assertRaises(IdemantisApiError) as raised:
            self.client.admit("sha256:" + "0" * 64, {})
        self.assertEqual(raised.exception.status, 422)
        with self.assertRaises(IdemantisApiError) as missing:
            self.client.observe(
                "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
                {"observation_id": observation_id_for("x"), "observation_type": "a.b", "payload": {}},
            )
        self.assertEqual(missing.exception.status, 404)


if __name__ == "__main__":
    unittest.main()
