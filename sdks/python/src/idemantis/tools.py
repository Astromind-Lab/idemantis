import hashlib
import hmac
import json
import os
import re
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

PROTOCOL_VERSION = 1
_ROUTE = re.compile(r"^/operations/([^/]+)/([a-z]+)$")
_NAME = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_MAXIMUM_BODY = 1_048_576


class ToolRejected(Exception):
    pass


class ToolUnavailable(Exception):
    pass


@dataclass
class ToolContext:
    run_id: str
    effect_id: str
    attempt_id: str
    idempotency_key: str
    deadline: str


@dataclass
class Tool:
    name: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    run: Callable[[Dict[str, Any], ToolContext], Any]
    lookup: Optional[Callable[[Dict[str, Any], ToolContext], Any]] = None
    effect_class: str = "record_mutation"
    consequence: str = "reversible"
    timeout_ms: int = 30_000

    def descriptor(self, adapter: str) -> Dict[str, Any]:
        return {
            "key": self.name,
            "adapter": adapter,
            "operation": self.name,
            "operation_schema_version": 1,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
            "subject_schema": {"type": "string"},
            "effect_class": self.effect_class,
            "consequence": self.consequence,
            "capabilities": {
                "duplicate_safety": "keyed_uniqueness",
                "reconciliation": "authoritative",
                "fencing_mode": "none",
                "fencing_scope": "operation",
                "handoff_mode": "settle_first",
                "execution_location": "either",
                "cancellation": False,
                "compensation": False,
                "observation_stream": False,
            },
            "timeout": {
                "minimum_ms": 1,
                "default_ms": self.timeout_ms,
                "maximum_ms": max(self.timeout_ms, 300_000),
            },
            "reconciliation_lookup_fields": ["effect_id"],
            "observation_types": [],
            "observation_correlation_fields": [],
            "secret_requirements": [],
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class ToolLedger:
    def __init__(self, path: str = ":memory:"):
        created = path != ":memory:" and not os.path.exists(path)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        if created:
            os.chmod(path, 0o600)
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA busy_timeout = 5000")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS tool_calls ("
            "effect_id TEXT PRIMARY KEY, tool TEXT NOT NULL, input_json TEXT NOT NULL, "
            "status TEXT NOT NULL, output_json TEXT, message TEXT, "
            "started_at TEXT NOT NULL, finished_at TEXT)"
        )

    def begin(self, effect_id: str, tool: str, value: Any) -> Optional[Dict[str, Any]]:
        with self._lock:
            cursor = self._db.execute(
                "INSERT INTO tool_calls(effect_id, tool, input_json, status, started_at) "
                "VALUES (?, ?, ?, 'running', ?) ON CONFLICT(effect_id) DO NOTHING",
                (effect_id, tool, json.dumps(value), _now()),
            )
            if cursor.rowcount == 1:
                return None
        return self.get(effect_id)

    def finish(
        self, effect_id: str, status: str, output: Any = None, message: Optional[str] = None
    ) -> None:
        with self._lock:
            self._db.execute(
                "UPDATE tool_calls SET status = ?, output_json = ?, message = ?, finished_at = ? "
                "WHERE effect_id = ? AND status = 'running'",
                (
                    status,
                    None if status != "completed" else json.dumps(output),
                    message,
                    _now(),
                    effect_id,
                ),
            )

    def forget(self, effect_id: str) -> None:
        with self._lock:
            self._db.execute(
                "DELETE FROM tool_calls WHERE effect_id = ? AND status != 'completed'",
                (effect_id,),
            )

    def get(self, effect_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._db.execute(
                "SELECT tool, input_json, status, output_json, message FROM tool_calls "
                "WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "tool": row[0],
            "input": json.loads(row[1]),
            "status": row[2],
            "output": json.loads(row[3]) if row[3] is not None else None,
            "message": row[4],
        }

    def count(self) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM tool_calls").fetchone()[0]

    def close(self) -> None:
        with self._lock:
            self._db.close()


@dataclass
class ToolServer:
    adapter: str
    version: str
    ledger: ToolLedger
    token: Optional[str] = None
    tools: Dict[str, Tool] = field(default_factory=dict)
    _inflight: Set[str] = field(default_factory=set)
    _inflight_lock: threading.Lock = field(default_factory=threading.Lock)
    _server: Optional[ThreadingHTTPServer] = None

    def __post_init__(self) -> None:
        if not _NAME.match(self.adapter):
            raise ValueError("Adapter names use lowercase letters, digits, dots and dashes")

    def tool(
        self,
        name: str,
        input_schema: Dict[str, Any],
        output_schema: Dict[str, Any],
        lookup: Optional[Callable[[Dict[str, Any], ToolContext], Any]] = None,
        effect_class: str = "record_mutation",
        consequence: str = "reversible",
        timeout_ms: int = 30_000,
    ) -> Callable[[Callable[[Dict[str, Any], ToolContext], Any]], Callable[..., Any]]:
        if not _NAME.match(name) or name in self.tools:
            raise ValueError(f"Tool name {name} is invalid or already registered")

        def register(run: Callable[[Dict[str, Any], ToolContext], Any]) -> Callable[..., Any]:
            self.tools[name] = Tool(
                name, input_schema, output_schema, run, lookup, effect_class, consequence, timeout_ms
            )
            return run

        return register

    def manifest(self) -> Dict[str, Any]:
        operations = [self.tools[name].descriptor(self.adapter) for name in sorted(self.tools)]
        digest = hashlib.sha256(
            json.dumps(
                {"adapter": self.adapter, "version": self.version, "operations": operations},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return {
            "protocol_version": PROTOCOL_VERSION,
            "manifest": {
                "adapter": self.adapter,
                "adapter_version": self.version,
                "adapter_package_digest": f"sha256:{digest}",
                "operations": operations,
            },
        }

    def issue(self, tool: Tool, context: ToolContext, value: Dict[str, Any]) -> Dict[str, Any]:
        prior = self.ledger.begin(context.effect_id, tool.name, value)
        if prior is not None:
            if prior["status"] == "completed":
                return {
                    "result": "completed",
                    "output": prior["output"],
                    "evidence": {"deduplicated": True},
                }
            if prior["status"] == "rejected":
                return {"result": "rejected", "evidence": {"message": prior["message"] or ""}}
            return {"result": "uncertain", "evidence": {"reason": f"prior call is {prior['status']}"}}
        with self._inflight_lock:
            self._inflight.add(context.effect_id)
        try:
            output = tool.run(value, context)
            self.ledger.finish(context.effect_id, "completed", output)
            return {"result": "completed", "output": output}
        except ToolRejected as error:
            self.ledger.finish(context.effect_id, "rejected", message=str(error)[:1024])
            return {"result": "rejected", "evidence": {"message": str(error)[:1024]}}
        except ToolUnavailable as error:
            self.ledger.forget(context.effect_id)
            return {"result": "failed", "evidence": {"message": str(error)[:1024]}}
        except Exception as error:
            self.ledger.finish(context.effect_id, "errored", message=str(error)[:1024])
            return {"result": "uncertain", "evidence": {"message": str(error)[:1024]}}
        finally:
            with self._inflight_lock:
                self._inflight.discard(context.effect_id)

    def reconcile(self, tool: Tool, context: ToolContext) -> Dict[str, Any]:
        entry = self.ledger.get(context.effect_id)
        if entry is None or entry["status"] == "rejected":
            return {"result": "confirmed_not_applied"}
        if entry["status"] == "completed":
            return {"result": "confirmed_completed", "output": entry["output"]}
        with self._inflight_lock:
            if context.effect_id in self._inflight:
                return {"result": "confirmed_in_progress"}
        if tool.lookup is None:
            return {"result": "unknown", "evidence": {"reason": "tool call was interrupted"}}
        found = tool.lookup(entry["input"], context)
        if found is None:
            return {"result": "unknown", "evidence": {"reason": "lookup could not decide"}}
        if found is False:
            self.ledger.forget(context.effect_id)
            return {"result": "confirmed_not_applied", "evidence": {"reason": "lookup found no effect"}}
        self.ledger.forget(context.effect_id)
        self.ledger.begin(context.effect_id, tool.name, entry["input"])
        self.ledger.finish(context.effect_id, "completed", found)
        return {"result": "confirmed_completed", "output": found}

    def handle(self, method: str, path: str, headers: Dict[str, str], body: bytes) -> Tuple[int, Any]:
        if self.token is not None:
            supplied = headers.get("authorization", "")
            if not hmac.compare_digest(supplied.encode(), f"Bearer {self.token}".encode()):
                return 401, {"error": {"code": "UNAUTHORIZED", "message": "Adapter credentials are invalid"}}
        if path == "/manifest":
            if method != "GET":
                return 405, {"error": {"code": "METHOD_NOT_ALLOWED", "message": "Manifest requires GET"}}
            return 200, self.manifest()
        match = _ROUTE.match(path)
        if match is None:
            return 404, {"error": {"code": "ROUTE_NOT_FOUND", "message": "Adapter route does not exist"}}
        if method != "POST":
            return 405, {"error": {"code": "METHOD_NOT_ALLOWED", "message": "Operations require POST"}}
        tool = self.tools.get(unquote(match.group(1)))
        action = match.group(2)
        if tool is None:
            return 404, {"error": {"code": "OPERATION_NOT_FOUND", "message": "Operation does not exist"}}
        if action not in ("issue", "reconcile"):
            return 404, {"error": {"code": "ACTION_UNSUPPORTED", "message": f"Tools do not support {action}"}}
        try:
            document = json.loads(body.decode("utf-8"))
            raw = document["context"]
            context = ToolContext(
                run_id=raw["run_id"],
                effect_id=raw["effect_id"],
                attempt_id=raw["attempt_id"],
                idempotency_key=raw["effect_id"],
                deadline=raw["deadline"],
            )
            if document.get("protocol_version") != PROTOCOL_VERSION:
                raise ValueError("protocol version")
        except (ValueError, KeyError, TypeError):
            return 400, {"error": {"code": "REQUEST_INVALID", "message": f"{action} request is invalid"}}
        if action == "issue":
            return 200, self.issue(tool, context, document.get("input"))
        return 200, self.reconcile(tool, context)

    def serve(self, host: str = "127.0.0.1", port: int = 0) -> str:
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def _respond(self, method: str) -> None:
                length = int(self.headers.get("content-length") or 0)
                if length > _MAXIMUM_BODY:
                    status, payload = 413, {"error": {"code": "REQUEST_TOO_LARGE", "message": "Request body is too large"}}
                else:
                    body = self.rfile.read(length) if length else b""
                    headers = {key.lower(): value for key, value in self.headers.items()}
                    status, payload = owner.handle(method, urlparse(self.path).path, headers, body)
                text = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("content-type", "application/json; charset=utf-8")
                self.send_header("content-length", str(len(text)))
                self.send_header("cache-control", "no-store")
                self.end_headers()
                self.wfile.write(text)

            def do_GET(self) -> None:
                self._respond("GET")

            def do_POST(self) -> None:
                self._respond("POST")

            def log_message(self, format: str, *args: Any) -> None:
                return

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        bound_host, bound_port = self._server.server_address[:2]
        return f"http://{bound_host}:{bound_port}/"

    def close(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
