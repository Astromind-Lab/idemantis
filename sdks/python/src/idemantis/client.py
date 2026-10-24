import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional

from .ids import client_request_id_for

_KEY = re.compile(r"^idm_[0-9A-Z]{26}_[A-Za-z0-9_-]{43}$")


class IdemantisApiError(Exception):
    def __init__(self, status: int, code: str, message: str, request_id: Optional[str] = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.request_id = request_id


def _settled(context: Dict[str, Any]) -> bool:
    return context.get("terminal_outcome") is not None or context["next"]["actor"] != "runner"


class IdemantisClient:
    def __init__(self, url: str, api_key: str, timeout: float = 30.0):
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("Runner URL must be an HTTP or HTTPS origin")
        if not _KEY.match(api_key):
            raise ValueError("API key is malformed")
        self._origin = f"{parsed.scheme}://{parsed.netloc}"
        self._api_key = api_key
        self._timeout = timeout

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/api/health")

    def operations(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/api/operations")["items"]

    def list_definitions(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/api/definitions")["items"]

    def register_plan(
        self, plan: Dict[str, Any], subject_reference_keys: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"plan": plan}
        if subject_reference_keys is not None:
            body["subject_reference_keys"] = subject_reference_keys
        return self._request("POST", "/api/plans", body)

    def admit(
        self,
        definition_digest: str,
        input: Dict[str, Any],
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"definition_digest": definition_digest, "input": input}
        if idempotency_key is not None:
            body["client_request_id"] = client_request_id_for(f"admission:{idempotency_key}")
        return self._request("POST", "/api/missions", body)

    def start(self, run_id: str) -> Dict[str, Any]:
        return self._request("POST", f"/api/missions/{self._id(run_id)}/start", {})

    def launch(
        self,
        definition_digest: str,
        input: Dict[str, Any],
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        admitted = self.admit(definition_digest, input, idempotency_key)
        self.start(admitted["summary"]["run_id"])
        return admitted

    def mission(self, run_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/missions/{self._id(run_id)}")

    def context(self, run_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/missions/{self._id(run_id)}/context")

    def list_missions(self, **query: Any) -> Dict[str, Any]:
        suffix = f"?{urllib.parse.urlencode(query)}" if query else ""
        return self._request("GET", f"/api/missions{suffix}")

    def observe(self, run_id: str, observation: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(
            "POST", f"/api/missions/{self._id(run_id)}/observations", observation
        )

    def resolve(
        self,
        run_id: str,
        action_code: str,
        idempotency_key: str,
        input: Optional[Dict[str, Any]] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        request_id = client_request_id_for(f"{run_id}:{idempotency_key}")
        base = f"/api/missions/{self._id(run_id)}/interventions"
        try:
            return self._request("GET", f"{base}/{request_id}/receipt")
        except IdemantisApiError as error:
            if error.status != 404:
                raise
        detail = self.mission(run_id)
        action = next(
            (item for item in detail["valid_interventions"] if item["action_code"] == action_code),
            None,
        )
        if action is None:
            raise ValueError(f"Mission {run_id} does not offer {action_code}")
        prepare: Dict[str, Any] = {
            "client_request_id": request_id,
            "expected_sequence": detail["summary"]["current_sequence"],
            "action_code": action_code,
            "target": action["target"],
            "input": input or {},
        }
        if reason is not None:
            prepare["reason"] = reason
        prepared = self._request("POST", f"{base}/prepare", prepare)
        return self._request(
            "POST",
            f"{base}/submit",
            {"client_request_id": request_id, "proposal_digest": prepared["proposal_digest"]},
        )

    def wait_for(
        self,
        run_id: str,
        until: Optional[Callable[[Dict[str, Any]], bool]] = None,
        timeout: float = 60.0,
        interval: float = 0.25,
    ) -> Dict[str, Any]:
        condition = until or _settled
        deadline = time.monotonic() + timeout
        while True:
            context = self.context(run_id)
            if condition(context) or context.get("terminal_outcome") is not None:
                return context
            if time.monotonic() + interval > deadline:
                raise IdemantisApiError(
                    408, "WAIT_TIMEOUT", f"Mission {run_id} did not reach the condition in time"
                )
            time.sleep(interval)

    def _id(self, run_id: str) -> str:
        return urllib.parse.quote(run_id, safe="")

    def _request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        headers = {"accept": "application/json", "authorization": f"Bearer {self._api_key}"}
        data = None
        if body is not None:
            headers["content-type"] = "application/json"
            data = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"{self._origin}{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                text = response.read().decode("utf-8")
                return json.loads(text) if text else {}
        except urllib.error.HTTPError as error:
            text = error.read().decode("utf-8", "replace")
            try:
                detail = json.loads(text).get("error", {})
            except ValueError:
                detail = {}
            raise IdemantisApiError(
                error.code,
                detail.get("code", "REQUEST_FAILED"),
                detail.get("message", f"Runner returned HTTP {error.code}"),
                detail.get("request_id"),
            ) from None
        except (urllib.error.URLError, OSError) as error:
            raise IdemantisApiError(0, "RUNNER_UNREACHABLE", str(error)) from None
