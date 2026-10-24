from typing import Any, Dict, List, Optional


def from_input(*path: str) -> Dict[str, Any]:
    if not path:
        raise ValueError("from_input needs at least one path segment")
    return {"kind": "input", "path": list(path)}


def from_output(step: str, *path: str) -> Dict[str, Any]:
    return {"kind": "output", "state": step, "path": list(path)}


def literal(value: Any) -> Dict[str, Any]:
    return {"kind": "literal", "value": value}


def fields(**values: Dict[str, Any]) -> Dict[str, Any]:
    return {"kind": "object", "fields": dict(values)}


def step(name: str, operation: str, input: Dict[str, Any]) -> Dict[str, Any]:
    return {"name": name, "operation": operation, "input": input}


def plan(
    id: str,
    version: str,
    input_schema: Dict[str, Any],
    steps: List[Dict[str, Any]],
    attempts: Optional[int] = None,
    backoff_ms: Optional[int] = None,
) -> Dict[str, Any]:
    document: Dict[str, Any] = {
        "id": id,
        "version": version,
        "input_schema": input_schema,
        "steps": steps,
    }
    if attempts is not None:
        document["attempts"] = attempts
    if backoff_ms is not None:
        document["backoff_ms"] = backoff_ms
    return document
