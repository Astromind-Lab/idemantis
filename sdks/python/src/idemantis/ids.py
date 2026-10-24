import hashlib
import os

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode(data: bytes) -> str:
    value = int.from_bytes(data[:17], "big")
    text = ""
    for _ in range(26):
        text = _ALPHABET[value & 31] + text
        value >>= 5
    return text


def _check(key: str, label: str) -> None:
    if not 1 <= len(key) <= 512:
        raise ValueError(f"{label} keys must contain 1 to 512 characters")


def observation_id_for(key: str) -> str:
    _check(key, "Observation")
    digest = hashlib.sha256(f"observation:{key}".encode("utf-8")).digest()
    return f"obs_{_encode(digest)}"


def random_observation_id() -> str:
    return f"obs_{_encode(os.urandom(17))}"


def client_request_id_for(key: str) -> str:
    _check(key, "Request")
    digest = hashlib.sha256(f"request:{key}".encode("utf-8")).digest()
    return f"req-{_encode(digest).lower()}"
