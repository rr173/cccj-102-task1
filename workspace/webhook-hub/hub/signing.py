"""HMAC-SHA256 request signing.

Signature base string is ``"<unix_ts>.<raw body>"`` — the timestamp is covered
by the MAC so receivers can reject replays outside their tolerance window.
"""

import hashlib
import hmac


def sign(secret: str, timestamp: int, body: bytes) -> str:
    mac = hmac.new(secret.encode("utf-8"), f"{timestamp}.".encode("utf-8") + body, hashlib.sha256)
    return mac.hexdigest()


def verify(secret: str, timestamp: int, body: bytes, expected_hex: str) -> bool:
    return hmac.compare_digest(sign(secret, timestamp, body), expected_hex)
