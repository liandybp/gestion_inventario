from __future__ import annotations

from app.auth import hash_password, verify_password


def test_hash_and_verify_password_roundtrip() -> None:
    password = "S3gura!"
    password_hash = hash_password(password)

    assert password_hash.startswith("pbkdf2_sha256$")
    assert verify_password(password, password_hash)
    assert not verify_password("wrong", password_hash)


def test_verify_password_handles_invalid_hash() -> None:
    assert not verify_password("abc", "")
    assert not verify_password("abc", "sha256$1000$salt$hash")
    assert not verify_password("abc", "pbkdf2_sha256$bad$parts")

