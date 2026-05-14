from segav_core.api_security import issue_token, verify_token
from segav_core.auth import hash_password, verify_password


def test_hash_and_verify_password():
    salt, hashed = hash_password("segav1234")
    assert verify_password("segav1234", salt, hashed) is True
    assert verify_password("otro", salt, hashed) is False


def test_issue_and_verify_token():
    token = issue_token({"sub": 1, "username": "alan", "role": "SUPERADMIN"}, ttl_seconds=300, secret="test-secret")
    payload = verify_token(token, secret="test-secret")
    assert payload is not None
    assert payload["username"] == "alan"
