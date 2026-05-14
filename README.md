from datetime import datetime, timezone

from segav_core.security_hardening import is_account_locked, is_password_strong, next_failure_state

UTC = timezone.utc


def test_password_strength_policy():
    assert is_password_strong("abc")[0] is False
    ok, _ = is_password_strong("Segav1234!")
    assert ok is True


def test_next_failure_state_locks_after_threshold():
    failures, lock_until = next_failure_state(4, now=datetime(2026, 4, 21, 12, 0, tzinfo=UTC))
    assert failures == 5
    assert lock_until is not None


def test_is_account_locked_detects_future_timestamp():
    locked, until = is_account_locked({"locked_until": "2099-01-01T00:00:00+00:00"}, now=datetime(2026, 4, 21, 12, 0, tzinfo=UTC))
    assert locked is True
    assert until.startswith("2099-01-01")
