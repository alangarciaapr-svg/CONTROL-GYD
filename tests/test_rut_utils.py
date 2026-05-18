from segav_core.rut_utils import format_rut_chileno, rut_parts, validate_rut_dv


def test_rut_parts():
    assert rut_parts("12.345.678-5") == ("12345678", "5")


def test_validate_rut_ok():
    assert validate_rut_dv("12.345.678-5") is True


def test_validate_rut_fail():
    assert validate_rut_dv("12.345.678-9") is False


def test_format_rut():
    assert format_rut_chileno("123456785") == "12.345.678-5"
