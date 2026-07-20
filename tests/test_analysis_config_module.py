from __future__ import annotations

import pytest

from analysis_config import get_beacon_rate


def test_get_beacon_rate_returns_configured_float(monkeypatch):
    monkeypatch.setenv("BEACON_RATE", "1.024")

    assert get_beacon_rate() == 1.024


def test_get_beacon_rate_preserves_invalid_value_fallback(monkeypatch, capsys):
    monkeypatch.setenv("BEACON_RATE", "invalid")

    assert get_beacon_rate() == 0.1024
    assert capsys.readouterr().out == (
        "Invalid float value in BEACON_RATE env variable.\n"
    )


def test_get_beacon_rate_preserves_missing_value_error(monkeypatch):
    monkeypatch.delenv("BEACON_RATE", raising=False)

    with pytest.raises(TypeError):
        get_beacon_rate()
