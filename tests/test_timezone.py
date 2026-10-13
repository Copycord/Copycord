import logging

import pytest

from copycord import envfile
from copycord.common import timezone as tzmod


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.delenv("TZ", raising=False)
    monkeypatch.delenv("UI_TZ", raising=False)
    tzmod._warned.clear()


def test_default_is_utc():
    assert tzmod.configured_tz_name() == "UTC"
    assert str(tzmod.configured_tz()) == "UTC"


def test_valid_name(monkeypatch):
    monkeypatch.setenv("TZ", "Europe/Berlin")
    assert str(tzmod.configured_tz()) == "Europe/Berlin"


def test_invalid_name_falls_back_with_one_warning(monkeypatch, caplog):
    monkeypatch.setenv("TZ", "Not/AZone")
    with caplog.at_level(logging.WARNING, logger="common.timezone"):
        assert tzmod.configured_tz_name() == "UTC"
        assert str(tzmod.configured_tz()) == "UTC"
    assert len(caplog.records) == 1


def test_ui_tz_used_when_tz_unset_with_warning(monkeypatch, caplog):
    monkeypatch.setenv("UI_TZ", "Asia/Tokyo")
    with caplog.at_level(logging.WARNING, logger="common.timezone"):
        assert str(tzmod.configured_tz()) == "Asia/Tokyo"
        tzmod.configured_tz()
    assert len(caplog.records) == 1
    assert "TZ" in caplog.records[0].getMessage()


def test_tz_wins_over_ui_tz(monkeypatch, caplog):
    monkeypatch.setenv("TZ", "Europe/Berlin")
    monkeypatch.setenv("UI_TZ", "Asia/Tokyo")
    with caplog.at_level(logging.WARNING, logger="common.timezone"):
        assert str(tzmod.configured_tz()) == "Europe/Berlin"
    assert not caplog.records


def test_template_has_no_ui_tz(tmp_path):
    assert "UI_TZ" not in envfile.template(tmp_path / "home")
    assert "UI_TZ" not in envfile.ENV_KEYS
    assert "UI_TZ" not in envfile.ENV_DEFAULTS
    assert "UI_TZ" not in envfile.TEMPLATE_KEYS
