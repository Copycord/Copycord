"""copycord.common.env: dev mode is COPYCORD_ENV=dev, in any case, read on every call."""
import pytest

from copycord import __main__ as cli
from copycord.common import env


@pytest.mark.parametrize("value", ["dev", "DEV", "Dev", " dev "])
def test_dev_in_any_case(monkeypatch, value):
    monkeypatch.setenv("COPYCORD_ENV", value)
    assert env.is_dev() is True


@pytest.mark.parametrize("value", ["", "prod", "production", "1", "true", "development", "dev1"])
def test_anything_else_is_not_dev(monkeypatch, value):
    monkeypatch.setenv("COPYCORD_ENV", value)
    assert env.is_dev() is False


def test_unset_is_not_dev_and_is_read_each_call(monkeypatch):
    monkeypatch.delenv("COPYCORD_ENV", raising=False)
    assert env.is_dev() is False
    monkeypatch.setenv("COPYCORD_ENV", "dev")
    assert env.is_dev() is True


def test_listed_in_the_advanced_environment():
    assert "COPYCORD_ENV" in dict(cli.ADVANCED_ENV)
