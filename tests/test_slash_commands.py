"""Slash commands the clone bot registers: the removed announcement and role block commands
stay gone (forwarding and the mapping filters replace them)."""
import importlib
import logging

import discord
import pytest

REMOVED = {
    "block_add",
    "block_list",
    "announcement_trigger_add",
    "announce_subscription_toggle",
    "announce_trigger_list",
    "announce_subscription_list",
    "announce_help",
    "role_block",
    "role_block_clear",
    "token_debug",
}


def _command_names():
    # importing the commands module can retune library log levels; put them back
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    levels = {n: lg.level for n, lg in logging.root.manager.loggerDict.items() if isinstance(lg, logging.Logger)}
    try:
        cog = importlib.import_module("copycord.server.commands").CloneCommands
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        for n, lg in list(logging.root.manager.loggerDict.items()):
            if isinstance(lg, logging.Logger):
                lg.setLevel(levels.get(n, logging.NOTSET))
    return {v.name for v in vars(cog).values() if isinstance(v, discord.ApplicationCommand)}


def test_removed_commands_are_not_registered():
    names = _command_names()
    assert "onjoin_dm" in names
    assert not names & REMOVED


def _http_error(cls, status, code, message):
    resp = type("Resp", (), {"status": status, "reason": message})()
    return cls(resp, {"code": code, "message": message})


async def _run_on_ready(error, caplog):
    from types import SimpleNamespace

    _command_names()
    mod = importlib.import_module("copycord.server.commands")

    async def sync_commands():
        raise error

    fake = SimpleNamespace(
        allowed_users=[1],
        _refresh_command_guilds=lambda: [2],
        bot=SimpleNamespace(sync_commands=sync_commands),
    )
    with caplog.at_level(logging.DEBUG, logger="server"):
        await mod.CloneCommands.on_ready(fake)
    return mod


@pytest.mark.asyncio
async def test_missing_access_sync_logs_one_warning(caplog):
    err = _http_error(discord.Forbidden, 403, 50001, "Missing Access")
    mod = await _run_on_ready(err, caplog)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert [r.getMessage() for r in warnings] == [mod.SLASH_MISSING_ACCESS_HINT]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert not [r for r in caplog.records if r.exc_info]


@pytest.mark.asyncio
async def test_other_sync_error_keeps_traceback(caplog):
    err = _http_error(discord.HTTPException, 500, 0, "boom")
    await _run_on_ready(err, caplog)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info
    assert errors[0].getMessage() == "Slash command sync failed"
