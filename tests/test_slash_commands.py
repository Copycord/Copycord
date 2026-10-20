"""Slash commands the clone bot registers: the removed announcement and role block commands
stay gone (forwarding and the mapping filters replace them)."""
import importlib
import logging

import discord

REMOVED = {
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
    assert "onjoin_dm" in names and "block_list" in names
    assert not names & REMOVED
