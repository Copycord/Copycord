import logging, re, time
from typing import Optional, Tuple
from common.rate_limiter import ActionType

log = logging.getLogger("discord_hooks")

# (pattern, action) pairs; extend as needed
_ROUTE_MAP: Tuple[Tuple[re.Pattern, ActionType], ...] = (
    (re.compile(r"/channels/\{channel_id\}/webhooks"), ActionType.WEBHOOK_CREATE),
    (re.compile(r"/guilds/\{guild_id\}/emojis"),        ActionType.EMOJI_CREATE),
    (re.compile(r"/guilds/\{guild_id\}/channels"),      ActionType.CREATE_CHANNEL),
    (re.compile(r"/channels/\{channel_id\}"),           ActionType.EDIT_CHANNEL), 
    (re.compile(r"/channels/\{channel_id\}/messages"),  ActionType.WEBHOOK_MESSAGE),
    (re.compile(r"/channels/\{channel_id\}/threads"),   ActionType.THREAD),
    (re.compile(r"/channels/\{channel_id\}/messages/\{message_id\}"), ActionType.DELETE_CHANNEL),
)

def _pick_major(parts: list[str]) -> str | None:
    for p in parts:
        if p and p.isdigit():
            return p
    return None

class DiscordHTTPRLHandler(logging.Handler):
    _rx = re.compile(r"Retrying in ([\d.]+) seconds.*bucket \"([^\"]+)\"")

    def __init__(self, ratelimit_mgr):
        super().__init__(level=logging.WARNING)
        self.rlm = ratelimit_mgr

    def _map_bucket(self, bucket: str) -> tuple[Optional[ActionType], Optional[str], str]:
        parts = bucket.split(":")
        major = _pick_major(parts) 
        route = parts[-1]

        action = None
        for pat, act in _ROUTE_MAP:
            if pat.search(route):
                action = act
                break

        key = None
        if action == ActionType.WEBHOOK_CREATE:
            key = None
        elif action == ActionType.WEBHOOK_MESSAGE and major:
            key = major
            
        log.debug("Bucket map: route=%s parts=%s -> action=%s key=%s",
                  route, parts, getattr(action, "name", None), key)
        return action, key, route

    def emit(self, record: logging.LogRecord):
        try:
            m = self._rx.search(record.getMessage())
            if not m:
                return

            FIXED_COOLDOWN_SECONDS = 300

            bucket = m.group(2)
            action, key, route = self._map_bucket(bucket)
            if not action:
                log.warning("No ActionType mapping for route=%s (bucket=%s); no penalty applied", route, bucket)
                return

            if action == ActionType.WEBHOOK_MESSAGE:
                return

            if action == ActionType.WEBHOOK_CREATE:
                key = None

            self.rlm.penalize(action, FIXED_COOLDOWN_SECONDS, key=key)
            log.warning("Rate limit detected; applying %ss safety net cooldown before next %s action",
                        FIXED_COOLDOWN_SECONDS, action.name)
        except Exception as e:
            log.exception("Error in DiscordHTTPRLHandler.emit: %s", e)

def install_discord_rl_probe(ratelimit_mgr):
    http_log = logging.getLogger("discord.http")
    if not any(isinstance(h, DiscordHTTPRLHandler) for h in http_log.handlers):
        http_log.addHandler(DiscordHTTPRLHandler(ratelimit_mgr))
        log.info("Installed DiscordHTTPRLHandler on 'discord.http' logger")
    else:
        log.debug("DiscordHTTPRLHandler already installed on 'discord.http'")