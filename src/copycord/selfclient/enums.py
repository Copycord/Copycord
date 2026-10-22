# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Discord enum values and gateway constants.

The model enums are IntEnums with discord.py's member names, so they compare
equal to raw payload ints and lookups by name keep working.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Any, Optional, Type, TypeVar

E = TypeVar("E", bound="_Enum")


class _Enum(IntEnum):
    # discord.py's enums print as "ClassName.member"; Python 3.11 IntEnum would
    # print the bare number, which changes what str(message.type) serializes to.
    def __str__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"

    @classmethod
    def try_value(cls: Type[E], value: Any) -> Any:
        """The member for ``value``, or ``value`` unchanged if it is unknown."""
        try:
            return cls(int(value))
        except (TypeError, ValueError):
            return value


class ChannelType(_Enum):
    text = 0
    private = 1
    voice = 2
    group = 3
    category = 4
    news = 5
    news_thread = 10
    public_thread = 11
    private_thread = 12
    stage_voice = 13
    directory = 14
    forum = 15
    media = 16

    # discord.py's ChannelType prints as the bare name.
    def __str__(self) -> str:
        return self.name


THREAD_TYPES = frozenset(
    {ChannelType.news_thread, ChannelType.public_thread, ChannelType.private_thread}
)
TEXT_LIKE_TYPES = frozenset({ChannelType.text, ChannelType.news})
VOICE_LIKE_TYPES = frozenset({ChannelType.voice, ChannelType.stage_voice})
FORUM_LIKE_TYPES = frozenset({ChannelType.forum, ChannelType.media})


class MessageType(_Enum):
    default = 0
    recipient_add = 1
    recipient_remove = 2
    call = 3
    channel_name_change = 4
    channel_icon_change = 5
    pins_add = 6
    new_member = 7
    premium_guild_subscription = 8
    premium_guild_tier_1 = 9
    premium_guild_tier_2 = 10
    premium_guild_tier_3 = 11
    channel_follow_add = 12
    guild_stream = 13
    guild_discovery_disqualified = 14
    guild_discovery_requalified = 15
    guild_discovery_grace_period_initial_warning = 16
    guild_discovery_grace_period_final_warning = 17
    thread_created = 18
    reply = 19
    chat_input_command = 20
    application_command = 20
    thread_starter_message = 21
    guild_invite_reminder = 22
    context_menu_command = 23
    auto_moderation_action = 24
    role_subscription_purchase = 25
    interaction_premium_upsell = 26
    stage_start = 27
    stage_end = 28
    stage_speaker = 29
    stage_raise_hand = 30
    stage_topic = 31
    guild_application_premium_subscription = 32
    guild_incident_alert_mode_enabled = 36
    guild_incident_alert_mode_disabled = 37
    guild_incident_report_raid = 38
    guild_incident_report_false_alarm = 39
    purchase_notification = 44
    poll_result = 46


# Types discord.py's Message.is_system() treats as user content, not system.
NON_SYSTEM_MESSAGE_TYPES = frozenset(
    {
        MessageType.default,
        MessageType.reply,
        MessageType.chat_input_command,
        MessageType.context_menu_command,
        MessageType.thread_starter_message,
    }
)


class StickerType(_Enum):
    standard = 1
    guild = 2


class StickerFormatType(_Enum):
    png = 1
    apng = 2
    lottie = 3
    gif = 4


class VideoQualityMode(_Enum):
    auto = 1
    full = 2


class OverwriteType(_Enum):
    role = 0
    member = 1


class GatewayOp:
    """Gateway opcodes a user-account client sends or receives."""

    DISPATCH = 0
    HEARTBEAT = 1
    IDENTIFY = 2
    PRESENCE_UPDATE = 3
    VOICE_STATE_UPDATE = 4
    RESUME = 6
    RECONNECT = 7
    REQUEST_GUILD_MEMBERS = 8
    INVALID_SESSION = 9
    HELLO = 10
    HEARTBEAT_ACK = 11
    GUILD_SUBSCRIPTIONS = 14  # deprecated, replaced by BULK_GUILD_SUBSCRIBE
    BULK_GUILD_SUBSCRIBE = 37
    QOS_HEARTBEAT = 40  # the web client's heartbeat; it never sends op 1
    UPDATE_TIME_SPENT_SESSION_ID = 41


class CloseCode:
    """Gateway close codes."""

    # Sent by us, not by Discord: closing with 4000 instead of 1000 keeps the
    # session resumable.
    RESUME = 4000

    UNKNOWN_ERROR = 4000
    UNKNOWN_OPCODE = 4001
    DECODE_ERROR = 4002
    NOT_AUTHENTICATED = 4003
    AUTHENTICATION_FAILED = 4004
    ALREADY_AUTHENTICATED = 4005
    INVALID_SEQ = 4007
    RATE_LIMITED = 4008
    SESSION_TIMED_OUT = 4009
    INVALID_SHARD = 4010
    SHARDING_REQUIRED = 4011
    INVALID_API_VERSION = 4012
    INVALID_INTENTS = 4013
    DISALLOWED_INTENTS = 4014


# Close codes no reconnect can fix.
FATAL_CLOSE_CODES = frozenset({4004, 4010, 4011, 4012, 4013, 4014})


class Capabilities:
    """IDENTIFY ``capabilities`` bits.

    ``WEB`` is what the web client sends. ``DEFAULT`` drops CHANNEL_OBFUSCATION
    so READY carries real channel names instead of ``___hidden___``.
    """

    LAZY_USER_NOTES = 1 << 0
    NO_AFFINE_USER_IDS = 1 << 1
    VERSIONED_READ_STATES = 1 << 2
    VERSIONED_USER_GUILD_SETTINGS = 1 << 3
    DEDUPE_USER_OBJECTS = 1 << 4
    PRIORITIZED_READY_PAYLOAD = 1 << 5
    MULTIPLE_GUILD_EXPERIMENT_POPULATIONS = 1 << 6
    NON_CHANNEL_READ_STATES = 1 << 7
    AUTH_TOKEN_REFRESH = 1 << 8
    USER_SETTINGS_PROTO = 1 << 9
    CLIENT_STATE_V2 = 1 << 10
    PASSIVE_GUILD_UPDATE = 1 << 11
    AUTO_CALL_CONNECT = 1 << 12
    DEBOUNCE_MESSAGE_REACTIONS = 1 << 13
    PASSIVE_GUILD_UPDATE_V2 = 1 << 14
    CHANNEL_OBFUSCATION = 1 << 15

    WEB = 1767421
    DEFAULT = WEB & ~CHANNEL_OBFUSCATION  # 1734653


API_VERSION = 9


def try_enum(cls: Type[E], value: Any) -> Optional[Any]:
    """``cls(value)`` if known, the raw value if not, None for None."""
    if value is None:
        return None
    return cls.try_value(value)


__all__ = [
    "ChannelType",
    "MessageType",
    "StickerType",
    "StickerFormatType",
    "VideoQualityMode",
    "OverwriteType",
    "GatewayOp",
    "CloseCode",
    "Capabilities",
    "FATAL_CLOSE_CODES",
    "THREAD_TYPES",
    "TEXT_LIKE_TYPES",
    "VOICE_LIKE_TYPES",
    "FORUM_LIKE_TYPES",
    "NON_SYSTEM_MESSAGE_TYPES",
    "API_VERSION",
    "try_enum",
]
