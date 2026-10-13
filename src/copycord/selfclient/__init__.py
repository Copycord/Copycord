# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Copycord's user-account Discord client.

Replaces discord.py-self for the client role. Nothing in this package imports
``discord``.
"""

from . import client, enums, errors, events, fingerprint, gateway, http, models, state
from .client import Client
from .enums import (
    API_VERSION,
    Capabilities,
    ChannelType,
    CloseCode,
    GatewayOp,
    MessageType,
    StickerFormatType,
    StickerType,
    VideoQualityMode,
)
from .errors import (
    ClientException,
    ConnectionClosed,
    DiscordServerError,
    Forbidden,
    HTTPException,
    LoginFailure,
    NotFound,
    RateLimited,
    SelfClientException,
)
from .events import Dispatch, parse as parse_event
from .gateway import GatewayClient
from .http import HTTPClient
from .state import ConnectionState
from .models import (
    Asset,
    Attachment,
    CategoryChannel,
    Colour,
    Color,
    DMChannel,
    Embed,
    Emoji,
    ForumChannel,
    ForumTag,
    GroupChannel,
    Guild,
    GuildChannel,
    Member,
    Message,
    MessageFlags,
    MessageReference,
    Object,
    PartialEmoji,
    PartialMessageable,
    PermissionOverwrite,
    Permissions,
    Role,
    StageChannel,
    Sticker,
    TextChannel,
    Thread,
    User,
    VoiceChannel,
    channel_from_dict,
)

__all__ = [
    "client",
    "enums",
    "errors",
    "events",
    "fingerprint",
    "gateway",
    "http",
    "models",
    "state",
    "Client",
    "ConnectionState",
    "API_VERSION",
    "Capabilities",
    "ChannelType",
    "CloseCode",
    "GatewayOp",
    "MessageType",
    "StickerFormatType",
    "StickerType",
    "VideoQualityMode",
    "ClientException",
    "ConnectionClosed",
    "DiscordServerError",
    "Forbidden",
    "HTTPException",
    "LoginFailure",
    "NotFound",
    "RateLimited",
    "SelfClientException",
    "Dispatch",
    "GatewayClient",
    "HTTPClient",
    "parse_event",
    "Asset",
    "Attachment",
    "CategoryChannel",
    "Colour",
    "Color",
    "DMChannel",
    "Embed",
    "Emoji",
    "ForumChannel",
    "ForumTag",
    "GroupChannel",
    "Guild",
    "GuildChannel",
    "Member",
    "Message",
    "MessageFlags",
    "MessageReference",
    "Object",
    "PartialEmoji",
    "PartialMessageable",
    "PermissionOverwrite",
    "Permissions",
    "Role",
    "StageChannel",
    "Sticker",
    "TextChannel",
    "Thread",
    "User",
    "VoiceChannel",
    "channel_from_dict",
]
