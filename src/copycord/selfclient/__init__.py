# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""selfclient: Copycord's own user-account Discord client.

Replaces discord.py-self for the client role (docs/v4/PLAN.md section 3). No
``discord`` import anywhere in this package.

So far: errors, enums, model wrappers over raw payloads, the curl_cffi REST
client with rate-limit pacing and paginators, the fingerprint re-export, the
raw-websockets gateway (HELLO/IDENTIFY/RESUME, heartbeat, op 37 subscriptions,
zlib-stream, reconnect), the event dispatch table, the cache state and
``Client``, which ties them together. Nothing in src/copycord/client uses this package
yet.
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
