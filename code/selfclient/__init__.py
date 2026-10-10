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

This is the v4/selfclient-core cut: errors, enums, model wrappers over raw
payloads, the curl_cffi REST client with rate-limit pacing and paginators, and
the fingerprint re-export. ``Client`` and the gateway (HELLO/IDENTIFY/RESUME,
heartbeat, op 37 subscriptions, zlib-stream, event dispatch) land in
v4/selfclient-gateway; nothing in code/client uses this package yet.
"""

from . import enums, errors, fingerprint, http, models
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
from .http import HTTPClient
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
    "enums",
    "errors",
    "fingerprint",
    "http",
    "models",
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
    "HTTPClient",
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
