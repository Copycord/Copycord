# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""The selfclient home of the header / super-properties / TLS builder.

A re-export of common/selfbot_headers, not a copy: that module stays the one
fingerprint definition in the process (the server's user-token REST and the
admin scraper import it directly), and this name is where selfclient code
reaches it. Nothing here changes its behaviour.

The import is absolute (``copycord.common.selfbot_headers``), like every
other import between copycord's subpackages.

Module-level state that selfbot_headers rebinds (the shared build) is read
through its functions (get_build_info / set_build_info), never re-exported as
a value, so a refresh is always visible here. ``selfbot_headers`` itself is
exported for anything that needs the module object.
"""

from __future__ import annotations

from copycord.common import selfbot_headers
from copycord.common.selfbot_headers import (
    CHROME_MAJOR,
    DEFAULT_BUILD,
    LOCALE,
    REAL_CLIENT_ABSENT,
    REAL_CLIENT_HEADERS,
    SUPPRESSED_PROFILE_HEADERS,
    TIMEZONE,
    TLS_IMPERSONATE,
    TLS_SESSION_IDLE_TTL,
    build_headers,
    channel_referer,
    close_all_tls_sessions,
    close_tls_session,
    context_properties,
    diff_against_real_client,
    gateway_properties,
    get_build_info,
    get_tls_session,
    make_fingerprint,
    reap_idle_tls_sessions,
    refresh_build_info,
    set_build_info,
    set_heartbeat_session_id,
)

__all__ = [
    "selfbot_headers",
    "CHROME_MAJOR",
    "DEFAULT_BUILD",
    "LOCALE",
    "REAL_CLIENT_ABSENT",
    "REAL_CLIENT_HEADERS",
    "SUPPRESSED_PROFILE_HEADERS",
    "TIMEZONE",
    "TLS_IMPERSONATE",
    "TLS_SESSION_IDLE_TTL",
    "build_headers",
    "channel_referer",
    "close_all_tls_sessions",
    "close_tls_session",
    "context_properties",
    "diff_against_real_client",
    "gateway_properties",
    "get_build_info",
    "get_tls_session",
    "make_fingerprint",
    "reap_idle_tls_sessions",
    "refresh_build_info",
    "set_build_info",
    "set_heartbeat_session_id",
]
