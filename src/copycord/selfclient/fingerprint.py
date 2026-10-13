# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Re-export of copycord.common.selfbot_headers for selfclient code.

The shared build info is read through get_build_info / set_build_info rather
than re-exported as a value, so a refresh is always visible here.
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
