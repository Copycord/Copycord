# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Old browser backfill frames rebuilt from the job frames.

This module exists only until the Channels page's backfill UI is removed
(plan L11) and must be deleted then. It maps backfill_job, backfill_job_progress,
backfill_job_cleanup and backfill_job_end (keyed by job_id "<run_id>:<channel_id>")
to backfill_started, backfill_progress, backfill_cleanup and backfill_done in the
shapes BackfillTracker and BackfillManager published them.
"""


def _channel_of(data: dict) -> str | None:
    cid = data.get("original_channel_id")
    if cid is not None:
        return str(cid)
    job_id = str(data.get("job_id") or "")
    if ":" in job_id:
        return job_id.rsplit(":", 1)[1] or None
    return None


def legacy_frame(msg: dict) -> dict | None:
    """The old browser frame for one job frame, or None when it has no old twin."""
    typ = msg.get("type")
    data = msg.get("data") or {}
    job_id = str(data.get("job_id") or "") or None
    cid = _channel_of(data)
    if not job_id or cid is None:
        return None

    if typ == "backfill_job":
        if data.get("state") != "running":
            return None
        return {
            "type": "backfill_started",
            "task_id": job_id,
            "data": {"channel_id": cid},
        }

    if typ == "backfill_job_progress":
        out = {"channel_id": cid}
        pulled = data.get("pulled")
        if pulled is not None:
            out["delivered"] = int(pulled)
            out["sent"] = int(pulled)
        total = data.get("total_estimate")
        if total is not None:
            out["total"] = int(total)
        return {"type": "backfill_progress", "task_id": job_id, "data": out}

    if typ == "backfill_job_cleanup":
        return {
            "type": "backfill_cleanup",
            "channel_id": cid,
            "task_id": job_id,
            "data": {"channel_id": cid, "task_id": job_id, "state": "waiting"},
        }

    if typ == "backfill_job_end":
        return {
            "type": "backfill_done",
            "channel_id": cid,
            "task_id": job_id,
            "data": {
                "channel_id": cid,
                "task_id": job_id,
                "sent": int(data.get("delivered") or 0),
                "total": None,
            },
        }

    return None
