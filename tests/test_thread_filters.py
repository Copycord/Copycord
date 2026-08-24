"""
Filtering an individual thread or forum post.

The sitemap already honoured a thread's own id, but the message path judged a
thread purely by its parent channel, so adding a thread id to the block list
trimmed it from the sitemap and then forwarded its messages anyway.

These call the real rule rather than a copy of it.
"""
import pytest

pytest.importorskip(
    "discord", reason="client module needs a discord library present"
)

from client.sitemap import SitemapService  # noqa: E402

# Unbound: the rule reads only its arguments, so no instance is needed.
_filtered = SitemapService._is_filtered_out_view

CHANNEL, CATEGORY, THREAD, SIBLING = 100, 10, 555, 556


def _view(*, include=(), include_cats=(), exclude=(), exclude_cats=(), whitelist=False):
    return {
        "include_channel_ids": set(include),
        "include_category_ids": set(include_cats),
        "excluded_channel_ids": set(exclude),
        "excluded_category_ids": set(exclude_cats),
        "whitelist_enabled": whitelist,
    }


def _drop(view, *, thread=None, channel=CHANNEL, category=CATEGORY):
    return _filtered(None, channel, category, view, thread_id=thread)


class TestBlockingOneThread:
    def test_the_named_thread_is_dropped(self):
        assert _drop(_view(exclude=[THREAD]), thread=THREAD)

    def test_its_parent_channel_keeps_syncing(self):
        # The point of the feature: lose the thread, not the channel.
        assert not _drop(_view(exclude=[THREAD]))

    def test_a_sibling_thread_is_unaffected(self):
        assert not _drop(_view(exclude=[THREAD]), thread=SIBLING)


class TestParentsStillCoverTheirThreads:
    def test_blocking_a_channel_blocks_its_threads(self):
        assert _drop(_view(exclude=[CHANNEL]), thread=THREAD)

    def test_blocking_a_category_blocks_its_threads(self):
        assert _drop(_view(exclude_cats=[CATEGORY]), thread=THREAD)


class TestWhitelist:
    def test_a_named_thread_is_enough_on_its_own(self):
        view = _view(include=[THREAD], whitelist=True)
        assert not _drop(view, thread=THREAD)

    def test_naming_a_thread_does_not_admit_its_parent(self):
        view = _view(include=[THREAD], whitelist=True)
        assert _drop(view)

    def test_an_unlisted_thread_is_dropped(self):
        view = _view(include=[THREAD], whitelist=True)
        assert _drop(view, thread=SIBLING)

    def test_allowing_a_channel_allows_its_threads(self):
        view = _view(include=[CHANNEL], whitelist=True)
        assert not _drop(view, thread=THREAD)


class TestPrecedence:
    def test_a_named_thread_survives_an_excluded_parent(self):
        # Naming the thread is the more specific instruction.
        view = _view(include=[THREAD], exclude=[CHANNEL])
        assert not _drop(view, thread=THREAD)
        # …without letting the rest of that channel through.
        assert _drop(view)

    def test_include_beats_exclude_for_the_same_id(self):
        # Matches how a channel in both lists already behaves.
        view = _view(include=[THREAD], exclude=[THREAD])
        assert not _drop(view, thread=THREAD)


class TestNothingElseChanged:
    def test_no_filters_keeps_everything(self):
        assert not _drop(_view(), thread=THREAD)

    def test_plain_channel_blocking_is_untouched(self):
        assert _drop(_view(exclude=[CHANNEL]))

    def test_plain_category_blocking_is_untouched(self):
        assert _drop(_view(exclude_cats=[CATEGORY]))

    def test_whitelisted_category_still_admits_its_channels(self):
        view = _view(include_cats=[CATEGORY], whitelist=True)
        assert not _drop(view)


class _Guild:
    def __init__(self, gid):
        self.id = gid


class _Parent:
    def __init__(self, cid, category_id, guild):
        self.id = cid
        self.category_id = category_id
        self.guild = guild


class _Thread:
    """Enough of discord.Thread for the scope checks."""

    def __init__(self, tid, parent):
        self.id = tid
        self.parent = parent


class TestInScopeThread:
    """The gate every message inside a thread passes through.

    A forum post filtered out of the sitemap is simply never created, so it
    looked like thread filtering worked. A text-channel thread is created on
    demand from the message, so it needs this check to be thread-aware — it
    was not, and those messages were forwarded regardless.
    """

    def _service(self, view):
        import logging

        svc = SitemapService.__new__(SitemapService)
        svc.logger = logging.getLogger("test")
        # A guild with no mappings, so the guild-level view below is what gets
        # consulted. Without a real db here the new per-mapping lookup raises
        # into in_scope_thread's catch-all, which returns True -- so every
        # "stays in scope" case would pass without testing anything.
        svc.db = _FakeDB([], {})
        svc._build_filter_view_for_guild = lambda gid: view
        svc._build_filter_view_for_mapping = lambda gid, cid: view
        return svc

    def _thread(self):
        return _Thread(THREAD, _Parent(CHANNEL, CATEGORY, _Guild(1)))

    def test_a_blocked_thread_is_out_of_scope(self):
        svc = self._service(_view(exclude=[THREAD]))
        assert svc.in_scope_thread(self._thread()) is False

    def test_a_thread_in_an_allowed_channel_stays_in_scope(self):
        svc = self._service(_view(exclude=[SIBLING]))
        assert svc.in_scope_thread(self._thread()) is True

    def test_blocking_the_parent_still_blocks_the_thread(self):
        svc = self._service(_view(exclude=[CHANNEL]))
        assert svc.in_scope_thread(self._thread()) is False

    def test_a_whitelisted_thread_is_in_scope(self):
        svc = self._service(_view(include=[THREAD], whitelist=True))
        assert svc.in_scope_thread(self._thread()) is True

    def test_an_unlisted_thread_is_out_of_scope_under_whitelist(self):
        svc = self._service(_view(include=[SIBLING], whitelist=True))
        assert svc.in_scope_thread(self._thread()) is False

    def test_a_thread_with_no_parent_is_out_of_scope(self):
        svc = self._service(_view())
        assert svc.in_scope_thread(_Thread(THREAD, None)) is False


class _FakeDB:
    """Filters live per mapping; a guild-scoped read cannot see those rows."""

    def __init__(self, mappings, per_mapping):
        self._mappings = mappings
        self._per_mapping = per_mapping

    def list_guild_mappings(self):
        return self._mappings

    def get_filters(self, *, original_guild_id=None, cloned_guild_id=None):
        empty = {
            "whitelist": {"category": set(), "channel": set()},
            "exclude": {"category": set(), "channel": set()},
        }
        if cloned_guild_id is None:
            # Exactly what the real query does: mapping-scoped rows require a
            # clone id to match, so this comes back empty.
            return empty
        return self._per_mapping.get(
            (int(original_guild_id), int(cloned_guild_id)), empty
        )


class TestFiltersAreEvaluatedPerMapping:
    """The root cause behind thread filters appearing to do nothing.

    should_ignore() has no clone context, so scope was resolved at guild
    scope — a lookup that structurally cannot see mapping-scoped filter rows
    and therefore always answered "keep everything". Channel filters only
    looked like they worked because a filtered channel is never cloned, so its
    messages had nowhere to land. A text-channel thread is created on demand
    from the message, so nothing upstream covered for it.
    """

    ORIGIN, CLONE_A, CLONE_B = 700, 800, 801

    def _service(self, per_mapping, mappings=None):
        import logging

        svc = SitemapService.__new__(SitemapService)
        svc.logger = logging.getLogger("test")
        svc.db = _FakeDB(
            mappings
            if mappings is not None
            else [{"original_guild_id": self.ORIGIN, "cloned_guild_id": self.CLONE_A}],
            per_mapping,
        )
        return svc

    def _thread(self):
        return _Thread(THREAD, _Parent(CHANNEL, CATEGORY, _Guild(self.ORIGIN)))

    def test_a_mapping_scoped_thread_filter_is_honoured(self):
        svc = self._service(
            {(self.ORIGIN, self.CLONE_A): _blocklist(channels=[THREAD])}
        )
        assert svc.in_scope_thread(self._thread()) is False

    def test_a_mapping_scoped_channel_filter_is_honoured(self):
        svc = self._service(
            {(self.ORIGIN, self.CLONE_A): _blocklist(channels=[CHANNEL])}
        )
        assert svc.in_scope_thread(self._thread()) is False

    def test_an_unfiltered_thread_still_goes_through(self):
        svc = self._service(
            {(self.ORIGIN, self.CLONE_A): _blocklist(channels=[SIBLING])}
        )
        assert svc.in_scope_thread(self._thread()) is True

    def test_one_clone_still_wanting_it_keeps_the_message(self):
        # The client sends one payload per source message, so it may only be
        # dropped when no clone wants it.
        svc = self._service(
            {
                (self.ORIGIN, self.CLONE_A): _blocklist(channels=[THREAD]),
                (self.ORIGIN, self.CLONE_B): _blocklist(channels=[]),
            },
            mappings=[
                {"original_guild_id": self.ORIGIN, "cloned_guild_id": self.CLONE_A},
                {"original_guild_id": self.ORIGIN, "cloned_guild_id": self.CLONE_B},
            ],
        )
        assert svc.in_scope_thread(self._thread()) is True

    def test_dropped_only_when_every_clone_filters_it(self):
        svc = self._service(
            {
                (self.ORIGIN, self.CLONE_A): _blocklist(channels=[THREAD]),
                (self.ORIGIN, self.CLONE_B): _blocklist(channels=[THREAD]),
            },
            mappings=[
                {"original_guild_id": self.ORIGIN, "cloned_guild_id": self.CLONE_A},
                {"original_guild_id": self.ORIGIN, "cloned_guild_id": self.CLONE_B},
            ],
        )
        assert svc.in_scope_thread(self._thread()) is False


def _blocklist(*, channels=(), categories=()):
    return {
        "whitelist": {"category": set(), "channel": set()},
        "exclude": {"category": set(categories), "channel": set(channels)},
    }
