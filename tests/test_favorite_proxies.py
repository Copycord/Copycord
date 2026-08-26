"""
Favorite proxies for the client's gateway connection.

A starred proxy is preferred whenever it is usable. When none is, the client
falls back to an ordinary proxy rather than waiting -- staying connected
matters more than staying on the preferred route -- and an optional task
re-tests the favorites and moves back as soon as one answers.

These call the real rotator and the real client methods.
"""
import asyncio
import types

import pytest

from client.proxy_rotator import ProxyRotator

A = "http://a.example:8080"
B = "http://b.example:8080"
C = "http://c.example:8080"


def _rotator(proxies=(A, B, C), favorites=()):
    r = ProxyRotator()
    r.reload(list(proxies))
    r.set_enabled(True)
    r.set_favorites(list(favorites))
    return r


class TestFavoritesAreParsedLikeProxies:
    def test_raw_forms_are_normalised(self):
        # The UI stores what the user typed; the rotator works in normalised
        # URLs. Comparing the two directly would match nothing at all.
        r = ProxyRotator()
        r.reload(["user:pass@host.example:8080"])
        r.set_favorites(["user:pass@host.example:8080"])
        assert r.favorites == ["http://user:pass@host.example:8080"]
        assert r.has_favorites is True

    def test_unparseable_favorites_are_dropped(self):
        r = _rotator(favorites=["", "   ", "not a proxy"])
        assert r.favorites == []

    def test_a_favorite_not_in_the_list_does_not_count(self):
        # Otherwise the client believes it has a preference it can never use,
        # and the re-check task would run forever against nothing.
        r = _rotator(proxies=(A, B), favorites=(C,))
        assert r.has_favorites is False


class TestSelectionPrefersFavorites:
    def test_a_favorite_is_chosen_over_others(self):
        r = _rotator(favorites=(C,))
        assert r.next() == C

    def test_choice_stays_among_favorites_when_several_exist(self):
        r = _rotator(favorites=(B, C))
        picks = set()
        for _ in range(40):
            r._current_proxy = None
            picks.add(r.next())
        assert picks <= {B, C}, picks

    def test_without_favorites_nothing_changes(self):
        r = _rotator()
        picks = set()
        for _ in range(60):
            r._current_proxy = None
            picks.add(r.next())
        assert picks == {A, B, C}

    def test_a_suspended_favorite_falls_back_to_a_normal_proxy(self):
        # Staying connected beats staying preferred.
        r = _rotator(favorites=(C,))
        for _ in range(ProxyRotator.MAX_FAILURES):
            r.report_failure(C)
        r._current_proxy = None
        assert r.next() in (A, B)

    def test_rotation_also_prefers_favorites(self):
        # Timed rotation must not quietly walk off the preferred proxy.
        r = _rotator(favorites=(B, C))
        r.next()
        for _ in range(20):
            assert r.rotate_now() in (B, C)


class TestFallbackDetection:
    def test_no_favorites_means_never_on_fallback(self):
        r = _rotator()
        r.next()
        assert r.on_fallback() is False

    def test_sitting_on_a_favorite_is_not_a_fallback(self):
        r = _rotator(favorites=(C,))
        assert r.next() == C
        assert r.on_fallback() is False

    def test_sitting_on_a_normal_proxy_is_a_fallback(self):
        r = _rotator(favorites=(C,))
        for _ in range(ProxyRotator.MAX_FAILURES):
            r.report_failure(C)
        r._current_proxy = None
        r.next()
        assert r.on_fallback() is True

    def test_healthy_favorites_reports_only_usable_ones(self):
        r = _rotator(favorites=(B, C))
        for _ in range(ProxyRotator.MAX_FAILURES):
            r.report_failure(B)
        assert r.healthy_favorites() == [C]

    def test_clearing_a_suspension_makes_it_selectable_again(self):
        # The re-check task proves a proxy works, so waiting out the rest of
        # the suspension would ignore evidence we just gathered.
        r = _rotator(favorites=(C,))
        for _ in range(ProxyRotator.MAX_FAILURES):
            r.report_failure(C)
        assert r.healthy_favorites() == []
        r.clear_suspension(C)
        assert r.healthy_favorites() == [C]


pytest.importorskip("discord", reason="client module needs a discord library")
import client.client as cc  # noqa: E402


class _FakeWS:
    def __init__(self):
        self.closed_with = None

    async def close(self, code=1000):
        self.closed_with = code


def _listener(rotator, *, enabled=True, interval=1):
    self = cc.ClientListener.__new__(cc.ClientListener)
    self.proxy_rotator = rotator
    self._favorite_recheck_enabled = enabled
    self._favorite_recheck_interval = interval
    self.bot = types.SimpleNamespace(
        http=types.SimpleNamespace(proxy=None), ws=_FakeWS()
    )
    return self


class TestSwitchingBack:
    @pytest.mark.asyncio
    async def test_it_moves_the_live_gateway_not_just_http(self):
        # Setting http.proxy alone leaves the existing websocket on the old
        # route: it was dialled through that proxy and stays there. The socket
        # has to be closed for discord.py's connect loop to redial.
        r = _rotator(favorites=(C,))
        self = _listener(r)
        await self._switch_to_proxy(C)
        assert self.bot.http.proxy == C
        assert self.bot.ws.closed_with == 4000

    @pytest.mark.asyncio
    async def test_a_clean_close_code_is_not_used(self):
        # discord.py treats 1000 as a deliberate shutdown and will not
        # reconnect, which would leave the client offline rather than moved.
        r = _rotator(favorites=(C,))
        self = _listener(r)
        await self._switch_to_proxy(C)
        assert self.bot.ws.closed_with != 1000

    @pytest.mark.asyncio
    async def test_it_survives_having_no_socket_yet(self):
        r = _rotator(favorites=(C,))
        self = _listener(r)
        self.bot.ws = None
        await self._switch_to_proxy(C)
        assert self.bot.http.proxy == C

    @pytest.mark.asyncio
    async def test_the_rotator_is_told_about_the_move(self):
        # Otherwise on_fallback() still reports a fallback and the task keeps
        # switching to a proxy it is already on.
        r = _rotator(favorites=(C,))
        self = _listener(r)
        await self._switch_to_proxy(C)
        assert r._current_proxy == C
        assert r.on_fallback() is False


class TestTheRecheckLoop:
    async def _run_once(self, self_obj, probe_results):
        """Drive exactly one pass of the loop and stop it."""
        probed = []

        async def fake_probe(proxy):
            probed.append(proxy)
            return probe_results.get(proxy, False)

        self_obj._probe_proxy = fake_probe
        task = asyncio.create_task(self_obj._favorite_recheck_loop())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return probed

    def _on_fallback(self, favorites=(C,)):
        r = _rotator(favorites=favorites)
        for f in favorites:
            for _ in range(ProxyRotator.MAX_FAILURES):
                r.report_failure(f)
        r._current_proxy = None
        r.next()
        assert r.on_fallback() is True
        return r

    @pytest.mark.asyncio
    async def test_it_switches_back_when_a_favorite_answers(self):
        r = self._on_fallback()
        listener = _listener(r, interval=0.01)
        probed = await self._run_once(listener, {C: True})
        assert C in probed
        assert listener.bot.http.proxy == C
        assert r.on_fallback() is False

    @pytest.mark.asyncio
    async def test_a_failing_favorite_leaves_us_where_we_are(self):
        r = self._on_fallback()
        before = r._current_proxy
        listener = _listener(r, interval=0.01)
        await self._run_once(listener, {C: False})
        assert r._current_proxy == before
        assert listener.bot.ws.closed_with is None

    @pytest.mark.asyncio
    async def test_it_does_nothing_while_the_toggle_is_off(self):
        r = self._on_fallback()
        listener = _listener(r, enabled=False, interval=0.01)
        probed = await self._run_once(listener, {C: True})
        assert probed == []

    @pytest.mark.asyncio
    async def test_it_does_not_probe_while_already_on_a_favorite(self):
        r = _rotator(favorites=(C,))
        assert r.next() == C
        listener = _listener(r, interval=0.01)
        probed = await self._run_once(listener, {C: True})
        assert probed == []

    @pytest.mark.asyncio
    async def test_it_does_not_probe_when_no_favorites_are_set(self):
        r = _rotator()
        r.next()
        listener = _listener(r, interval=0.01)
        probed = await self._run_once(listener, {A: True, B: True, C: True})
        assert probed == []

    @pytest.mark.asyncio
    async def test_a_probe_failure_does_not_kill_the_loop(self):
        # The task runs for the process lifetime; one bad probe must not end
        # it, or the client silently never returns to a favorite again.
        r = self._on_fallback()
        listener = _listener(r, interval=0.01)

        async def boom(proxy):
            raise OSError("network down")

        listener._probe_proxy = boom
        task = asyncio.create_task(listener._favorite_recheck_loop())
        await asyncio.sleep(0.05)
        assert not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
