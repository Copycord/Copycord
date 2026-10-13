"""
Recognising a dead proxy so the client rotates instead of giving up.

A 502 from the proxy at startup killed the client outright: it logged
"Unexpected error while running client" and shut down, rather than trying
another route. Two separate reasons the check missed it.

The client already knows how to rotate on a proxy failure. It just never got
told that this was one.
"""
import pytest

import copycord.client.client as cc  # noqa: E402

_is_proxy_error = cc.ClientListener._is_proxy_error


def _chained(inner: BaseException, outer: BaseException) -> BaseException:
    """Raise ``outer`` while handling ``inner`` so __context__ is set for real."""
    try:
        raise inner
    except type(inner):
        try:
            raise outer
        except type(outer) as e:
            return e


class TestTheReportedCrash:
    def test_an_attributeerror_over_a_curl_failure_is_a_proxy_error(self):
        # curl_cffi fails the CONNECT tunnel; the retry path then reads
        # self.ws.sequence while self.ws is still None, so the AttributeError
        # propagates and the real cause survives only as __context__.
        from curl_cffi.curl import CurlError

        exc = _chained(
            CurlError(
                "Failed to perform, curl: (7) CONNECT tunnel failed, response 502."
            ),
            AttributeError("'NoneType' object has no attribute 'sequence'"),
        )
        assert _is_proxy_error(None, exc) is True

    def test_a_bare_curl_error_is_a_proxy_error(self):
        # CurlError inherits from Exception, not OSError, so an isinstance
        # test against the socket error types never matched it, even though
        # curl_cffi raises it for every gateway dial through a dead proxy.
        from curl_cffi.curl import CurlError

        assert not issubclass(CurlError, OSError)
        exc = CurlError("curl: (7) CONNECT tunnel failed, response 502.")
        assert _is_proxy_error(None, exc) is True


class TestWhatItStillCatches:
    def test_the_gateway_watchdog_timeout(self):
        assert _is_proxy_error(None, cc.GatewayConnectTimeout("no connect")) is True

    @pytest.mark.parametrize(
        "message",
        [
            "proxy connect failed",
            "Cannot connect to host",
            "connection aborted",
            "curl: (28) timed out",
        ],
    )
    def test_socket_errors_naming_a_route_failure(self, message):
        assert _is_proxy_error(None, OSError(message)) is True


class TestWhatItMustNotSwallow:
    def test_an_unrelated_error_is_left_alone(self):
        # Classifying everything as a proxy problem would hide real bugs
        # behind an endless rotation.
        assert _is_proxy_error(None, ValueError("bad value")) is False

    def test_an_unrelated_attributeerror_is_left_alone(self):
        # The fix keys on the chained cause, not on AttributeError itself.
        exc = AttributeError("'Foo' object has no attribute 'bar'")
        assert _is_proxy_error(None, exc) is False

    def test_a_wrapper_over_an_unrelated_error_is_left_alone(self):
        exc = _chained(ValueError("real bug"), AttributeError("secondary"))
        assert _is_proxy_error(None, exc) is False


class TestChainWalking:
    def test_it_reaches_a_cause_several_links_down(self):
        from curl_cffi.curl import CurlError

        inner = _chained(
            CurlError("curl: (7) CONNECT tunnel failed"), RuntimeError("mid")
        )
        outer = _chained(inner, AttributeError("outer"))
        assert _is_proxy_error(None, outer) is True

    def test_an_explicit_cause_is_followed(self):
        from curl_cffi.curl import CurlError

        exc = AttributeError("outer")
        exc.__cause__ = CurlError("curl: (7) CONNECT tunnel failed")
        assert _is_proxy_error(None, exc) is True

    def test_a_self_referential_chain_terminates(self):
        # A cycle here would hang the client on its own error handler.
        exc = ValueError("loop")
        exc.__context__ = exc
        assert _is_proxy_error(None, exc) is False

    def test_a_very_deep_chain_terminates(self):
        exc = ValueError("bottom")
        for i in range(50):
            nxt = ValueError(f"link {i}")
            nxt.__context__ = exc
            exc = nxt
        assert _is_proxy_error(None, exc) is False


class TestMaskedProxiesAreDistinguishable:
    """Rotating-proxy pools differ only in the credentials.

    Every entry shares a host and port and carries its own session id, so
    masking the credentials alone printed all of them identically and a
    rotation log could not be told from a loop on one dead proxy.
    """

    A = (
        "http://np_x-country-PL-session-EYEK4GQ5-time-1440"
        ":pH4sVLL1oZpRHPGQ@eu-1.nodeproxies.xyz:8080"
    )
    B = (
        "http://np_x-country-PL-session-VUE4IMTJ-time-1440"
        ":pH4sVLL1oZpRHPGQ@eu-1.nodeproxies.xyz:8080"
    )

    def test_two_sessions_on_one_host_look_different(self):
        from copycord.client.proxy_rotator import _mask_proxy_url

        assert _mask_proxy_url(self.A) != _mask_proxy_url(self.B)

    def test_the_same_proxy_always_looks_the_same(self):
        # Otherwise a log cannot be read as "back on the one from earlier".
        from copycord.client.proxy_rotator import _mask_proxy_url

        assert _mask_proxy_url(self.A) == _mask_proxy_url(self.A)

    def test_no_credentials_reach_the_log(self):
        from copycord.client.proxy_rotator import _mask_proxy_url

        out = _mask_proxy_url(self.A)
        assert "pH4sVLL1oZpRHPGQ" not in out
        assert "EYEK4GQ5" not in out
        assert "eu-1.nodeproxies.xyz:8080" in out

    def test_a_proxy_without_credentials_still_gets_a_marker(self):
        from copycord.client.proxy_rotator import _mask_proxy_url

        out = _mask_proxy_url("http://1.2.3.4:8080")
        assert "1.2.3.4:8080" in out
        assert out != "http://1.2.3.4:8080"
