"""The install key (common/install_identity.py) and the signed helper client
(common/helper_client.py), checked against a copy of the helper's verifier."""
import asyncio
import base64
import binascii
import hashlib
import json
import os
import socket
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from copycord.admin import app as app_module
from copycord.common import helper_client, install_identity
from copycord.version import package_version
from copycord.common.http_session import close_shared_session
from copycord.common.install_identity import KEY_FILENAME


@pytest.fixture(autouse=True)
def _fresh_identity(tmp_path):
    install_identity.reset()
    yield
    install_identity.reset()


# ── A copy of the helper's verifier ──────────────────────────────────────────
# copycord_helper src/app/services/role_claims.py (feature/role-claims):
# b64url_decode, signing_message and verify_signed_request. Copied, never
# imported, so a change on either side that breaks the wire format fails here.

class SignatureError(ValueError):
    pass


HDRS = ("X-Copycord-Install", "X-Copycord-Timestamp", "X-Copycord-Nonce",
        "X-Copycord-Version", "X-Copycord-Signature")


def _helper_b64url_decode(value):
    value = value.strip()
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError) as e:
        raise SignatureError("bad base64url") from e


def _helper_signing_message(method, path, timestamp, nonce, version, body):
    return "\n".join((
        method.upper(), path, timestamp, nonce, version,
        hashlib.sha256(body or b"").hexdigest(),
    )).encode("utf-8")


@pytest.fixture()
def helper_verify():
    import re
    nonce_re = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
    version_re = re.compile(r"^[0-9A-Za-z.+_-]{1,32}$")
    seen = set()

    def verify(*, method, path, headers, body, now=None):
        values = {}
        for name in HDRS:
            v = headers.get(name)
            if not v:
                raise SignatureError(f"missing {name}")
            values[name] = v.strip()
        raw_key = _helper_b64url_decode(values["X-Copycord-Install"])
        if len(raw_key) != 32:
            raise SignatureError("install key must be 32 bytes")
        sig = _helper_b64url_decode(values["X-Copycord-Signature"])
        if len(sig) != 64:
            raise SignatureError("signature must be 64 bytes")
        ts_raw = values["X-Copycord-Timestamp"]
        if not ts_raw.isdigit():
            raise SignatureError("bad timestamp")
        now = time.time() if now is None else now
        if abs(now - int(ts_raw)) > 300:
            raise SignatureError("timestamp outside the allowed window")
        nonce = values["X-Copycord-Nonce"]
        if not nonce_re.match(nonce):
            raise SignatureError("bad nonce")
        version = values["X-Copycord-Version"]
        if not version_re.match(version):
            raise SignatureError("bad version")
        msg = _helper_signing_message(method, path, ts_raw, nonce, version, body)
        try:
            Ed25519PublicKey.from_public_bytes(raw_key).verify(sig, msg)
        except (InvalidSignature, ValueError) as e:
            raise SignatureError("signature does not verify") from e
        install = base64.urlsafe_b64encode(raw_key).decode().rstrip("=")
        if (install, nonce) in seen:
            raise SignatureError("nonce already used")
        seen.add((install, nonce))
        return install, version

    return verify


# ── The install key ──────────────────────────────────────────────────────────

def test_key_is_created_once_and_reused(tmp_path):
    path = tmp_path / KEY_FILENAME
    assert not path.exists()
    key = install_identity.load_or_create(tmp_path)
    assert path.is_file()
    pem = path.read_bytes()
    assert pem.startswith(b"-----BEGIN PRIVATE KEY-----")
    assert install_identity.load_or_create(tmp_path) is key
    pub = install_identity.public_key_b64url()

    install_identity.reset()  # a restart reads the same key back
    install_identity.load_or_create(tmp_path)
    assert install_identity.public_key_b64url() == pub
    assert path.read_bytes() == pem
    assert len(base64.urlsafe_b64decode(pub + "=")) == 32
    assert "=" not in pub
    assert [p.name for p in tmp_path.iterdir()] == [KEY_FILENAME]  # no temp files left


@pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")
def test_key_file_is_0600(tmp_path):
    install_identity.load_or_create(tmp_path)
    assert (tmp_path / KEY_FILENAME).stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("junk", [b"", b"not a key", b"-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"])
def test_corrupt_key_is_regenerated(tmp_path, junk, caplog):
    path = tmp_path / KEY_FILENAME
    path.write_bytes(junk)
    key = install_identity.load_or_create(tmp_path)
    assert "corrupt" in caplog.text
    assert path.read_bytes() != junk
    install_identity.reset()
    again = install_identity.load_or_create(tmp_path)
    assert again.public_key().public_bytes_raw() == key.public_key().public_bytes_raw()


def test_non_ed25519_key_is_regenerated(tmp_path, caplog):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    other = ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    (tmp_path / KEY_FILENAME).write_bytes(other)
    install_identity.load_or_create(tmp_path)
    assert "not an Ed25519 key" in caplog.text
    assert (tmp_path / KEY_FILENAME).read_bytes() != other


def test_admin_startup_creates_the_key(tmp_path, monkeypatch):
    """An upgraded install has no key yet; its first start makes one."""
    monkeypatch.setattr(app_module, "DATA_DIR", tmp_path)
    asyncio.run(app_module._load_install_key())
    assert (tmp_path / KEY_FILENAME).is_file()


def test_sign_verifies_with_the_public_key(tmp_path):
    install_identity.load_or_create(tmp_path)
    raw = base64.urlsafe_b64decode(install_identity.public_key_b64url() + "=")
    sig = install_identity.sign(b"hello")
    assert len(sig) == 64
    Ed25519PublicKey.from_public_bytes(raw).verify(sig, b"hello")


# ── Signing ──────────────────────────────────────────────────────────────────

def test_signed_headers_verify_against_the_helper(tmp_path, helper_verify):
    install_identity.load_or_create(tmp_path)
    body = b'{"code":"ABCD"}'
    headers = helper_client.signed_headers("POST", "/api/public/redeem/start", body)
    install, version = helper_verify(method="POST", path="/api/public/redeem/start",
                                     headers=headers, body=body)
    assert install == install_identity.public_key_b64url()
    assert version == helper_client.VERSION
    assert version.startswith("v" + package_version().split("+")[0])
    assert headers["X-Copycord-Timestamp"].isdigit()


@pytest.mark.parametrize("change", ["method", "path", "body", "version", "old"])
def test_helper_rejects_a_changed_request(tmp_path, helper_verify, change):
    install_identity.load_or_create(tmp_path)
    path, body = "/api/public/role-events", b""
    headers = helper_client.signed_headers("GET", path, body)
    kwargs = dict(method="GET", path=path, headers=dict(headers), body=body)
    if change == "method":
        kwargs["method"] = "POST"
    elif change == "path":
        kwargs["path"] = path + "?version=1"
    elif change == "body":
        kwargs["body"] = b"{}"
    elif change == "version":
        kwargs["headers"]["X-Copycord-Version"] = "9.9.9"
    else:
        kwargs["now"] = time.time() + 301
    with pytest.raises(SignatureError):
        helper_verify(**kwargs)


def test_empty_body_hash_matches_the_contract():
    msg = helper_client.signing_message("get", "/p", "1", "n", "v", b"")
    assert msg == b"GET\n/p\n1\nn\nv\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


# ── Configuration ────────────────────────────────────────────────────────────

def test_helper_url(monkeypatch):
    monkeypatch.delenv(helper_client.HELPER_URL_ENV, raising=False)
    assert helper_client.helper_url() == helper_client.DEFAULT_HELPER_URL == "https://copycord.xyz"
    assert helper_client.helper_url({"COPYCORD_HELPER_URL": ""}) == ""
    assert helper_client.helper_url({"COPYCORD_HELPER_URL": "  "}) == ""
    assert helper_client.helper_url({"COPYCORD_HELPER_URL": "http://h:8080/x/"}) == "http://h:8080/x"


# ── The client against a local fake helper ───────────────────────────────────

def _fake_helper(helper_verify, seen, responses):
    """A helper that verifies like the real one and answers from responses[path]."""
    async def handle(request):
        body = await request.read()
        try:
            install, version = helper_verify(method=request.method, path=request.raw_path,
                                             headers=request.headers, body=body)
        except SignatureError as e:
            return web.json_response({"error": f"bad signature: {e}"}, status=401)
        seen.append({"method": request.method, "raw_path": request.raw_path, "body": body,
                     "install": install, "version": version,
                     "content_type": request.headers.get("Content-Type")})
        status, payload = responses[request.path]
        if isinstance(payload, str):
            return web.Response(text=payload, status=status)
        return web.json_response(payload, status=status)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handle)
    return app


def _run_against(helper_verify, responses, call, prefix=""):
    seen = []

    async def main():
        server = TestServer(_fake_helper(helper_verify, seen, responses))
        await server.start_server()
        try:
            os.environ[helper_client.HELPER_URL_ENV] = str(server.make_url(prefix)).rstrip("/")
            return await call()
        finally:
            await close_shared_session()
            await server.close()

    try:
        return asyncio.run(main()), seen
    finally:
        os.environ.pop(helper_client.HELPER_URL_ENV, None)


def test_start_claim_is_signed_with_its_body(tmp_path, helper_verify):
    install_identity.load_or_create(tmp_path)
    claimed = {"status": "claimed", "message": "Role claimed", "reward": "role"}
    result, seen = _run_against(
        helper_verify, {"/api/public/redeem/start": (200, claimed)},
        lambda: helper_client.start_claim("ABCD-EFGH"))
    assert result == claimed
    assert seen[0]["method"] == "POST"
    assert seen[0]["raw_path"] == "/api/public/redeem/start"
    assert json.loads(seen[0]["body"]) == {"code": "ABCD-EFGH"}
    assert seen[0]["content_type"] == "application/json"


def test_base_url_path_is_part_of_the_signed_path(tmp_path, helper_verify):
    install_identity.load_or_create(tmp_path)
    claimed = {"status": "claimed", "message": "Role claimed", "reward": "role"}
    result, seen = _run_against(
        helper_verify, {"/helper/api/public/redeem/start": (200, claimed)},
        lambda: helper_client.start_claim("x"), prefix="/helper")
    assert result == claimed
    assert seen[0]["raw_path"] == "/helper/api/public/redeem/start"


@pytest.mark.parametrize("status,payload,message", [
    (400, {"error": "Invalid code", "status": "invalid"}, "Invalid code"),
    (409, {"error": "Role already claimed", "status": "already"}, "Role already claimed"),
    (400, {"error": "Join the Copycord Discord server first", "status": "error"},
     "Join the Copycord Discord server first"),
    (502, {"error": "Could not add the role, try again later", "status": "error"},
     "Could not add the role, try again later"),
    (429, {"error": "too many wrong codes, try again later"}, "too many wrong codes, try again later"),
    (500, "<html>oops</html>", "Server error (HTTP 500)"),
])
def test_helper_errors_pass_through(tmp_path, helper_verify, status, payload, message):
    install_identity.load_or_create(tmp_path)

    async def call():
        with pytest.raises(helper_client.HelperError) as info:
            await helper_client.start_claim("x")
        return info.value

    err, _ = _run_against(helper_verify, {"/api/public/redeem/start": (status, payload)}, call)
    assert err.status == status
    assert err.message == message


def test_bad_success_body_is_an_error(tmp_path, helper_verify):
    install_identity.load_or_create(tmp_path)

    async def call():
        with pytest.raises(helper_client.HelperError) as info:
            await helper_client.start_claim("x")
        return info.value

    err, _ = _run_against(helper_verify, {"/api/public/redeem/start": (200, "not json")}, call)
    assert err.message == helper_client.BAD_RESPONSE


def test_unreachable_helper(tmp_path, monkeypatch):
    install_identity.load_or_create(tmp_path)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    monkeypatch.setenv(helper_client.HELPER_URL_ENV, f"http://127.0.0.1:{port}")

    async def call():
        try:
            with pytest.raises(helper_client.HelperError) as info:
                await helper_client.start_claim("x")
            return info.value
        finally:
            await close_shared_session()

    err = asyncio.run(call())
    assert err.message == "Server is down"
    assert err.status is None
