"""This install's Ed25519 identity for signed requests to the Copycord helper.

The key lives in <data dir>/install_key.pem (PKCS8 PEM, 0600) and is created on
the first start that finds none, so fresh installs and upgrades both get one. The
raw 32-byte public key, base64url encoded, is the install id the helper knows us
by. A key file that does not load is logged and replaced, never a crash.
"""
from __future__ import annotations

import base64
import logging
import threading
from pathlib import Path
from typing import Optional, Union

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from copycord.common.paths import data_dir
from copycord.common.private_file import write_private_file

logger = logging.getLogger("common.install_identity")

KEY_FILENAME = "install_key.pem"

_lock = threading.Lock()
_key: Optional[Ed25519PrivateKey] = None
_path: Optional[Path] = None


def key_path(directory: Union[str, Path, None] = None) -> Path:
    return Path(directory if directory is not None else data_dir()) / KEY_FILENAME


def _read(path: Path) -> Optional[Ed25519PrivateKey]:
    """The key in path, or None when there is none or it does not load."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError:
        logger.warning("Install key %s could not be read, generating a new one", path, exc_info=True)
        return None
    try:
        key = serialization.load_pem_private_key(data, password=None)
    except Exception:
        logger.warning("Install key %s is corrupt, generating a new one", path)
        return None
    if not isinstance(key, Ed25519PrivateKey):
        logger.warning("Install key %s is not an Ed25519 key, generating a new one", path)
        return None
    return key


def _create(path: Path) -> Ed25519PrivateKey:
    key = Ed25519PrivateKey.generate()
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    try:
        write_private_file(path, pem)
        logger.info("Created install key %s", path)
    except OSError:
        # Still usable for this run; the next start tries to save one again.
        logger.exception("Could not save install key %s", path)
    return key


def load_or_create(directory: Union[str, Path, None] = None) -> Ed25519PrivateKey:
    """The install key, loaded or created on first use and then cached.

    directory defaults to the one from the last call, else the data dir.
    """
    global _key, _path
    with _lock:
        if directory is None and _path is not None:
            path = _path
        else:
            path = key_path(directory).resolve()
        if _key is not None and path == _path:
            return _key
        key = _read(path) or _create(path)
        _key, _path = key, path
        return key


def reset() -> None:
    """Forget the cached key (tests)."""
    global _key, _path
    with _lock:
        _key, _path = None, None


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def public_key_b64url() -> str:
    """The install id: the raw 32-byte public key, base64url without padding."""
    return b64url(load_or_create().public_key().public_bytes_raw())


def sign(message: bytes) -> bytes:
    """The 64-byte Ed25519 signature of message."""
    return load_or_create().sign(message)
