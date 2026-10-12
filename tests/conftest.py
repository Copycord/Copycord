"""
Shared fixtures for Copycord tests.
"""
import os
import sys
import tempfile
import pytest

# Add the code/ directory to sys.path so `from common.db import DBManager` works
CODE_DIR = os.path.join(os.path.dirname(__file__), "..", "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(CODE_DIR))

# Modules such as admin.app create the data dir at import time; point it at a
# session temp dir before any test imports them (never /data or C:\data).
_SESSION_DATA_DIR = tempfile.mkdtemp(prefix="copycord-test-data-")
os.environ["DATA_DIR"] = _SESSION_DATA_DIR


@pytest.fixture()
def tmp_db_path(tmp_path):
    """Return a temporary database file path."""
    return str(tmp_path / "test.db")


@pytest.fixture()
def db(tmp_db_path):
    """Create a fresh DBManager with schema initialized."""
    from common.db import DBManager
    return DBManager(tmp_db_path, init_schema=True)


def pytest_sessionfinish(session, exitstatus):
    import shutil

    shutil.rmtree(_SESSION_DATA_DIR, ignore_errors=True)
