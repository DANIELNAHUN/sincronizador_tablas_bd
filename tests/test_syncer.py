"""Unit tests for db_table_sync — tasks 6.1 through 6.11."""
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd

from db_table_sync import Syncer, get_engine_from_env


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_users_table(engine, rows=3):
    """Create a 'users' table and insert *rows* rows into *engine*."""
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
        for i in range(1, rows + 1):
            conn.execute(text(f"INSERT INTO users (id, name) VALUES ({i}, 'user{i}')"))
        conn.commit()


# ---------------------------------------------------------------------------
# 6.1 — Initialization
# ---------------------------------------------------------------------------

def test_init_with_engine():
    """Syncer accepts Engine instances directly."""
    src = create_engine("sqlite:///:memory:")
    tgt = create_engine("sqlite:///:memory:")
    syncer = Syncer(
        source=src,
        target=tgt,
        source_table="t",
        column_list=["id"],
    )
    assert isinstance(syncer._source_engine, Engine)
    assert isinstance(syncer._target_engine, Engine)


def test_init_with_conn_string():
    """Syncer accepts connection strings and converts them to Engine instances."""
    syncer = Syncer(
        source="sqlite:///:memory:",
        target="sqlite:///:memory:",
        source_table="t",
        column_list=["id"],
    )
    assert isinstance(syncer._source_engine, Engine)
    assert isinstance(syncer._target_engine, Engine)


# ---------------------------------------------------------------------------
# 6.2 — target_table defaults
# ---------------------------------------------------------------------------

def test_target_table_defaults_to_source_table():
    """When target_table is None, _target_table equals _source_table."""
    syncer = Syncer(
        source="sqlite:///:memory:",
        target="sqlite:///:memory:",
        source_table="my_table",
        column_list=["id"],
    )
    assert syncer._target_table == syncer._source_table


# ---------------------------------------------------------------------------
# 6.3 — if_exists default
# ---------------------------------------------------------------------------

def test_if_exists_defaults_to_append():
    """Default value of _if_exists is 'append'."""
    syncer = Syncer(
        source="sqlite:///:memory:",
        target="sqlite:///:memory:",
        source_table="t",
        column_list=["id"],
    )
    assert syncer._if_exists == "append"


# ---------------------------------------------------------------------------
# 6.4 — Empty column_list raises ValueError
# ---------------------------------------------------------------------------

def test_empty_column_list_raises_value_error():
    """ValueError is raised when column_list is empty."""
    with pytest.raises(ValueError):
        Syncer(
            source="sqlite:///:memory:",
            target="sqlite:///:memory:",
            source_table="t",
            column_list=[],
        )


# ---------------------------------------------------------------------------
# 6.5 — Invalid column_mapping key raises ValueError
# ---------------------------------------------------------------------------

def test_invalid_column_mapping_key_raises_value_error():
    """ValueError is raised when column_mapping contains a key not in column_list."""
    with pytest.raises(ValueError):
        Syncer(
            source="sqlite:///:memory:",
            target="sqlite:///:memory:",
            source_table="t",
            column_list=["id"],
            column_mapping={"nonexistent": "x"},
        )


# ---------------------------------------------------------------------------
# 6.6 — Invalid if_exists raises ValueError
# ---------------------------------------------------------------------------

def test_invalid_if_exists_raises_value_error():
    """ValueError is raised when if_exists is not one of the allowed values."""
    with pytest.raises(ValueError):
        Syncer(
            source="sqlite:///:memory:",
            target="sqlite:///:memory:",
            source_table="t",
            column_list=["id"],
            if_exists="upsert",
        )


# ---------------------------------------------------------------------------
# 6.7 — get_engine_from_env with present variable
# ---------------------------------------------------------------------------

def test_get_engine_from_env_with_present_variable(monkeypatch):
    """get_engine_from_env returns an Engine when the variable is set."""
    monkeypatch.setenv("TEST_DB_URL", "sqlite:///:memory:")
    engine = get_engine_from_env("TEST_DB_URL")
    assert isinstance(engine, Engine)


# ---------------------------------------------------------------------------
# 6.8 — get_engine_from_env with missing variable
# ---------------------------------------------------------------------------

def test_get_engine_from_env_with_missing_variable(monkeypatch):
    """get_engine_from_env raises KeyError when the variable is absent."""
    monkeypatch.delenv("MISSING_DB_VAR", raising=False)
    with pytest.raises(KeyError):
        get_engine_from_env("MISSING_DB_VAR")


# ---------------------------------------------------------------------------
# 6.9 — End-to-end sync
# ---------------------------------------------------------------------------

def test_sync_end_to_end():
    """sync() inserts all rows and returns the correct row count."""
    src_engine = create_engine("sqlite:///:memory:")
    tgt_engine = create_engine("sqlite:///:memory:")
    _make_users_table(src_engine, rows=3)

    syncer = Syncer(
        source=src_engine,
        target=tgt_engine,
        source_table="users",
        column_list=["id", "name"],
    )
    result = syncer.sync()

    assert result == 3

    with tgt_engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
    assert rows == 3


# ---------------------------------------------------------------------------
# 6.10 — Sync with column_mapping
# ---------------------------------------------------------------------------

def test_sync_with_column_mapping():
    """sync() renames columns according to column_mapping."""
    src_engine = create_engine("sqlite:///:memory:")
    tgt_engine = create_engine("sqlite:///:memory:")
    _make_users_table(src_engine, rows=3)

    syncer = Syncer(
        source=src_engine,
        target=tgt_engine,
        source_table="users",
        column_list=["id", "name"],
        column_mapping={"id": "user_id", "name": "user_name"},
    )
    syncer.sync()

    with tgt_engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM users"), con=conn)

    assert "user_id" in df.columns
    assert "user_name" in df.columns


# ---------------------------------------------------------------------------
# 6.11 — RuntimeError on missing source table
# ---------------------------------------------------------------------------

def test_sync_runtime_error_on_missing_source_table():
    """sync() raises RuntimeError when the source table does not exist."""
    syncer = Syncer(
        source="sqlite:///:memory:",
        target="sqlite:///:memory:",
        source_table="nonexistent_table",
        column_list=["id"],
    )
    with pytest.raises(RuntimeError):
        syncer.sync()
