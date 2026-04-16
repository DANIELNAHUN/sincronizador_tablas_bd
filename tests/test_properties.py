"""Property-based tests for db_table_sync — tasks 7.1 through 7.7."""
import logging

import pytest
from hypothesis import given, settings, HealthCheck, strategies as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from db_table_sync import Syncer

settings.register_profile("ci", max_examples=100, suppress_health_check=[HealthCheck.too_slow])
settings.load_profile("ci")

# ---------------------------------------------------------------------------
# Shared alphabet for valid SQL identifiers
# ---------------------------------------------------------------------------
# SQL identifiers must start with a letter or underscore, then letters/digits/underscore.
# We build a strategy that guarantees the first character is a letter or underscore.
_LETTER_OR_UNDERSCORE = st.characters(
    whitelist_categories=("Lu", "Ll"),
    whitelist_characters="_",
)
_IDENT_TAIL_CHAR = st.characters(
    whitelist_categories=("Lu", "Ll", "Nd"),
    whitelist_characters="_",
)
_IDENT = st.builds(
    lambda head, tail: head + tail,
    head=_LETTER_OR_UNDERSCORE,
    tail=st.text(alphabet=_IDENT_TAIL_CHAR, max_size=10),
)


# ---------------------------------------------------------------------------
# 7.1 — Property 1: Cadenas de conexión producen engines válidos
# ---------------------------------------------------------------------------
# Feature: db-table-sync, Property 1: conn strings produce valid Engine instances

@given(conn_str=st.just("sqlite:///:memory:"))
def test_conn_string_creates_engine(conn_str):
    syncer = Syncer(source=conn_str, target=conn_str, source_table="t", column_list=["id"])
    assert isinstance(syncer._source_engine, Engine)
    assert isinstance(syncer._target_engine, Engine)


# ---------------------------------------------------------------------------
# 7.2 — Property 3: Resolución de target_table
# ---------------------------------------------------------------------------
# Feature: db-table-sync, Property 3: target_table resolution

@given(
    source_table=_IDENT,
    target_table=st.one_of(st.none(), _IDENT),
)
def test_target_table_resolution(source_table, target_table):
    syncer = Syncer(
        source="sqlite:///:memory:",
        target="sqlite:///:memory:",
        source_table=source_table,
        column_list=["id"],
        target_table=target_table,
    )
    if target_table is None:
        assert syncer._target_table == source_table
    else:
        assert syncer._target_table == target_table


# ---------------------------------------------------------------------------
# 7.3 — Property 4: Sync_Query generada correctamente
# ---------------------------------------------------------------------------
# Feature: db-table-sync, Property 4: sync query format

@given(
    columns=st.lists(_IDENT, min_size=1, unique=True),
    table=_IDENT,
)
def test_sync_query_format(columns, table):
    syncer = Syncer(
        source="sqlite:///:memory:",
        target="sqlite:///:memory:",
        source_table=table,
        column_list=columns,
    )
    query = syncer._build_sync_query()
    for col in columns:
        assert col in query
    assert table in query
    assert query.startswith("SELECT")
    assert "FROM" in query


# ---------------------------------------------------------------------------
# 7.4 — Property 6: Mapeo de columnas aplicado correctamente
# ---------------------------------------------------------------------------
# Feature: db-table-sync, Property 6: column mapping applied to target columns

@given(
    column_list=st.lists(_IDENT, min_size=1, max_size=5, unique=True),
    data=st.data(),
)
def test_column_mapping_applied(column_list, data):
    # Build a mapping for a random non-empty subset of column_list
    keys_to_map = data.draw(
        st.lists(st.sampled_from(column_list), min_size=1, unique=True)
    )
    # Generate destination names (different from source names to verify renaming)
    dest_names = data.draw(
        st.lists(
            _IDENT.filter(lambda x: x not in column_list),
            min_size=len(keys_to_map),
            max_size=len(keys_to_map),
            unique=True,
        )
    )
    column_mapping = dict(zip(keys_to_map, dest_names))

    src_engine = create_engine("sqlite:///:memory:")
    tgt_engine = create_engine("sqlite:///:memory:")

    # Create source table with the given columns and insert one row
    col_defs = ", ".join(f'"{c}" TEXT' for c in column_list)
    with src_engine.connect() as conn:
        conn.execute(text(f'CREATE TABLE src_tbl ({col_defs})'))
        placeholders = ", ".join(["'val'"] * len(column_list))
        conn.execute(text(f'INSERT INTO src_tbl VALUES ({placeholders})'))
        conn.commit()

    syncer = Syncer(
        source=src_engine,
        target=tgt_engine,
        source_table="src_tbl",
        target_table="tgt_tbl",
        column_list=column_list,
        column_mapping=column_mapping,
    )
    syncer.sync()

    with tgt_engine.connect() as conn:
        import pandas as pd
        df = pd.read_sql(text("SELECT * FROM tgt_tbl"), con=conn)

    # Mapped columns should appear under their destination names
    for dest in dest_names:
        assert dest in df.columns
    # Original source names that were mapped should NOT appear
    for src_col in keys_to_map:
        assert src_col not in df.columns


# ---------------------------------------------------------------------------
# 7.5 — Property 8: Round-trip de sincronización
# ---------------------------------------------------------------------------
# Feature: db-table-sync, Property 8: sync round-trip row count

@given(rows=st.lists(st.fixed_dictionaries({"val": st.integers()}), max_size=20))
def test_sync_round_trip(rows):
    src_engine = create_engine("sqlite:///:memory:")
    tgt_engine = create_engine("sqlite:///:memory:")

    with src_engine.connect() as conn:
        conn.execute(text("CREATE TABLE data (val INTEGER)"))
        for row in rows:
            conn.execute(text(f"INSERT INTO data (val) VALUES ({row['val']})"))
        conn.commit()

    syncer = Syncer(
        source=src_engine,
        target=tgt_engine,
        source_table="data",
        column_list=["val"],
    )
    result = syncer.sync()

    assert result == len(rows)

    with tgt_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM data")).scalar()
    assert count == len(rows)


# ---------------------------------------------------------------------------
# 7.6 — Property 10: Validación de if_exists
# ---------------------------------------------------------------------------
# Feature: db-table-sync, Property 10: if_exists validation

@given(valid=st.sampled_from(["append", "replace", "fail"]))
def test_if_exists_valid_values_do_not_raise(valid):
    # Should not raise
    syncer = Syncer(
        source="sqlite:///:memory:",
        target="sqlite:///:memory:",
        source_table="t",
        column_list=["id"],
        if_exists=valid,
    )
    assert syncer._if_exists == valid


@given(
    invalid=st.text(min_size=1).filter(lambda x: x not in {"append", "replace", "fail"})
)
def test_if_exists_invalid_values_raise(invalid):
    with pytest.raises(ValueError):
        Syncer(
            source="sqlite:///:memory:",
            target="sqlite:///:memory:",
            source_table="t",
            column_list=["id"],
            if_exists=invalid,
        )


# ---------------------------------------------------------------------------
# 7.7 — Property 13: Logs emitidos en cada etapa del sync
# ---------------------------------------------------------------------------
# Feature: db-table-sync, Property 13: sync emits INFO logs on success, ERROR on failure

@given(rows=st.lists(st.fixed_dictionaries({"val": st.integers()}), max_size=10))
def test_sync_emits_logs_on_success(rows):
    src_engine = create_engine("sqlite:///:memory:")
    tgt_engine = create_engine("sqlite:///:memory:")

    with src_engine.connect() as conn:
        conn.execute(text("CREATE TABLE log_data (val INTEGER)"))
        for row in rows:
            conn.execute(text(f"INSERT INTO log_data (val) VALUES ({row['val']})"))
        conn.commit()

    syncer = Syncer(
        source=src_engine,
        target=tgt_engine,
        source_table="log_data",
        column_list=["val"],
    )

    log_records = []
    handler = logging.handlers_capture = _ListHandler(log_records)
    syncer_logger = logging.getLogger("db_table_sync.syncer")
    syncer_logger.addHandler(handler)
    syncer_logger.setLevel(logging.DEBUG)
    try:
        syncer.sync()
    finally:
        syncer_logger.removeHandler(handler)

    info_count = sum(1 for r in log_records if r.levelno == logging.INFO)
    assert info_count >= 3


@given(st.just(None))
def test_sync_emits_error_log_on_failure(_):
    syncer = Syncer(
        source="sqlite:///:memory:",
        target="sqlite:///:memory:",
        source_table="nonexistent_table",
        column_list=["id"],
    )

    log_records = []
    handler = _ListHandler(log_records)
    syncer_logger = logging.getLogger("db_table_sync.syncer")
    syncer_logger.addHandler(handler)
    syncer_logger.setLevel(logging.DEBUG)
    try:
        with pytest.raises(RuntimeError):
            syncer.sync()
    finally:
        syncer_logger.removeHandler(handler)

    error_count = sum(1 for r in log_records if r.levelno == logging.ERROR)
    assert error_count >= 1


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

class _ListHandler(logging.Handler):
    """Simple logging handler that appends records to a list."""

    def __init__(self, records: list):
        super().__init__()
        self._records = records

    def emit(self, record: logging.LogRecord) -> None:
        self._records.append(record)
