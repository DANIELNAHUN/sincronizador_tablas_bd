"""syncer.py — Syncer class for db_table_sync."""
from __future__ import annotations

import logging
from typing import Literal

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class Syncer:
    """Synchronizes a table from a source database to a target database."""

    def __init__(
        self,
        source: Engine | str,
        target: Engine | str,
        source_table: str,
        column_list: list[str],
        target_table: str | None = None,
        column_mapping: dict[str, str] | None = None,
        if_exists: Literal["append", "replace", "fail"] = "append",
    ) -> None:
        # Validate column_list
        if not column_list:
            raise ValueError("column_list must contain at least one column name.")

        # Validate if_exists
        allowed_if_exists = ("append", "replace", "fail")
        if if_exists not in allowed_if_exists:
            raise ValueError(
                f"if_exists must be one of {allowed_if_exists!r}, got {if_exists!r}."
            )

        # Validate column_mapping keys exist in column_list
        if column_mapping is not None:
            column_set = set(column_list)
            for key in column_mapping:
                if key not in column_set:
                    raise ValueError(
                        f"column_mapping key {key!r} does not exist in column_list."
                    )

        # Convert connection strings to Engine and verify connectivity
        self._source_engine = self._resolve_engine(source, "source")
        self._target_engine = self._resolve_engine(target, "target")

        # Store validated attributes
        self._source_table = source_table
        self._target_table = target_table if target_table is not None else source_table
        self._column_list = column_list
        self._column_mapping = column_mapping
        self._if_exists = if_exists

    def _resolve_engine(self, connection: Engine | str, label: str) -> Engine:
        """Convert a connection string to an Engine if needed, then verify connectivity."""
        if isinstance(connection, str):
            engine = create_engine(connection)
        else:
            engine = connection

        try:
            with engine.connect():
                pass
        except Exception as exc:
            raise ConnectionError(
                f"Could not connect to {label} database: {exc}"
            ) from exc

        return engine

    def _build_sync_query(self) -> str:
        """Return the SQL text for the sync SELECT query."""
        columns_sql = ", ".join(self._column_list)
        return f"SELECT {columns_sql} FROM {self._source_table}"

    def sync(self) -> int:
        """Execute the sync and return the number of rows inserted."""
        # Task 4.1 — Build the Sync_Query
        query = text(self._build_sync_query())

        logger.info("Executing sync query on table %r (%d columns)", self._source_table, len(self._column_list))

        # Task 4.2 — Execute query and load into DataFrame
        try:
            with self._source_engine.connect() as conn:
                df = pd.read_sql(query, con=conn)
        except Exception as exc:
            logger.error("Error reading from source: %s", exc, exc_info=True)
            raise RuntimeError(
                f"Failed to execute sync query on source table {self._source_table!r}: {exc}"
            ) from exc

        logger.info("Loaded %d rows from source table %r", len(df), self._source_table)

        # Task 4.3 — Apply column mapping if provided
        if self._column_mapping is not None:
            df = df.rename(columns=self._column_mapping)

        # Task 4.4 — Insert DataFrame into target table
        try:
            df.to_sql(
                self._target_table,
                con=self._target_engine,
                if_exists=self._if_exists,
                index=False,
            )
        except Exception as exc:
            logger.error("Error inserting into target: %s", exc, exc_info=True)
            raise RuntimeError(
                f"Failed to insert into target table {self._target_table!r}: {exc}"
            ) from exc

        logger.info("Inserted %d rows into target table %r", len(df), self._target_table)

        # Task 4.5 — Return number of rows inserted
        return int(len(df))
