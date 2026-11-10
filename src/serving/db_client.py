"""
Serving Layer Database Manager (Proposal Section III.4 & Note 8 Lambda Architecture).
- Connects to PostgreSQL (`postgresql-service`) to store and merge Batch Views & Real-time Speed Views.
- Automatically falls back to a local SQLite database (`data/serving_layer.db`) if PostgreSQL is not running,
  ensuring 100% reproducibility both inside Docker Compose and in standalone local execution.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from src.config import BASE_DIR, DB_CONFIG, DatabaseConfig

logger = logging.getLogger(__name__)


class ServingDatabaseClient:
    """Unified database client for the Lambda Architecture Serving Layer."""

    def __init__(self, config: DatabaseConfig = DB_CONFIG, force_sqlite: bool = False) -> None:
        self.config = config
        self.force_sqlite = force_sqlite
        self._engine: Any = None
        self._using_postgres: bool = False
        self._initialize_connection()

    @property
    def is_postgres(self) -> bool:
        return self._using_postgres

    @property
    def backend_name(self) -> str:
        return (
            f"PostgreSQL ({self.config.host}:{self.config.port}/{self.config.database})"
            if self._using_postgres
            else f"SQLite Local ({self.config.sqlite_fallback_path.name})"
        )

    def _initialize_connection(self) -> None:
        if not self.force_sqlite:
            try:
                from sqlalchemy import create_engine, text

                engine = create_engine(
                    self.config.sqlalchemy_postgres_url,
                    connect_args={"connect_timeout": 3},
                    pool_pre_ping=True,
                )
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                self._engine = engine
                self._using_postgres = True
                logger.info("Connected to PostgreSQL Serving Layer at %s", self.config.host)
                return
            except Exception as exc:
                logger.info(
                    "PostgreSQL not reachable (%s). Falling back to local SQLite Serving Layer.",
                    exc,
                )

        self._using_postgres = False
        self.config.sqlite_fallback_path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def ensure_schema(self) -> None:
        """Initialize Serving Layer tables if they do not exist."""
        init_sql_path = BASE_DIR / "sql" / "init.sql"
        if not init_sql_path.exists():
            return
        raw_sql = init_sql_path.read_text(encoding="utf-8")

        if self._using_postgres and self._engine is not None:
            from sqlalchemy import text

            with self._engine.begin() as conn:
                conn.execute(text(raw_sql))
        else:
            # Adapt PostgreSQL SERIAL syntax for SQLite compatibility
            sqlite_sql = raw_sql.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
            with sqlite3.connect(self.config.sqlite_fallback_path) as conn:
                conn.executescript(sqlite_sql)
                conn.commit()

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
    ) -> None:
        """Write a Pandas DataFrame into the Serving Layer table."""
        self.ensure_schema()
        if self._using_postgres and self._engine is not None:
            df.to_sql(table_name, self._engine, if_exists=if_exists, index=False, method="multi")
        else:
            with sqlite3.connect(self.config.sqlite_fallback_path) as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)

    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Read a Serving Layer table into a Pandas DataFrame."""
        query = f"SELECT * FROM {table_name}"
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        return self.query_df(query)

    def query_df(self, sql_query: str) -> pd.DataFrame:
        """Execute an arbitrary SQL SELECT query against the Serving Layer."""
        try:
            if self._using_postgres and self._engine is not None:
                return pd.read_sql_query(sql_query, self._engine)
            with sqlite3.connect(self.config.sqlite_fallback_path) as conn:
                return pd.read_sql_query(sql_query, conn)
        except Exception as exc:
            logger.warning("Query failed (%s): %s", sql_query, exc)
            return pd.DataFrame()

