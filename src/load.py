"""Database loading and schema management module for RetailFlow."""

from pathlib import Path
from typing import Dict, Any, Optional
import time
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg
from sqlalchemy import create_engine
import logging

from src.config import settings, PROJECT_ROOT
from src.validate import DataQualityReport

logger = logging.getLogger("retailflow.load")


def get_db_connection() -> psycopg.Connection:
    """Create and return a raw psycopg connection using application settings."""
    conn_info = (
        f"host={settings.db_host} "
        f"port={settings.db_port} "
        f"dbname={settings.db_name} "
        f"user={settings.db_user} "
        f"password={settings.db_password}"
    )
    return psycopg.connect(conn_info)


def get_sqlalchemy_engine():
    """Create and return an SQLAlchemy engine."""
    return create_engine(settings.get_db_url(), pool_pre_ping=True)


def init_database_schema(schema_file: Optional[Path] = None) -> None:
    """Execute DDL statements to create retailflow schema and tables."""
    path = schema_file or (PROJECT_ROOT / "sql" / "schema.sql")
    logger.info(f"Initializing database schema from: {path}")

    with open(path, "r", encoding="utf-8") as f:
        ddl = f.read()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

    logger.info("Database schema initialized successfully.")


def refresh_analytical_views(views_file: Optional[Path] = None) -> None:
    """Execute SQL script to create or refresh analytical views."""
    path = views_file or (PROJECT_ROOT / "sql" / "analytics.sql")
    logger.info(f"Refreshing analytical views from: {path}")

    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

    logger.info("Analytical views created/refreshed successfully.")


def _clean_tuple_for_postgres(tup: tuple) -> tuple:
    """Convert numpy/pandas NaNs and pd.NA to Python None for PostgreSQL NULL insertion."""
    clean = []
    for val in tup:
        if pd.isna(val):
            clean.append(None)
        elif isinstance(val, (np.integer, int)):
            clean.append(int(val))
        elif isinstance(val, (np.floating, float)):
            clean.append(float(val))
        elif isinstance(val, (np.bool_, bool)):
            clean.append(bool(val))
        else:
            clean.append(val)
    return tuple(clean)


def load_accepted_transactions(conn: psycopg.Connection, accepted_df: pd.DataFrame) -> int:
    """
    Stream accepted transaction records into PostgreSQL fact table idempotently
    using high-performance temporary staging table and atomic UPSERT.

    Parameters
    ----------
    conn : psycopg.Connection
        Active database connection.
    accepted_df : pd.DataFrame
        DataFrame of validated clean transactions.

    Returns
    -------
    int
        Count of loaded/upserted records.
    """
    if accepted_df.empty:
        logger.warning("Accepted transactions DataFrame is empty. Skipping fact table load.")
        return 0

    cols = [
        "source_row_id",
        "invoice",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "price",
        "customer_id",
        "country",
        "line_total",
        "is_cancellation",
        "is_return",
        "transaction_type",
    ]

    sub_df = accepted_df[cols].copy()
    row_count = len(sub_df)
    logger.info(f"Loading {row_count:,} accepted records into PostgreSQL...")

    with conn.cursor() as cur:
        # Create temp staging table mirroring fact table
        cur.execute(
            """
            DROP TABLE IF EXISTS tmp_fct_staging;
            CREATE TEMP TABLE tmp_fct_staging (
                LIKE retailflow.fct_transactions INCLUDING DEFAULTS
            ) ON COMMIT DROP;
            """
        )

        # Stream records into temp staging table using fast COPY protocol
        copy_sql = f"COPY tmp_fct_staging ({', '.join(cols)}) FROM STDIN"
        with cur.copy(copy_sql) as copy:
            for row in sub_df.itertuples(index=False, name=None):
                copy.write_row(_clean_tuple_for_postgres(row))

        # Idempotent upsert into permanent fact table
        upsert_sql = f"""
            INSERT INTO retailflow.fct_transactions ({', '.join(cols)})
            SELECT {', '.join(cols)} FROM tmp_fct_staging
            ON CONFLICT (source_row_id) DO UPDATE SET
                invoice = EXCLUDED.invoice,
                stock_code = EXCLUDED.stock_code,
                description = EXCLUDED.description,
                quantity = EXCLUDED.quantity,
                invoice_date = EXCLUDED.invoice_date,
                price = EXCLUDED.price,
                customer_id = EXCLUDED.customer_id,
                country = EXCLUDED.country,
                line_total = EXCLUDED.line_total,
                is_cancellation = EXCLUDED.is_cancellation,
                is_return = EXCLUDED.is_return,
                transaction_type = EXCLUDED.transaction_type;
            DROP TABLE IF EXISTS tmp_fct_staging;
        """
        cur.execute(upsert_sql)

    logger.info(f"Successfully loaded {row_count:,} accepted records into retailflow.fct_transactions.")
    return row_count


def load_rejected_records(conn: psycopg.Connection, rejected_df: pd.DataFrame) -> int:
    """
    Stream quarantined/rejected records into data_quality_rejected table idempotently.

    Parameters
    ----------
    conn : psycopg.Connection
        Active database connection.
    rejected_df : pd.DataFrame
        DataFrame of quarantined records with failure reasons.

    Returns
    -------
    int
        Count of loaded/upserted rejected records.
    """
    if rejected_df.empty:
        logger.info("No rejected records to load.")
        return 0

    cols = [
        "source_row_id",
        "invoice",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "price",
        "customer_id",
        "country",
        "rejection_reasons",
    ]

    # Prepare string versions of raw fields to avoid database type constraint violations on bad inputs
    sub_df = pd.DataFrame()
    for col in cols:
        if col in rejected_df.columns:
            if col == "source_row_id":
                sub_df[col] = rejected_df[col].astype(int)
            else:
                sub_df[col] = rejected_df[col].astype(str).replace({"nan": None, "None": None, "<NA>": None})
        else:
            sub_df[col] = None

    row_count = len(sub_df)
    logger.info(f"Loading {row_count:,} quarantined records into PostgreSQL...")

    with conn.cursor() as cur:
        cur.execute(
            """
            DROP TABLE IF EXISTS tmp_rej_staging;
            CREATE TEMP TABLE tmp_rej_staging (
                LIKE retailflow.data_quality_rejected INCLUDING DEFAULTS
            ) ON COMMIT DROP;
            """
        )

        copy_sql = f"COPY tmp_rej_staging ({', '.join(cols)}) FROM STDIN"
        with cur.copy(copy_sql) as copy:
            for row in sub_df.itertuples(index=False, name=None):
                copy.write_row(_clean_tuple_for_postgres(row))

        upsert_sql = f"""
            INSERT INTO retailflow.data_quality_rejected ({', '.join(cols)})
            SELECT {', '.join(cols)} FROM tmp_rej_staging
            ON CONFLICT (source_row_id) DO UPDATE SET
                invoice = EXCLUDED.invoice,
                stock_code = EXCLUDED.stock_code,
                description = EXCLUDED.description,
                quantity = EXCLUDED.quantity,
                invoice_date = EXCLUDED.invoice_date,
                price = EXCLUDED.price,
                customer_id = EXCLUDED.customer_id,
                country = EXCLUDED.country,
                rejection_reasons = EXCLUDED.rejection_reasons;
            DROP TABLE IF EXISTS tmp_rej_staging;
        """
        cur.execute(upsert_sql)

    logger.info(f"Successfully loaded {row_count:,} quarantined records into retailflow.data_quality_rejected.")
    return row_count


def record_pipeline_run(
    conn: psycopg.Connection,
    status: str,
    report: DataQualityReport,
    gross_sales: float,
    return_amount: float,
    net_sales: float,
    started_at: datetime,
    completed_at: datetime,
    error_message: Optional[str] = None,
) -> int:
    """Record metadata audit log for the ETL run."""
    duration_sec = round((completed_at - started_at).total_seconds(), 2)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO retailflow.pipeline_runs (
                pipeline_name, status, input_rows, accepted_rows, rejected_rows,
                duplicate_rows, gross_sales_amount, return_amount, net_sales_amount,
                started_at, completed_at, duration_seconds, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING run_id;
            """,
            (
                "retailflow_etl",
                status,
                report.total_input_rows,
                report.accepted_rows,
                report.rejected_rows,
                report.duplicate_rows,
                gross_sales,
                return_amount,
                net_sales,
                started_at,
                completed_at,
                duration_sec,
                error_message,
            ),
        )
        run_id = cur.fetchone()[0]

    logger.info(f"Pipeline run recorded with run_id={run_id} (Status: {status}, Duration: {duration_sec}s).")
    return run_id


def verify_database_counts(conn: psycopg.Connection, expected_accepted: int, expected_rejected: int) -> Dict[str, Any]:
    """Verify that table row counts in PostgreSQL match in-memory validation counts."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM retailflow.fct_transactions;")
        fct_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM retailflow.data_quality_rejected;")
        rej_count = cur.fetchone()[0]

    reconciled = (fct_count == expected_accepted) and (rej_count == expected_rejected)
    verification = {
        "db_fct_count": fct_count,
        "db_rejected_count": rej_count,
        "expected_accepted": expected_accepted,
        "expected_rejected": expected_rejected,
        "tables_reconciled": reconciled,
    }

    if not reconciled:
        msg = f"Database count discrepancy: DB fct={fct_count} (exp {expected_accepted}), DB rej={rej_count} (exp {expected_rejected})"
        logger.warning(msg)
    else:
        logger.info(f"Database verification PASSED: fct_transactions={fct_count:,}, data_quality_rejected={rej_count:,}.")

    return verification


def load_pipeline_data(
    accepted_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    report: DataQualityReport,
    started_at: datetime,
) -> Dict[str, Any]:
    """
    Execute full transactional loading pipeline:
    1. Initialize schema and tables if missing
    2. Load accepted records idempotently
    3. Load quarantined records idempotently
    4. Compute financial audit metrics
    5. Record pipeline run metadata
    6. Verify database counts
    7. Refresh analytical views
    """
    logger.info("Starting database loading process...")

    init_database_schema()

    # Financial sums for audit logging
    gross_sales = float(accepted_df.loc[accepted_df["transaction_type"] == "SALE", "line_total"].sum()) if not accepted_df.empty else 0.0
    return_amount = float(accepted_df.loc[accepted_df["is_return"] | accepted_df["is_cancellation"], "line_total"].abs().sum()) if not accepted_df.empty else 0.0
    net_sales = float(accepted_df["line_total"].sum()) if not accepted_df.empty else 0.0

    gross_sales = round(gross_sales, 2)
    return_amount = round(return_amount, 2)
    net_sales = round(net_sales, 2)

    with get_db_connection() as conn:
        with conn.transaction():
            loaded_accepted = load_accepted_transactions(conn, accepted_df)
            loaded_rejected = load_rejected_records(conn, rejected_df)

            completed_at = datetime.now()
            run_id = record_pipeline_run(
                conn=conn,
                status="SUCCESS",
                report=report,
                gross_sales=gross_sales,
                return_amount=return_amount,
                net_sales=net_sales,
                started_at=started_at,
                completed_at=completed_at,
            )

        # Verification step
        verification = verify_database_counts(conn, len(accepted_df), len(rejected_df))

    # Refresh analytical views
    refresh_analytical_views()

    summary = {
        "run_id": run_id,
        "loaded_accepted": loaded_accepted,
        "loaded_rejected": loaded_rejected,
        "gross_sales": gross_sales,
        "return_amount": return_amount,
        "net_sales": net_sales,
        "verification": verification,
    }
    return summary
