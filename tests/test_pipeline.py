"""Pipeline and integration tests for RetailFlow."""

from pathlib import Path
import tempfile
import pandas as pd
import pytest

from src.extract import extract_raw_data, ExtractionError
from src.transform import transform_data
from src.validate import validate_data
from src.config import settings
from src.load import get_db_connection, load_accepted_transactions, load_rejected_records


def test_extract_nonexistent_file():
    """Verify ExtractionError is raised if file is missing."""
    with pytest.raises(ExtractionError):
        extract_raw_data(file_path=Path("/tmp/nonexistent_retail_data_xyz.csv"))


def test_synthetic_pipeline_flow():
    """Test full extract -> transform -> validate flow on a temporary synthetic CSV."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        tmp.write("Invoice,StockCode,Description,Quantity,InvoiceDate,Price,Customer ID,Country\n")
        tmp.write("500001,85048,LIGHTS,10,2010-01-05 10:00:00,5.0,12345,United Kingdom\n")
        tmp.write("500001,85048,LIGHTS,10,2010-01-05 10:00:00,5.0,12345,United Kingdom\n") # Duplicate
        tmp.write("C500002,22041,RETURN ITEM,-2,2010-01-05 11:00:00,10.0,12345,United Kingdom\n")
        tmp.write("500003,22042,DAMAGED ITEM,-5,2010-01-05 12:00:00,0.0,,United Kingdom\n") # Zero price writeoff
        tmp_path = Path(tmp.name)

    try:
        raw_df, meta = extract_raw_data(file_path=tmp_path)
        assert len(raw_df) == 4
        assert "source_row_id" in raw_df.columns

        transformed_df, stats = transform_data(raw_df)
        assert len(transformed_df) == 4
        assert stats["total_cancellations"] == 1

        accepted_df, rejected_df, report = validate_data(transformed_df)
        assert report.total_input_rows == 4
        assert report.accepted_rows == 2  # Row 1 (first sale) and Row 3 (cancellation)
        assert report.rejected_rows == 2  # Row 2 (exact dup) and Row 4 (zero price)
        assert report.is_reconciled() is True
    finally:
        tmp_path.unlink(missing_ok=True)


@pytest.mark.integration
def test_database_idempotency():
    """
    Verify database loading idempotency:
    Loading the same batch of records twice must not create duplicates.
    """
    try:
        conn = get_db_connection()
    except Exception as e:
        pytest.skip(f"Database connection not available for integration test: {e}")

    # Create synthetic test dataframe
    test_df = pd.DataFrame({
        "source_row_id": [9999901, 9999902],
        "invoice": ["TEST001", "TEST002"],
        "stock_code": ["CODE1", "CODE2"],
        "description": ["TEST PRODUCT 1", "TEST PRODUCT 2"],
        "quantity": [5, 10],
        "invoice_date": [pd.Timestamp("2026-01-01 10:00:00"), pd.Timestamp("2026-01-01 11:00:00")],
        "price": [10.00, 20.00],
        "customer_id": [pd.Series([99001], dtype="Int64")[0], pd.Series([99002], dtype="Int64")[0]],
        "country": ["United Kingdom", "United Kingdom"],
        "line_total": [50.00, 200.00],
        "is_cancellation": [False, False],
        "is_return": [False, False],
        "transaction_type": ["SALE", "SALE"],
    })

    with conn:
        with conn.transaction():
            # Initial load
            load_accepted_transactions(conn, test_df)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM retailflow.fct_transactions WHERE source_row_id IN (9999901, 9999902);"
                )
                count_first = cur.fetchone()[0]
                assert count_first == 2

            # Second load with same records (simulating pipeline rerun)
            load_accepted_transactions(conn, test_df)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM retailflow.fct_transactions WHERE source_row_id IN (9999901, 9999902);"
                )
                count_second = cur.fetchone()[0]
                # Idempotency assertion: count remains 2, no duplicates created
                assert count_second == 2

            # Clean up test records
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM retailflow.fct_transactions WHERE source_row_id IN (9999901, 9999902);"
                )
