"""Unit tests for RetailFlow transformation logic."""

import pandas as pd
import numpy as np
import pytest

from src.transform import (
    standardize_columns,
    clean_text_fields,
    parse_customer_id,
    convert_datatypes,
    calculate_derived_fields,
    transform_data,
)


@pytest.fixture
def sample_raw_df() -> pd.DataFrame:
    """Fixture providing synthetic raw transaction records."""
    return pd.DataFrame({
        "source_row_id": [1, 2, 3, 4, 5],
        "Invoice": ["489434", "C489435", "489436", "489437", "489438"],
        "StockCode": ["85048 ", " 79323p", "22041", "POST", "B"],
        "Description": [" LIGHTS ", "PINK LIGHTS", "FRAME", "POSTAGE", "ADJUST BAD DEBT"],
        "Quantity": ["12", "-2", "48", "-5", "1"],
        "InvoiceDate": [
            "2009-12-01 07:45:00",
            "2009-12-01 08:00:00",
            "2009-12-01 08:15:00",
            "2009-12-01 08:30:00",
            "2009-12-01 08:45:00",
        ],
        "Price": ["6.95", "6.75", "2.10", "18.00", "-50.00"],
        "Customer ID": ["13085.0", "13085.0", "14108.0", None, np.nan],
        "Country": ["United Kingdom ", "United Kingdom", " france", "usa", "Eire"],
    })


def test_standardize_columns(sample_raw_df):
    """Test standardizing raw column names into snake_case."""
    df = standardize_columns(sample_raw_df)
    expected_cols = [
        "source_row_id",
        "invoice",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "price",
        "customer_id",
        "country",
    ]
    assert list(df.columns) == expected_cols


def test_clean_text_fields(sample_raw_df):
    """Test text stripping, uppercase stock code, and country normalization."""
    df = standardize_columns(sample_raw_df)
    df = clean_text_fields(df)

    assert df["stock_code"].iloc[0] == "85048"
    assert df["stock_code"].iloc[1] == "79323P"  # Uppercase
    assert df["description"].iloc[0] == "LIGHTS"  # Stripped
    assert df["country"].iloc[2] == "France"  # Title case
    assert df["country"].iloc[3] == "USA"  # Special country acronym
    assert df["country"].iloc[4] == "EIRE"


def test_parse_customer_id(sample_raw_df):
    """Test Customer ID conversion and removing '.0' float artifact."""
    df = standardize_columns(sample_raw_df)
    df = parse_customer_id(df)

    assert df["customer_id"].iloc[0] == 13085
    assert df["customer_id"].iloc[1] == 13085
    assert pd.isna(df["customer_id"].iloc[3])
    assert pd.isna(df["customer_id"].iloc[4])


def test_convert_datatypes(sample_raw_df):
    """Test safe conversion to numeric and datetime datatypes."""
    df = standardize_columns(sample_raw_df)
    df = convert_datatypes(df)

    assert pd.api.types.is_numeric_dtype(df["quantity"])
    assert pd.api.types.is_float_dtype(df["price"])
    assert pd.api.types.is_datetime64_any_dtype(df["invoice_date"])
    assert df["price"].iloc[0] == 6.95


def test_calculate_derived_fields(sample_raw_df):
    """Test calculation of line_total, is_cancellation, is_return, and transaction_type."""
    df = standardize_columns(sample_raw_df)
    df = convert_datatypes(df)
    df = calculate_derived_fields(df)

    # Row 0: Normal sale (12 * 6.95 = 83.40)
    assert df["line_total"].iloc[0] == 83.40
    assert not df["is_cancellation"].iloc[0]
    assert not df["is_return"].iloc[0]
    assert df["transaction_type"].iloc[0] == "SALE"

    # Row 1: Cancellation (-2 * 6.75 = -13.50)
    assert df["line_total"].iloc[1] == -13.50
    assert df["is_cancellation"].iloc[1]
    assert df["is_return"].iloc[1]
    assert df["transaction_type"].iloc[1] == "CANCELLATION"

    # Row 3: Return without C prefix (-5 * 18.00 = -90.00)
    assert df["line_total"].iloc[3] == -90.00
    assert not df["is_cancellation"].iloc[3]
    assert df["is_return"].iloc[3]
    assert df["transaction_type"].iloc[3] == "RETURN"


def test_end_to_end_transform(sample_raw_df):
    """Test full transform_data pipeline on synthetic dataset."""
    df_transformed, stats = transform_data(sample_raw_df)

    assert len(df_transformed) == len(sample_raw_df)
    assert stats["transformed_rows"] == 5
    assert stats["total_sales_records"] == 3
    assert stats["total_cancellations"] == 1
    assert stats["total_returns"] == 1
    assert stats["missing_customer_ids"] == 2
