"""Unit tests for RetailFlow data quality validation logic."""

import pandas as pd
import numpy as np
import pytest

from src.validate import (
    validate_data,
    RULE_EXACT_DUPLICATE,
    RULE_NEGATIVE_PRICE,
    RULE_ZERO_PRICE,
    RULE_MISSING_KEY,
    RULE_INVALID_DATE,
    RULE_INVALID_NUMERIC,
)


@pytest.fixture
def candidate_df() -> pd.DataFrame:
    """Fixture providing transformed records with deliberate quality anomalies."""
    return pd.DataFrame({
        "source_row_id": [1, 2, 3, 4, 5, 6, 7, 8],
        "invoice": ["489434", "489434", "C489435", "489436", "489437", None, "489439", "489440"],
        "stock_code": ["85048", "85048", "79323P", "22041", "B", "22042", "22043", "22044"],
        "description": ["LIGHTS", "LIGHTS", "PINK LIGHTS", "FRAME", "BAD DEBT", "TIN", "MUG", "PLATE"],
        "quantity": [12, 12, -2, 48, 1, 10, np.nan, 20],
        "invoice_date": [
            pd.Timestamp("2009-12-01 07:45:00"),
            pd.Timestamp("2009-12-01 07:45:00"),  # Duplicate of row 1
            pd.Timestamp("2009-12-01 08:00:00"),  # Valid cancellation
            pd.Timestamp("2009-12-01 08:15:00"),  # Valid sale with 0 price
            pd.Timestamp("2009-12-01 08:30:00"),  # Negative price
            pd.Timestamp("2009-12-01 08:45:00"),  # Missing invoice
            pd.NaT,                               # Invalid date
            pd.Timestamp("2009-12-01 09:15:00"),  # Valid sale with missing customer_id
        ],
        "price": [6.95, 6.95, 6.75, 0.00, -50.00, 2.50, 4.00, 3.50],
        "customer_id": [pd.Series([13085], dtype="Int64")[0],
                        pd.Series([13085], dtype="Int64")[0],
                        pd.Series([13085], dtype="Int64")[0],
                        pd.Series([14108], dtype="Int64")[0],
                        pd.NA,
                        pd.Series([15000], dtype="Int64")[0],
                        pd.Series([16000], dtype="Int64")[0],
                        pd.NA],  # Valid guest sale
        "country": ["United Kingdom", "United Kingdom", "United Kingdom", "France", "United Kingdom", "EIRE", "USA", "United Kingdom"],
        "line_total": [83.40, 83.40, -13.50, 0.00, -50.00, 25.00, np.nan, 70.00],
        "is_cancellation": [False, False, True, False, False, False, False, False],
        "is_return": [False, False, True, False, False, False, False, False],
        "transaction_type": ["SALE", "SALE", "CANCELLATION", "SALE", "SALE", "SALE", "SALE", "SALE"],
    })


def test_validation_partitions_and_reconciles(candidate_df):
    """Test that accepted and rejected records strictly reconcile to total inputs."""
    accepted_df, rejected_df, report = validate_data(candidate_df)

    assert report.total_input_rows == len(candidate_df)
    assert report.accepted_rows + report.rejected_rows == report.total_input_rows
    assert report.is_reconciled() is True


def test_duplicate_detection(candidate_df):
    """Test that exact duplicate is quarantined with RULE_EXACT_DUPLICATE."""
    accepted_df, rejected_df, report = validate_data(candidate_df)

    assert report.duplicate_rows == 1
    dup_row = rejected_df[rejected_df["source_row_id"] == 2]
    assert len(dup_row) == 1
    assert RULE_EXACT_DUPLICATE in dup_row["rejection_reasons"].iloc[0]


def test_price_validation(candidate_df):
    """Test detection of negative price and zero price."""
    accepted_df, rejected_df, report = validate_data(candidate_df)

    # Negative price on row 5
    neg_price_row = rejected_df[rejected_df["source_row_id"] == 5]
    assert len(neg_price_row) == 1
    assert RULE_NEGATIVE_PRICE in neg_price_row["rejection_reasons"].iloc[0]

    # Zero price on row 4
    zero_price_row = rejected_df[rejected_df["source_row_id"] == 4]
    assert len(zero_price_row) == 1
    assert RULE_ZERO_PRICE in zero_price_row["rejection_reasons"].iloc[0]


def test_missing_key_detection(candidate_df):
    """Test missing invoice is rejected with RULE_MISSING_KEY."""
    accepted_df, rejected_df, report = validate_data(candidate_df)

    missing_key_row = rejected_df[rejected_df["source_row_id"] == 6]
    assert len(missing_key_row) == 1
    assert RULE_MISSING_KEY in missing_key_row["rejection_reasons"].iloc[0]


def test_invalid_date_and_numeric_detection(candidate_df):
    """Test invalid date and invalid numeric detection."""
    accepted_df, rejected_df, report = validate_data(candidate_df)

    # Row 7 has NaN quantity and NaT date
    row7 = rejected_df[rejected_df["source_row_id"] == 7]
    assert len(row7) == 1
    reasons = row7["rejection_reasons"].iloc[0]
    assert RULE_INVALID_DATE in reasons
    assert RULE_INVALID_NUMERIC in reasons


def test_guest_checkout_retained(candidate_df):
    """Verify that records with missing customer ID but valid commercial fields are accepted."""
    accepted_df, rejected_df, report = validate_data(candidate_df)

    # Row 8 is a valid commercial sale with customer_id=pd.NA
    row8 = accepted_df[accepted_df["source_row_id"] == 8]
    assert len(row8) == 1
    assert pd.isna(row8["customer_id"].iloc[0])
    assert row8["line_total"].iloc[0] == 70.00
    assert row8["transaction_type"].iloc[0] == "SALE"


def test_accepted_cancellations_retained(candidate_df):
    """Verify valid commercial cancellations are accepted, not rejected."""
    accepted_df, rejected_df, report = validate_data(candidate_df)

    # Row 3 is a valid commercial cancellation (Invoice starts with C, Price > 0)
    row3 = accepted_df[accepted_df["source_row_id"] == 3]
    assert len(row3) == 1
    assert row3["transaction_type"].iloc[0] == "CANCELLATION"
    assert row3["line_total"].iloc[0] == -13.50
