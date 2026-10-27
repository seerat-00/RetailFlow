"""Data quality validation module for RetailFlow ETL pipeline."""

from dataclasses import dataclass, field
from typing import Tuple, Dict, Any, List
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("retailflow.validate")


@dataclass
class DataQualityReport:
    """Comprehensive data quality audit report."""
    total_input_rows: int = 0
    accepted_rows: int = 0
    rejected_rows: int = 0
    duplicate_rows: int = 0
    negative_price_rows: int = 0
    zero_price_rows: int = 0
    invalid_date_rows: int = 0
    missing_key_rows: int = 0
    invalid_numeric_rows: int = 0
    missing_customer_id_rows: int = 0
    missing_description_rows: int = 0
    failure_counts_by_rule: Dict[str, int] = field(default_factory=dict)

    def is_reconciled(self) -> bool:
        """Verify that accepted rows plus rejected rows exactly equal total input rows."""
        return self.accepted_rows + self.rejected_rows == self.total_input_rows

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary for logging and persistence."""
        return {
            "total_input_rows": self.total_input_rows,
            "accepted_rows": self.accepted_rows,
            "rejected_rows": self.rejected_rows,
            "duplicate_rows": self.duplicate_rows,
            "reconciliation_passed": self.is_reconciled(),
            "acceptance_rate_pct": round((self.accepted_rows / self.total_input_rows * 100), 2)
            if self.total_input_rows > 0
            else 0.0,
            "rejection_rate_pct": round((self.rejected_rows / self.total_input_rows * 100), 2)
            if self.total_input_rows > 0
            else 0.0,
            "missing_customer_id_rows": self.missing_customer_id_rows,
            "missing_customer_id_pct": round(
                (self.missing_customer_id_rows / self.total_input_rows * 100), 2
            )
            if self.total_input_rows > 0
            else 0.0,
            "failure_counts_by_rule": self.failure_counts_by_rule,
        }


# Data Quality Rule Identifiers
RULE_EXACT_DUPLICATE = "RULE_EXACT_DUPLICATE"
RULE_MISSING_KEY = "RULE_MISSING_KEY"
RULE_INVALID_DATE = "RULE_INVALID_DATE"
RULE_INVALID_NUMERIC = "RULE_INVALID_NUMERIC"
RULE_NEGATIVE_PRICE = "RULE_NEGATIVE_PRICE"
RULE_ZERO_PRICE = "RULE_ZERO_PRICE"


def validate_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, DataQualityReport]:
    """
    Apply rigorous data quality rules, quarantine invalid records,
    and produce an auditable reconciliation report.

    Parameters
    ----------
    df : pd.DataFrame
        Transformed DataFrame containing candidate transaction records.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, DataQualityReport]
        - accepted_df: Valid transaction records ready for PostgreSQL staging/fact tables.
        - rejected_df: Quarantined records with failure reasons for debugging and audit.
        - report: Detailed DataQualityReport object.
    """
    logger.info("Executing data quality checks...")
    total_input = len(df)
    report = DataQualityReport(total_input_rows=total_input)

    # Initialize rule failure tracker per row
    # Array of lists to collect multiple reasons per row
    reasons: List[List[str]] = [[] for _ in range(total_input)]

    # 1. Check Required Keys (Invoice, StockCode)
    missing_key_mask = df["invoice"].isna() | (df["invoice"] == "") | df["stock_code"].isna() | (df["stock_code"] == "")
    report.missing_key_rows = int(missing_key_mask.sum())
    if report.missing_key_rows > 0:
        for idx in np.where(missing_key_mask)[0]:
            reasons[idx].append(RULE_MISSING_KEY)

    # 2. Check Invalid Dates
    invalid_date_mask = df["invoice_date"].isna()
    report.invalid_date_rows = int(invalid_date_mask.sum())
    if report.invalid_date_rows > 0:
        for idx in np.where(invalid_date_mask)[0]:
            reasons[idx].append(RULE_INVALID_DATE)

    # 3. Check Invalid Numeric Values (NaN quantities or prices)
    invalid_numeric_mask = df["quantity"].isna() | df["price"].isna()
    report.invalid_numeric_rows = int(invalid_numeric_mask.sum())
    if report.invalid_numeric_rows > 0:
        for idx in np.where(invalid_numeric_mask)[0]:
            reasons[idx].append(RULE_INVALID_NUMERIC)

    # 4. Check Negative Prices (Accounting adjustments / bad debt)
    negative_price_mask = (df["price"] < 0) & df["price"].notna()
    report.negative_price_rows = int(negative_price_mask.sum())
    if report.negative_price_rows > 0:
        for idx in np.where(negative_price_mask)[0]:
            reasons[idx].append(RULE_NEGATIVE_PRICE)

    # 5. Check Zero Prices (Inventory write-offs, damages, missing price)
    zero_price_mask = (df["price"] == 0) & df["price"].notna()
    report.zero_price_rows = int(zero_price_mask.sum())
    if report.zero_price_rows > 0:
        for idx in np.where(zero_price_mask)[0]:
            reasons[idx].append(RULE_ZERO_PRICE)

    # 6. Check Exact Duplicates across core transaction fields
    dup_cols = [
        "invoice",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "price",
        "customer_id",
        "country",
    ]
    duplicate_mask = df.duplicated(subset=dup_cols, keep="first")
    report.duplicate_rows = int(duplicate_mask.sum())
    if report.duplicate_rows > 0:
        for idx in np.where(duplicate_mask)[0]:
            reasons[idx].append(RULE_EXACT_DUPLICATE)

    # Informational Metrics (Not rejected)
    report.missing_customer_id_rows = int(df["customer_id"].isna().sum())
    report.missing_description_rows = int(df["description"].isna().sum())

    # Compile rule violation counts
    rule_counts = {
        RULE_EXACT_DUPLICATE: report.duplicate_rows,
        RULE_MISSING_KEY: report.missing_key_rows,
        RULE_INVALID_DATE: report.invalid_date_rows,
        RULE_INVALID_NUMERIC: report.invalid_numeric_rows,
        RULE_NEGATIVE_PRICE: report.negative_price_rows,
        RULE_ZERO_PRICE: report.zero_price_rows,
    }
    report.failure_counts_by_rule = {k: v for k, v in rule_counts.items() if v > 0}

    # Format reasons into strings
    joined_reasons = [";".join(r) for r in reasons]
    df = df.copy()
    df["rejection_reasons"] = joined_reasons

    # Partition into Accepted and Rejected datasets
    is_rejected = df["rejection_reasons"] != ""
    rejected_df = df[is_rejected].copy()
    accepted_df = df[~is_rejected].drop(columns=["rejection_reasons"]).copy()

    # Fill missing descriptions in accepted data with default standard label
    accepted_df["description"] = accepted_df["description"].fillna("UNSPECIFIED")

    report.accepted_rows = len(accepted_df)
    report.rejected_rows = len(rejected_df)

    if not report.is_reconciled():
        msg = (
            f"Data reconciliation failed! Input ({report.total_input_rows}) != "
            f"Accepted ({report.accepted_rows}) + Rejected ({report.rejected_rows})"
        )
        logger.error(msg)
        raise ValueError(msg)

    logger.info(
        f"Validation summary: {report.total_input_rows:,} total rows evaluated. "
        f"Accepted: {report.accepted_rows:,} ({report.accepted_rows/report.total_input_rows*100:.2f}%), "
        f"Rejected: {report.rejected_rows:,} ({report.rejected_rows/report.total_input_rows*100:.2f}%). "
        f"Reconciled: {report.is_reconciled()}"
    )

    return accepted_df, rejected_df, report
