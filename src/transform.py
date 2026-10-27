"""Data transformation module for RetailFlow ETL pipeline."""

from typing import Tuple, Dict, Any
import numpy as np
import pandas as pd
import logging

from src.config import setup_logging

logger = logging.getLogger("retailflow.transform")

# Column mapping from raw dataset to standardized database schema
COLUMN_MAPPING = {
    "source_row_id": "source_row_id",
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "price",
    "Customer ID": "customer_id",
    "Country": "country",
}


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to lowercase snake_case standard."""
    df_out = df.rename(columns=COLUMN_MAPPING).copy()
    return df_out


def clean_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize text columns: strip whitespace, uppercase codes and country."""
    df = df.copy()

    for col in ["invoice", "stock_code", "country"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            # Replace 'nan' string with actual NaN
            df[col] = df[col].replace({"nan": np.nan, "None": np.nan, "": np.nan})

    if "country" in df.columns:
        # Standardize country names
        df["country"] = df["country"].str.strip().str.title()
        # Edge case: 'Usa' -> 'USA', 'Eire' -> 'EIRE', 'Rsa' -> 'RSA'
        df["country"] = df["country"].replace({
            "Usa": "USA",
            "United Kingdom": "United Kingdom",
            "Eire": "EIRE",
            "Rsa": "RSA",
        })

    if "stock_code" in df.columns:
        df["stock_code"] = df["stock_code"].str.upper()

    if "description" in df.columns:
        # Clean description whitespace but preserve case
        df["description"] = df["description"].astype(str).str.strip()
        df["description"] = df["description"].replace({"nan": np.nan, "None": np.nan, "": np.nan})

    return df


def parse_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format Customer ID into integer string or nullable integer.
    Handles '13085.0' -> '13085' or integer.
    """
    df = df.copy()
    if "customer_id" in df.columns:
        # Remove floating point suffix if present
        cleaned_cust = (
            df["customer_id"]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
            .replace({"nan": np.nan, "None": np.nan, "": np.nan})
        )
        # Convert to nullable Int64
        df["customer_id"] = pd.to_numeric(cleaned_cust, errors="coerce").astype("Int64")
    return df


def convert_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    """Safely convert numeric and datetime columns."""
    df = df.copy()

    # Convert Quantity to numeric (nullable Int64)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    # Convert Price to numeric float rounded to 2 decimals
    df["price"] = pd.to_numeric(df["price"], errors="coerce").round(2)

    # Convert InvoiceDate to datetime
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    return df


def calculate_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute business metrics and indicators:
    - line_total: quantity * price (rounded to 2 decimal places)
    - is_cancellation: True if invoice starts with 'C'
    - is_return: True if quantity < 0
    - transaction_type: 'SALE', 'CANCELLATION', or 'RETURN'
    """
    df = df.copy()

    # Cancellations: invoice starts with 'C'
    is_cancellation = df["invoice"].astype(str).str.startswith("C", na=False)
    # Returns: negative quantity
    is_return = df["quantity"] < 0

    df["is_cancellation"] = is_cancellation
    df["is_return"] = is_return

    # Transaction type classification
    df["transaction_type"] = "SALE"
    df.loc[is_cancellation, "transaction_type"] = "CANCELLATION"
    df.loc[(~is_cancellation) & is_return, "transaction_type"] = "RETURN"

    # Line total revenue: quantity * price
    df["line_total"] = (df["quantity"] * df["price"]).round(2)

    return df


def transform_data(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Run end-to-end transformation on raw transaction data.

    Parameters
    ----------
    raw_df : pd.DataFrame
        Raw DataFrame extracted from CSV.

    Returns
    -------
    Tuple[pd.DataFrame, Dict[str, Any]]
        Transformed DataFrame ready for validation, and transformation statistics.
    """
    logger.info("Starting data transformations...")
    initial_rows = len(raw_df)

    # 1. Rename columns
    df = standardize_columns(raw_df)

    # 2. Clean text fields
    df = clean_text_fields(df)

    # 3. Clean and convert customer IDs
    df = parse_customer_id(df)

    # 4. Safely convert datatypes
    df = convert_datatypes(df)

    # 5. Compute line revenue and business transaction classifications
    df = calculate_derived_fields(df)

    stats = {
        "transformed_rows": len(df),
        "total_sales_records": int((df["transaction_type"] == "SALE").sum()),
        "total_cancellations": int((df["transaction_type"] == "CANCELLATION").sum()),
        "total_returns": int((df["transaction_type"] == "RETURN").sum()),
        "missing_customer_ids": int(df["customer_id"].isna().sum()),
        "missing_descriptions": int(df["description"].isna().sum()),
    }

    logger.info(
        f"Transformations complete: {stats['transformed_rows']:,} rows processed "
        f"({stats['total_sales_records']:,} sales, {stats['total_cancellations']:,} cancellations, "
        f"{stats['total_returns']:,} returns)."
    )

    return df, stats
