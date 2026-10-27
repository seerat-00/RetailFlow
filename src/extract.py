"""Source data extraction module for RetailFlow ETL pipeline."""

from pathlib import Path
from typing import Optional, Tuple, Dict, Any
import pandas as pd
import logging

from src.config import settings, setup_logging

logger = logging.getLogger("retailflow.extract")


EXPECTED_RAW_COLUMNS = [
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
]


class ExtractionError(Exception):
    """Raised when source data extraction fails."""
    pass


def extract_raw_data(
    file_path: Optional[Path] = None,
    nrows: Optional[int] = None,
    encoding: str = "utf-8",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Extract raw retail transaction data from CSV file.

    Parameters
    ----------
    file_path : Optional[Path]
        Path to raw CSV file. Defaults to settings.raw_data_path.
    nrows : Optional[int]
        Number of rows to read. None reads all rows.
    encoding : str
        File encoding, default 'utf-8'.

    Returns
    -------
    Tuple[pd.DataFrame, Dict[str, Any]]
        Extracted DataFrame with 'source_row_id' appended, and extraction metadata dictionary.

    Raises
    ------
    ExtractionError
        If file does not exist, cannot be read, or lacks expected columns.
    """
    path = Path(file_path) if file_path else settings.raw_data_path

    logger.info(f"Initiating extraction from: {path}")

    if not path.exists():
        msg = f"Source CSV file not found at: {path}"
        logger.error(msg)
        raise ExtractionError(msg)

    if not path.is_file():
        msg = f"Source path is not a file: {path}"
        logger.error(msg)
        raise ExtractionError(msg)

    file_size_mb = path.stat().st_size / (1024 * 1024)
    logger.info(f"Source file size: {file_size_mb:.2f} MB")

    try:
        # Read with string dtype for identifiers to prevent loss of leading zeros or formatting
        df = pd.read_csv(
            path,
            nrows=nrows,
            encoding=encoding,
            dtype={
                "Invoice": str,
                "StockCode": str,
                "Customer ID": str,
            },
            low_memory=False,
        )
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 decoding failed for {path}. Retrying with 'latin1' / 'ISO-8859-1'...")
        try:
            df = pd.read_csv(
                path,
                nrows=nrows,
                encoding="latin1",
                dtype={
                    "Invoice": str,
                    "StockCode": str,
                    "Customer ID": str,
                },
                low_memory=False,
            )
        except Exception as e:
            msg = f"Failed to extract CSV using fallback encoding: {e}"
            logger.error(msg)
            raise ExtractionError(msg) from e
    except Exception as e:
        msg = f"Error reading raw CSV file {path}: {e}"
        logger.error(msg)
        raise ExtractionError(msg) from e

    # Validate header columns
    missing_cols = [col for col in EXPECTED_RAW_COLUMNS if col not in df.columns]
    if missing_cols:
        msg = f"Raw CSV missing required columns: {missing_cols}. Observed: {list(df.columns)}"
        logger.error(msg)
        raise ExtractionError(msg)

    # Attach 1-indexed deterministic source_row_id for source-to-database traceability
    # Note: Row 1 corresponds to line 2 in CSV (first data line after header)
    df.insert(0, "source_row_id", range(1, len(df) + 1))

    metadata = {
        "source_file": str(path.resolve()),
        "file_size_mb": round(file_size_mb, 2),
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
    }

    logger.info(
        f"Extraction complete: {metadata['row_count']:,} rows and {metadata['column_count']} columns extracted."
    )
    return df, metadata


if __name__ == "__main__":
    setup_logging()
    df_sample, meta = extract_raw_data(nrows=100)
    print("Metadata:", meta)
    print(df_sample.head())
