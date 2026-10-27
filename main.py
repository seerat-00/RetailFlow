"""RetailFlow - Main Application Entrypoint.

Retail ETL, Data Quality & Sales Analytics Pipeline.
"""

from src.config import setup_logging
from src.pipeline import run_pipeline


def main():
    """Main execution entrypoint for RetailFlow."""
    setup_logging()
    run_pipeline()


if __name__ == "__main__":
    main()
