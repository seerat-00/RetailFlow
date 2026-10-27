"""Master ETL Pipeline Orchestrator for RetailFlow."""

import sys
import time
from datetime import datetime
from typing import Dict, Any
import logging

from src.config import settings, setup_logging
from src.extract import extract_raw_data, ExtractionError
from src.transform import transform_data
from src.validate import validate_data, DataQualityReport
from src.load import load_pipeline_data
from src.analytics import fetch_executive_kpis, export_analytical_views_to_csv, print_executive_summary

logger = logging.getLogger("retailflow.pipeline")


def run_pipeline() -> Dict[str, Any]:
    """
    Execute full RetailFlow data engineering pipeline:
    Extract -> Transform -> Validate -> Load -> Analytics.
    """
    pipeline_start = datetime.now()
    t_start = time.time()

    logger.info("=" * 60)
    logger.info("RETAILFLOW ETL PIPELINE EXECUTION STARTED")
    logger.info("=" * 60)
    logger.info(f"Target Database: {settings.db_name} on {settings.db_host}:{settings.db_port}")
    logger.info(f"Database User: {settings.db_user} | Schema: {settings.db_schema}")
    logger.info(f"Source CSV Path: {settings.raw_data_path}")

    # Phase B: Source Extraction
    try:
        t0 = time.time()
        raw_df, extract_meta = extract_raw_data()
        t_extract = time.time() - t0
        logger.info(f"Phase B complete: Extracted {len(raw_df):,} rows in {t_extract:.2f}s.")
    except ExtractionError as e:
        logger.error(f"Pipeline aborted at Extraction phase: {e}")
        raise

    # Phase C: Transformation
    t0 = time.time()
    transformed_df, transform_stats = transform_data(raw_df)
    t_transform = time.time() - t0
    logger.info(f"Phase C complete: Transformed {len(transformed_df):,} rows in {t_transform:.2f}s.")

    # Phase D: Data Quality Validation
    t0 = time.time()
    accepted_df, rejected_df, dq_report = validate_data(transformed_df)
    t_validate = time.time() - t0
    logger.info(f"Phase D complete: Validated {dq_report.total_input_rows:,} rows in {t_validate:.2f}s.")

    # Phase E: PostgreSQL Loading and Audit
    t0 = time.time()
    load_summary = load_pipeline_data(
        accepted_df=accepted_df,
        rejected_df=rejected_df,
        report=dq_report,
        started_at=pipeline_start,
    )
    t_load = time.time() - t0
    logger.info(f"Phase E complete: Loaded {load_summary['loaded_accepted']:,} records in {t_load:.2f}s.")

    # Phase F: Analytics & Tableau Views Export
    t0 = time.time()
    exported_csvs = export_analytical_views_to_csv()
    t_analytics = time.time() - t0
    logger.info(f"Phase F complete: Exported {len(exported_csvs)} views in {t_analytics:.2f}s.")

    total_duration = time.time() - t_start

    # Final Summary Compilation
    summary = {
        "status": "SUCCESS",
        "started_at": pipeline_start.isoformat(),
        "completed_at": datetime.now().isoformat(),
        "total_duration_seconds": round(total_duration, 2),
        "extraction_duration_seconds": round(t_extract, 2),
        "transformation_duration_seconds": round(t_transform, 2),
        "validation_duration_seconds": round(t_validate, 2),
        "loading_duration_seconds": round(t_load, 2),
        "analytics_duration_seconds": round(t_analytics, 2),
        "extraction_metadata": extract_meta,
        "transformation_stats": transform_stats,
        "data_quality_report": dq_report.to_dict(),
        "database_load_summary": load_summary,
        "exported_csv_count": len(exported_csvs),
    }

    # Print Final Status
    logger.info("=" * 60)
    logger.info(f"RETAILFLOW ETL PIPELINE COMPLETED SUCCESSFULLY IN {total_duration:.2f}s")
    logger.info("=" * 60)

    print("\n" + "=" * 65)
    print("           RETAILFLOW PIPELINE EXECUTION SUMMARY")
    print("=" * 65)
    print(f" Status:                     SUCCESS")
    print(f" Total Runtime:              {total_duration:.2f} seconds")
    print(f" Source File:                {extract_meta['source_file']}")
    print(f" Input Records Extracted:    {dq_report.total_input_rows:,}")
    print(f" Records Accepted into DB:   {dq_report.accepted_rows:,} ({dq_report.accepted_rows / dq_report.total_input_rows * 100:.2f}%)")
    print(f" Records Quarantined:        {dq_report.rejected_rows:,} ({dq_report.rejected_rows / dq_report.total_input_rows * 100:.2f}%)")
    print(f" Exact Duplicates Flagged:   {dq_report.duplicate_rows:,}")
    print(f" Reconciliation Validated:   {dq_report.is_reconciled()}")
    print("-" * 65)
    print(f" Database Run ID:            {load_summary['run_id']}")
    print(f" Fact Records in Database:   {load_summary['verification']['db_fct_count']:,}")
    print(f" Rejected Records in DB:     {load_summary['verification']['db_rejected_count']:,}")
    print(f" Gross Commercial Sales:     ${load_summary['gross_sales']:,.2f}")
    print(f" Returns & Refunds:          ${load_summary['return_amount']:,.2f}")
    print(f" Net Realized Revenue:       ${load_summary['net_sales']:,.2f}")
    print("=" * 65)

    print_executive_summary()

    return summary


def main():
    """CLI Entry point."""
    setup_logging()
    try:
        run_pipeline()
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Fatal error during pipeline execution: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
