"""Analytics and business metrics execution module for RetailFlow."""

from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
import psycopg
import logging

from src.config import settings, PROJECT_ROOT
from src.load import get_sqlalchemy_engine

logger = logging.getLogger("retailflow.analytics")


def get_db_connection() -> psycopg.Connection:
    """Create raw database connection."""
    conn_info = (
        f"host={settings.db_host} "
        f"port={settings.db_port} "
        f"dbname={settings.db_name} "
        f"user={settings.db_user} "
        f"password={settings.db_password}"
    )
    return psycopg.connect(conn_info)


def fetch_executive_kpis() -> Dict[str, Any]:
    """Calculate core executive retail KPIs across the entire cleaned fact table."""
    query = """
        SELECT
            COUNT(DISTINCT invoice) AS total_invoices,
            COUNT(DISTINCT CASE WHEN transaction_type = 'SALE' THEN invoice END) AS sales_orders,
            COUNT(DISTINCT CASE WHEN is_cancellation THEN invoice END) AS cancellation_orders,
            COUNT(DISTINCT customer_id) AS total_customers,
            COUNT(DISTINCT stock_code) AS unique_products,
            SUM(CASE WHEN transaction_type = 'SALE' THEN quantity ELSE 0 END) AS total_units_sold,
            SUM(CASE WHEN is_return THEN ABS(quantity) ELSE 0 END) AS total_units_returned,
            ROUND(SUM(CASE WHEN transaction_type = 'SALE' THEN line_total ELSE 0 END), 2) AS gross_revenue,
            ROUND(SUM(CASE WHEN is_return OR is_cancellation THEN ABS(line_total) ELSE 0 END), 2) AS return_refund_amount,
            ROUND(SUM(line_total), 2) AS net_revenue
        FROM retailflow.fct_transactions;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            cols = [desc[0] for desc in cur.description]

    kpis = dict(zip(cols, row))

    # Derived rates
    sales_orders = kpis.get("sales_orders") or 1
    total_invoices = kpis.get("total_invoices") or 1
    gross_rev = float(kpis.get("gross_revenue") or 0.0)
    cancellations = kpis.get("cancellation_orders") or 0

    kpis["avg_order_value"] = round(gross_rev / sales_orders, 2)
    kpis["cancellation_rate_pct"] = round(100.0 * cancellations / total_invoices, 2)

    return kpis


def export_analytical_views_to_csv(output_dir: Path = None) -> List[Path]:
    """
    Query all analytical views in PostgreSQL and export clean CSV files
    to data/processed/ for Tableau connection and file-based BI workflows.
    """
    target_dir = output_dir or settings.processed_data_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    views = [
        ("vw_monthly_sales_trends", "monthly_sales_trends.csv"),
        ("vw_top_products_by_revenue", "top_products_by_revenue.csv"),
        ("vw_country_sales_performance", "country_sales_performance.csv"),
        ("vw_customer_order_metrics", "customer_order_metrics.csv"),
        ("vw_returns_and_cancellations", "returns_and_cancellations.csv"),
        ("vw_daily_sales_summary", "daily_sales_summary.csv"),
        ("vw_data_quality_summary", "data_quality_summary.csv"),
    ]

    exported_files = []
    logger.info(f"Exporting analytical views to CSV in {target_dir}...")

    engine = get_sqlalchemy_engine()
    with engine.connect() as conn:
        for view_name, csv_name in views:
            sql = f"SELECT * FROM retailflow.{view_name};"
            df = pd.read_sql_query(sql, conn)
            dest_file = target_dir / csv_name
            df.to_csv(dest_file, index=False)
            exported_files.append(dest_file)
            logger.info(f"Exported {len(df):,} rows from {view_name} -> {dest_file.name}")

    return exported_files


def print_executive_summary() -> None:
    """Print formatted executive metrics to console."""
    kpis = fetch_executive_kpis()
    print("\n" + "=" * 60)
    print(" RETAILFLOW EXECUTIVE SALES ANALYTICS SUMMARY")
    print("=" * 60)
    print(f" Total Net Revenue:        ${kpis['net_revenue']:,.2f}")
    print(f" Gross Commercial Sales:   ${kpis['gross_revenue']:,.2f}")
    print(f" Return / Refund Amount:   ${kpis['return_refund_amount']:,.2f}")
    print(f" Total Sales Orders:       {kpis['sales_orders']:,}")
    print(f" Average Order Value:      ${kpis['avg_order_value']:,.2f}")
    print(f" Total Unique Customers:   {kpis['total_customers']:,}")
    print(f" Total Unique Products:    {kpis['unique_products']:,}")
    print(f" Units Sold (Gross):       {kpis['total_units_sold']:,}")
    print(f" Units Returned:           {kpis['total_units_returned']:,}")
    print(f" Cancellation Rate:        {kpis['cancellation_rate_pct']:.2f}%")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    from src.config import setup_logging
    setup_logging()
    print_executive_summary()
    export_analytical_views_to_csv()
