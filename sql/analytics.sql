-- RetailFlow Analytical SQL Views
-- Schema: retailflow
-- Target Database: retailflow_db (PostgreSQL 14+)

-- ============================================================================
-- 1. Monthly Sales Trends
-- Measures gross sales, return amounts, net revenue, order volume, and AOV
-- ============================================================================
CREATE OR REPLACE VIEW retailflow.vw_monthly_sales_trends AS
WITH monthly_agg AS (
    SELECT
        TO_CHAR(invoice_date, 'YYYY-MM') AS order_month,
        DATE_TRUNC('month', invoice_date)::DATE AS month_start_date,
        COUNT(DISTINCT invoice) AS total_invoices,
        COUNT(DISTINCT CASE WHEN transaction_type = 'SALE' THEN invoice END) AS sales_orders,
        COUNT(DISTINCT CASE WHEN is_cancellation THEN invoice END) AS cancellation_orders,
        COUNT(DISTINCT customer_id) AS active_customers,
        SUM(CASE WHEN transaction_type = 'SALE' THEN quantity ELSE 0 END) AS items_sold,
        SUM(CASE WHEN is_return THEN ABS(quantity) ELSE 0 END) AS items_returned,
        SUM(CASE WHEN transaction_type = 'SALE' THEN line_total ELSE 0 END) AS gross_revenue,
        SUM(CASE WHEN is_return OR is_cancellation THEN ABS(line_total) ELSE 0 END) AS return_amount,
        SUM(line_total) AS net_revenue
    FROM retailflow.fct_transactions
    GROUP BY 1, 2
)
SELECT
    order_month,
    month_start_date,
    total_invoices,
    sales_orders,
    cancellation_orders,
    active_customers,
    items_sold,
    items_returned,
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(return_amount, 2) AS return_amount,
    ROUND(net_revenue, 2) AS net_revenue,
    ROUND(100.0 * cancellation_orders / NULLIF(total_invoices, 0), 2) AS cancellation_rate_pct,
    ROUND(gross_revenue / NULLIF(sales_orders, 0), 2) AS avg_order_value
FROM monthly_agg
ORDER BY month_start_date ASC;

-- ============================================================================
-- 2. Top Products by Net Revenue
-- Ranks products by commercial performance with return rate tracking
-- ============================================================================
CREATE OR REPLACE VIEW retailflow.vw_top_products_by_revenue AS
WITH product_agg AS (
    SELECT
        stock_code,
        MAX(description) AS product_name,
        COUNT(DISTINCT invoice) AS total_orders,
        SUM(CASE WHEN transaction_type = 'SALE' THEN quantity ELSE 0 END) AS units_sold,
        SUM(CASE WHEN is_return THEN ABS(quantity) ELSE 0 END) AS units_returned,
        SUM(quantity) AS net_units,
        SUM(CASE WHEN transaction_type = 'SALE' THEN line_total ELSE 0 END) AS gross_revenue,
        SUM(CASE WHEN is_return OR is_cancellation THEN ABS(line_total) ELSE 0 END) AS return_amount,
        SUM(line_total) AS net_revenue
    FROM retailflow.fct_transactions
    GROUP BY stock_code
)
SELECT
    stock_code,
    product_name,
    total_orders,
    units_sold,
    units_returned,
    net_units,
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(return_amount, 2) AS return_amount,
    ROUND(net_revenue, 2) AS net_revenue,
    ROUND(100.0 * units_returned / NULLIF(units_sold + units_returned, 0), 2) AS return_rate_pct,
    DENSE_RANK() OVER (ORDER BY net_revenue DESC) AS revenue_rank
FROM product_agg
ORDER BY net_revenue DESC;

-- ============================================================================
-- 3. Country-Level Sales Performance
-- Evaluates market penetration, global revenue contribution, and customer base
-- ============================================================================
CREATE OR REPLACE VIEW retailflow.vw_country_sales_performance AS
WITH country_agg AS (
    SELECT
        country,
        COUNT(DISTINCT invoice) AS order_count,
        COUNT(DISTINCT customer_id) AS customer_count,
        SUM(CASE WHEN transaction_type = 'SALE' THEN quantity ELSE 0 END) AS units_sold,
        SUM(CASE WHEN transaction_type = 'SALE' THEN line_total ELSE 0 END) AS gross_revenue,
        SUM(CASE WHEN is_return OR is_cancellation THEN ABS(line_total) ELSE 0 END) AS return_amount,
        SUM(line_total) AS net_revenue
    FROM retailflow.fct_transactions
    GROUP BY country
)
SELECT
    country,
    order_count,
    customer_count,
    units_sold,
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(return_amount, 2) AS return_amount,
    ROUND(net_revenue, 2) AS net_revenue,
    ROUND(100.0 * net_revenue / NULLIF(SUM(net_revenue) OVER (), 0), 2) AS pct_of_global_revenue,
    ROUND(gross_revenue / NULLIF(order_count, 0), 2) AS avg_order_value
FROM country_agg
ORDER BY net_revenue DESC;

-- ============================================================================
-- 4. Customer Order Metrics and Lifetime Value
-- Customer-level retention, frequency, monetary value, and tier segmentation
-- ============================================================================
CREATE OR REPLACE VIEW retailflow.vw_customer_order_metrics AS
WITH cust_agg AS (
    SELECT
        customer_id,
        MAX(country) AS country,
        MIN(invoice_date)::DATE AS first_purchase_date,
        MAX(invoice_date)::DATE AS latest_purchase_date,
        COUNT(DISTINCT invoice) AS total_invoices,
        COUNT(DISTINCT CASE WHEN transaction_type = 'SALE' THEN invoice END) AS total_orders,
        COUNT(DISTINCT CASE WHEN is_cancellation THEN invoice END) AS cancellation_orders,
        SUM(CASE WHEN transaction_type = 'SALE' THEN quantity ELSE 0 END) AS total_units_purchased,
        SUM(CASE WHEN transaction_type = 'SALE' THEN line_total ELSE 0 END) AS gross_spend,
        SUM(CASE WHEN is_return OR is_cancellation THEN ABS(line_total) ELSE 0 END) AS return_amount,
        SUM(line_total) AS net_spend
    FROM retailflow.fct_transactions
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    customer_id,
    country,
    first_purchase_date,
    latest_purchase_date,
    (latest_purchase_date - first_purchase_date) AS customer_tenure_days,
    total_invoices,
    total_orders,
    cancellation_orders,
    total_units_purchased,
    ROUND(gross_spend, 2) AS gross_spend,
    ROUND(return_amount, 2) AS return_amount,
    ROUND(net_spend, 2) AS net_spend,
    ROUND(net_spend / NULLIF(total_orders, 0), 2) AS avg_order_value,
    CASE
        WHEN net_spend >= 5000 THEN 'Platinum'
        WHEN net_spend >= 2000 THEN 'Gold'
        WHEN net_spend >= 500 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier
FROM cust_agg
ORDER BY net_spend DESC;

-- ============================================================================
-- 5. Returns and Cancellation Analysis
-- Monitors monthly refund volume and return ratios
-- ============================================================================
CREATE OR REPLACE VIEW retailflow.vw_returns_and_cancellations AS
SELECT
    TO_CHAR(invoice_date, 'YYYY-MM') AS order_month,
    COUNT(DISTINCT CASE WHEN is_cancellation THEN invoice END) AS cancellation_count,
    SUM(CASE WHEN is_return THEN ABS(quantity) ELSE 0 END) AS returned_items_count,
    ROUND(SUM(CASE WHEN is_return OR is_cancellation THEN ABS(line_total) ELSE 0 END), 2) AS total_refund_amount,
    ROUND(SUM(CASE WHEN transaction_type = 'SALE' THEN line_total ELSE 0 END), 2) AS gross_sales_amount,
    ROUND(
        100.0 * SUM(CASE WHEN is_return OR is_cancellation THEN ABS(line_total) ELSE 0 END)
        / NULLIF(SUM(CASE WHEN transaction_type = 'SALE' THEN line_total ELSE 0 END), 0),
        2
    ) AS refund_to_gross_ratio_pct
FROM retailflow.fct_transactions
GROUP BY 1
ORDER BY order_month ASC;

-- ============================================================================
-- 6. Daily Sales Summary
-- Granular time-series metrics with day-of-week breakdown
-- ============================================================================
CREATE OR REPLACE VIEW retailflow.vw_daily_sales_summary AS
SELECT
    invoice_date::DATE AS order_date,
    TO_CHAR(invoice_date, 'Day') AS day_of_week,
    EXTRACT(DOW FROM invoice_date)::INT AS day_of_week_num,
    COUNT(DISTINCT invoice) AS order_count,
    SUM(CASE WHEN transaction_type = 'SALE' THEN quantity ELSE 0 END) AS items_sold,
    ROUND(SUM(CASE WHEN transaction_type = 'SALE' THEN line_total ELSE 0 END), 2) AS gross_revenue,
    ROUND(SUM(CASE WHEN is_return OR is_cancellation THEN ABS(line_total) ELSE 0 END), 2) AS return_amount,
    ROUND(SUM(line_total), 2) AS net_revenue
FROM retailflow.fct_transactions
GROUP BY 1, 2, 3
ORDER BY order_date ASC;

-- ============================================================================
-- 7. Data Quality Audit Summary
-- Aggregates quarantined records and rejection reasons
-- ============================================================================
CREATE OR REPLACE VIEW retailflow.vw_data_quality_summary AS
SELECT
    rejection_reasons,
    COUNT(*) AS rejected_record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total_rejected
FROM retailflow.data_quality_rejected
GROUP BY rejection_reasons
ORDER BY rejected_record_count DESC;
