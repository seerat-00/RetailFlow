# RetailFlow — Tableau Analytics Dashboard Guide

This guide details how to connect Tableau Desktop or Tableau Public to RetailFlow analytical data (either via direct PostgreSQL live/extract connection or pre-aggregated CSV exports) and build a production-grade retail executive dashboard.

---

## 1. Data Connection Options

### Option A: Direct PostgreSQL Connection (Recommended for Tableau Desktop)

1. Open **Tableau Desktop**.
2. Under **Connect** > **To a Server**, select **PostgreSQL**.
3. Enter your local connection details:
   - **Server:** `localhost`
   - **Port:** `5432`
   - **Database:** `retailflow_db`
   - **Authentication:** Username and Password
   - **Username:** `chehak`
   - **Password:** *(Your local database password from `.env`)*
4. Select schema: `retailflow`.
5. You will see the prepared analytical views:
   - `vw_monthly_sales_trends`
   - `vw_top_products_by_revenue`
   - `vw_country_sales_performance`
   - `vw_customer_order_metrics`
   - `vw_returns_and_cancellations`
   - `vw_daily_sales_summary`
   - `vw_data_quality_summary`
6. Drag `vw_monthly_sales_trends` into the canvas, or create a data source for each dashboard sheet.

---

### Option B: Pre-aggregated CSV Exports (Tableau Public / Offline Fallback)

If using **Tableau Public** (which does not support direct live PostgreSQL connections without extract publishing) or working offline, the pipeline automatically exports clean analytical tables into `data/processed/`:

| View / File | Location | Primary Purpose |
| :--- | :--- | :--- |
| `monthly_sales_trends.csv` | `data/processed/monthly_sales_trends.csv` | Monthly executive KPIs, trends, and AOV |
| `top_products_by_revenue.csv` | `data/processed/top_products_by_revenue.csv` | Product ranking and return analysis |
| `country_sales_performance.csv`| `data/processed/country_sales_performance.csv` | Geographic revenue breakdown & map |
| `customer_order_metrics.csv` | `data/processed/customer_order_metrics.csv` | Customer tiering and retention |
| `returns_and_cancellations.csv`| `data/processed/returns_and_cancellations.csv`| Refund ratios and monthly returns |
| `daily_sales_summary.csv` | `data/processed/daily_sales_summary.csv` | Granular daily sales and day-of-week trends |
| `data_quality_summary.csv` | `data/processed/data_quality_summary.csv` | Audit summary of rejected vs accepted records |

To connect in Tableau Public:
1. Open **Tableau Public**.
2. Under **Connect** > **To a File**, select **Text file**.
3. Navigate to `~/RetailFlow/data/processed/` and select `monthly_sales_trends.csv` or `daily_sales_summary.csv`.

---

## 2. Dashboard Architecture & Visual Specifications

The dashboard layout is designed as an **Executive Sales Performance & Data Quality Dashboard** (1366 x 768 or 1920 x 1080 resolution).

```
+----------------------------------------------------------------------------------------------------+
|                                    RETAILFLOW EXECUTIVE DASHBOARD                                  |
|   Filters: [Date Range: 2009-12 to 2011-12]  |  [Country: All]  |  [Customer Tier: All]           |
+----------------------------------------------------------------------------------------------------+
|  [KPI 1: Net Revenue]   [KPI 2: Gross Sales]   [KPI 3: Return Amount]   [KPI 4: Orders]  [AOV]     |
|       $17.78M                  $18.89M                $1.11M                 43,654       $432.72  |
+----------------------------------------------------+-----------------------------------------------+
|  CHART 1: Monthly Net vs Gross Revenue Trend       |  CHART 2: Top 10 Products by Net Revenue      |
|  (Dual-Axis Area + Line Chart over 24 Months)     |  (Horizontal Bar Chart with Return Overlay)   |
+----------------------------------------------------+-----------------------------------------------+
|  CHART 3: Global Sales by Country                  |  CHART 4: Data Quality & Pipeline Health      |
|  (Choropleth Map / Tree Map of Geographic Sales)  |  (Donut Chart: 96.22% Accepted vs 3.78% Rej.) |
+----------------------------------------------------+-----------------------------------------------+
```

---

## 3. Step-by-Step Worksheet Construction

### Sheet 1: Executive KPI Scorecards
- **Data Source:** `vw_monthly_sales_trends` (or `vw_daily_sales_summary`)
- **Fields:**
  - **Net Revenue:** `SUM([Net Revenue])` — Formatted as Currency (`$#,##0.00`).
  - **Gross Sales:** `SUM([Gross Revenue])` — Formatted as Currency.
  - **Total Returns:** `SUM([Return Amount])` — Formatted as Currency.
  - **Total Orders:** `SUM([Sales Orders])` — Formatted as Integer (`#,##0`).
  - **Average Order Value (AOV):** `SUM([Gross Revenue]) / SUM([Sales Orders])` — Currency.
- **Mark Type:** Text / KPI Card.

### Sheet 2: Monthly Sales & Return Trends
- **Data Source:** `vw_monthly_sales_trends`
- **Columns:** `Month Start Date` (Continuous Month: `MONTH(month_start_date)`).
- **Rows:**
  - Measure 1: `SUM([Gross Revenue])` (Area Chart, subtle blue gradient).
  - Measure 2: `SUM([Net Revenue])` (Line Chart, deep navy blue).
  - Measure 3: `SUM([Return Amount])` (Dual-Axis Bar or Red Line).
- **Tooltips:** Order Month, Sales Orders, Cancellation Count, Return Amount, Net Revenue.

### Sheet 3: Top 10 Products by Net Revenue
- **Data Source:** `vw_top_products_by_revenue`
- **Rows:** `Product Name` (Sorted descending by `SUM([Net Revenue])`).
- **Filter:** `Product Name` top 10 by field `Net Revenue` (or filter `Revenue Rank <= 10`).
- **Columns:** `SUM([Net Revenue])`.
- **Color:** `Return Rate Pct` (Diverging Red-Green palette to highlight high-return products).
- **Label:** `SUM([Net Revenue])` formatted as `$K`.

### Sheet 4: Geographic Revenue Distribution
- **Data Source:** `vw_country_sales_performance`
- **Marks:** Map (Latitude/Longitude generated from `Country`).
- **Color:** `SUM([Net Revenue])` (Sequential Blue palette).
- **Size / Detail:** `Country`, `Order Count`, `Customer Count`.
- **Top Markets:** United Kingdom, EIRE, Germany, France, Netherlands.

### Sheet 5: Data Quality Audit Summary
- **Data Source:** `vw_data_quality_summary`
- **Chart Type:** Horizontal Bar or Donut Chart.
- **Dimension:** `Rejection Reasons` (`RULE_EXACT_DUPLICATE`, `RULE_ZERO_PRICE`, `RULE_NEGATIVE_PRICE`).
- **Measure:** `SUM([Rejected Record Count])`.
- **Callout Label:** Acceptance Rate: `96.22%` (1,026,999 records accepted), Rejection Rate: `3.78%` (40,372 records quarantined).

---

## 4. Key Business Definitions

| Metric | Formula / Business Definition | Why It Matters |
| :--- | :--- | :--- |
| **Gross Commercial Sales** | `SUM(line_total)` where `transaction_type = 'SALE'` | Total customer purchasing activity before any cancellations or returns. |
| **Return / Refund Amount** | `SUM(ABS(line_total))` where `is_return = TRUE` or `is_cancellation = TRUE` | Direct revenue lost due to returns, refunds, or cancellations. |
| **Net Revenue** | `Gross Commercial Sales - Return Amount` (`SUM(line_total)`) | Actual realized cash/revenue generated by the retail business. |
| **Average Order Value (AOV)** | `Gross Revenue / Sales Orders` | Key indicator of purchasing basket size and marketing effectiveness. |
| **Cancellation Rate** | `100.0 * Cancellation Orders / Total Invoices` | Measures order fulfillment friction and customer cancellation frequency. |

---

## 5. Adding Dashboard Screenshots

After assembling the dashboard in Tableau Desktop or Tableau Public:
1. Export the dashboard image: **Dashboard** > **Export Image...** (or take a screenshot: `Cmd + Shift + 4` on macOS).
2. Save the screenshot file into:
   ```text
   dashboard/screenshots/retailflow_executive_dashboard.png
   dashboard/screenshots/retailflow_quality_audit.png
   ```
3. The screenshots are automatically embedded in `README.md`.
