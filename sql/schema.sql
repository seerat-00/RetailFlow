-- RetailFlow Database DDL Schema
-- Target Database: retailflow_db (PostgreSQL 14+)
-- Target Schema: retailflow

CREATE SCHEMA IF NOT EXISTS retailflow;

-- ============================================================================
-- 1. Pipeline Runs Audit Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS retailflow.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(50) NOT NULL DEFAULT 'retailflow_etl',
    status VARCHAR(20) NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    input_rows INT,
    accepted_rows INT,
    rejected_rows INT,
    duplicate_rows INT,
    gross_sales_amount NUMERIC(14,2),
    return_amount NUMERIC(14,2),
    net_sales_amount NUMERIC(14,2),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(8,2),
    error_message TEXT
);

-- ============================================================================
-- 2. Clean Transactions Fact Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS retailflow.fct_transactions (
    source_row_id INT PRIMARY KEY,
    invoice VARCHAR(30) NOT NULL,
    stock_code VARCHAR(30) NOT NULL,
    description VARCHAR(255),
    quantity INT NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    customer_id INT,
    country VARCHAR(100) NOT NULL,
    line_total NUMERIC(12,2) NOT NULL,
    is_cancellation BOOLEAN NOT NULL DEFAULT FALSE,
    is_return BOOLEAN NOT NULL DEFAULT FALSE,
    transaction_type VARCHAR(20) NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for Query Performance
CREATE INDEX IF NOT EXISTS idx_fct_tx_invoice ON retailflow.fct_transactions(invoice);
CREATE INDEX IF NOT EXISTS idx_fct_tx_stock_code ON retailflow.fct_transactions(stock_code);
CREATE INDEX IF NOT EXISTS idx_fct_tx_invoice_date ON retailflow.fct_transactions(invoice_date);
CREATE INDEX IF NOT EXISTS idx_fct_tx_customer_id ON retailflow.fct_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_fct_tx_country ON retailflow.fct_transactions(country);
CREATE INDEX IF NOT EXISTS idx_fct_tx_type ON retailflow.fct_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_fct_tx_date_type ON retailflow.fct_transactions(invoice_date, transaction_type);

-- ============================================================================
-- 3. Data Quality Quarantined / Rejected Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS retailflow.data_quality_rejected (
    source_row_id INT PRIMARY KEY,
    invoice VARCHAR(50),
    stock_code VARCHAR(50),
    description TEXT,
    quantity VARCHAR(50),
    invoice_date VARCHAR(50),
    price VARCHAR(50),
    customer_id VARCHAR(50),
    country VARCHAR(100),
    rejection_reasons TEXT NOT NULL,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_reasons ON retailflow.data_quality_rejected(rejection_reasons);
