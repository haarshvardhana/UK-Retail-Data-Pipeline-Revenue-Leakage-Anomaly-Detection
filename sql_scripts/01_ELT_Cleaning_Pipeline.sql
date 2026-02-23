-- Purpose: ELT Process to clean raw UK Retail data and handle Excel-corrupted timestamps.
CREATE OR REPLACE TABLE `uk_retail_audit.cleaned_transactions` AS
WITH Raw_Ingestion AS (
    SELECT 
        TRIM(InvoiceNo) AS invoice_no,
        TRIM(StockCode) AS stock_code,
        TRIM(Description) AS product_description,
        SAFE_CAST(TRIM(Quantity) AS INT64) AS quantity,
        COALESCE(
            SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', TRIM(InvoiceDate)),
            SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(InvoiceDate)),
            SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M', TRIM(InvoiceDate)),
            SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M', TRIM(InvoiceDate)),
            SAFE_CAST(TRIM(InvoiceDate) AS TIMESTAMP)
        ) AS invoice_date, 
        SAFE_CAST(TRIM(UnitPrice) AS FLOAT64) AS unit_price,
        TRIM(CustomerID) AS customer_id,
        TRIM(Country) AS country
    FROM `uk_retail_audit.raw_transactions`
    WHERE TRIM(InvoiceNo) != 'InvoiceNo' 
)
SELECT 
    *,
    CASE WHEN STARTS_WITH(invoice_no, 'C') THEN 1 ELSE 0 END AS is_return,
    CASE WHEN customer_id IS NULL OR customer_id = '' THEN 0 ELSE 1 END AS is_registered_user,
    ROUND(quantity * unit_price, 2) AS line_total,
    DATE(invoice_date) AS order_date
FROM Raw_Ingestion
WHERE unit_price > 0 
  AND stock_code NOT IN ('POST', 'D', 'M', 'PADS', 'DOT', 'CRUK') 
  AND product_description IS NOT NULL;