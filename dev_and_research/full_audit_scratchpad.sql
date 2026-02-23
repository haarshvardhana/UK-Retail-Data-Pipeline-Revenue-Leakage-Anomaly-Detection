CREATE OR REPLACE TABLE `uk_retail_audit.cleaned_transactions` AS

WITH Raw_Ingestion AS (
    -- Step 1: Force standard data types using SAFE_CAST to prevent crashes
    SELECT 
        TRIM(InvoiceNo) AS invoice_no,
        TRIM(StockCode) AS stock_code,
        TRIM(Description) AS product_description,
        SAFE_CAST(Quantity AS INT64) AS quantity,
        
        -- We will try standard cast first. If it's formatted weirdly, we will fix it next.
        SAFE_CAST(InvoiceDate AS TIMESTAMP) AS invoice_date, 
        
        SAFE_CAST(UnitPrice AS FLOAT64) AS unit_price,
        TRIM(CustomerID) AS customer_id,
        TRIM(Country) AS country
    FROM `uk_retail_audit.raw_transactions`
),

Physics_Logic_Layer AS (
    -- Step 2: Build the Diagnostic Flags and Financial Math
    SELECT 
        invoice_no,
        stock_code,
        product_description,
        quantity,
        invoice_date,
        unit_price,
        customer_id,
        country,
        
        -- FLAG 1: The 'Bleed' Identifier. If it starts with 'C', it's a cancellation.
        CASE WHEN STARTS_WITH(invoice_no, 'C') THEN 1 ELSE 0 END AS is_return,
        
        -- FLAG 2: Cohort Tracking. Can we track this user, or were they a guest?
        CASE WHEN customer_id IS NULL OR customer_id = '' THEN 0 ELSE 1 END AS is_registered_user,
        
        -- THE MATH: Calculate the exact financial footprint of every row
        ROUND(quantity * unit_price, 2) AS line_total,
        
        -- EXTRACT: Isolate the Date for faster time-series grouping later
        DATE(invoice_date) AS order_date

    FROM Raw_Ingestion
    
    -- Step 3: The Filter (Removing 'Noise' to find the 'Signal')
    WHERE unit_price > 0 
      AND stock_code NOT IN ('POST', 'D', 'M', 'PADS', 'DOT', 'CRUK') 
      AND product_description IS NOT NULL 
)

-- Step 4: Final Output
SELECT * FROM Physics_Logic_Layer;

CREATE OR REPLACE TABLE `uk_retail_audit.cleaned_transactions` AS

WITH Raw_Ingestion AS (
    SELECT 
        TRIM(InvoiceNo) AS invoice_no,
        TRIM(StockCode) AS stock_code,
        TRIM(Description) AS product_description,
        SAFE_CAST(TRIM(Quantity) AS INT64) AS quantity,
        
        -- THE DATE FIX: Forcing BigQuery to read every possible dirty date format
        COALESCE(
            SAFE_CAST(TRIM(InvoiceDate) AS TIMESTAMP),
            SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M', TRIM(InvoiceDate)),
            SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M', TRIM(InvoiceDate)),
            SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', TRIM(InvoiceDate)),
            SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(InvoiceDate))
        ) AS invoice_date, 
        
        SAFE_CAST(TRIM(UnitPrice) AS FLOAT64) AS unit_price,
        TRIM(CustomerID) AS customer_id,
        TRIM(Country) AS country
    FROM `uk_retail_audit.raw_transactions`
),

Physics_Logic_Layer AS (
    SELECT 
        invoice_no,
        stock_code,
        product_description,
        quantity,
        invoice_date,
        unit_price,
        customer_id,
        country,
        
        CASE WHEN STARTS_WITH(invoice_no, 'C') THEN 1 ELSE 0 END AS is_return,
        CASE WHEN customer_id IS NULL OR customer_id = '' THEN 0 ELSE 1 END AS is_registered_user,
        ROUND(quantity * unit_price, 2) AS line_total,
        
        -- We now have a clean date to group by!
        DATE(invoice_date) AS order_date

    FROM Raw_Ingestion
    
    WHERE unit_price > 0 
      AND stock_code NOT IN ('POST', 'D', 'M', 'PADS', 'DOT', 'CRUK') 
      AND product_description IS NOT NULL 
)

SELECT * FROM Physics_Logic_Layer;


SELECT * FROM uk_retail_audit.cleaned_transactions LIMIT 20

SELECT InvoiceDate FROM `uk_retail_audit.raw_transactions` LIMIT 5;

CREATE OR REPLACE TABLE `uk_retail_audit.cleaned_transactions` AS

WITH Raw_Ingestion AS (
    SELECT 
        TRIM(InvoiceNo) AS invoice_no,
        TRIM(StockCode) AS stock_code,
        TRIM(Description) AS product_description,
        SAFE_CAST(TRIM(Quantity) AS INT64) AS quantity,
        
        -- THE SNIPER FIX: Capturing the exact Excel-corrupted format with dashes
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
    WHERE TRIM(InvoiceNo) != 'InvoiceNo' -- Kill the header row that snuck in!
),

Physics_Logic_Layer AS (
    SELECT 
        invoice_no,
        stock_code,
        product_description,
        quantity,
        invoice_date,
        unit_price,
        customer_id,
        country,
        
        CASE WHEN STARTS_WITH(invoice_no, 'C') THEN 1 ELSE 0 END AS is_return,
        CASE WHEN customer_id IS NULL OR customer_id = '' THEN 0 ELSE 1 END AS is_registered_user,
        ROUND(quantity * unit_price, 2) AS line_total,
        
        DATE(invoice_date) AS order_date

    FROM Raw_Ingestion
    
    WHERE unit_price > 0 
      AND stock_code NOT IN ('POST', 'D', 'M', 'PADS', 'DOT', 'CRUK') 
      AND product_description IS NOT NULL 
)

SELECT * FROM Physics_Logic_Layer;

SELECT * FROM uk_retail_audit.cleaned_transactions LIMIT 20

CREATE OR REPLACE TABLE `uk_retail_audit.daily_return_anomalies` AS

WITH Daily_Metrics AS (
    -- Step 1: Aggregate data to the daily level
    SELECT 
        order_date,
        -- Gross Revenue (Good sales)
        SUM(CASE WHEN is_return = 0 THEN line_total ELSE 0 END) AS expected_revenue,
        -- Margin Bleed (We use ABS to turn negative return values into a positive 'loss' number)
        SUM(CASE WHEN is_return = 1 THEN ABS(line_total) ELSE 0 END) AS revenue_lost_to_returns,
        -- Count how many distinct customers experienced a failure today
        COUNT(DISTINCT CASE WHEN is_return = 1 THEN customer_id END) AS customers_impacted
    FROM `uk_retail_audit.cleaned_transactions`
    WHERE order_date IS NOT NULL
    GROUP BY order_date
),

Rolling_Physics AS (
    -- Step 2: Calculate the 30-Day Moving Averages and Standard Deviations (The Ground Truth)
    SELECT 
        order_date,
        expected_revenue,
        revenue_lost_to_returns,
        customers_impacted,
        
        -- The 30-Day Rolling Average of Returns
        AVG(revenue_lost_to_returns) OVER (
            ORDER BY order_date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS rolling_30d_avg_loss,
        
        -- The 30-Day Rolling Standard Deviation (The Variance)
        STDDEV(revenue_lost_to_returns) OVER (
            ORDER BY order_date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS rolling_30d_stddev_loss
        
    FROM Daily_Metrics
)

-- Step 3: Calculate the Z-Score and trigger the Anomaly Alert
SELECT 
    order_date,
    expected_revenue,
    revenue_lost_to_returns,
    rolling_30d_avg_loss,
    rolling_30d_stddev_loss,
    customers_impacted,
    
    -- Z-SCORE MATH: (Actual - Average) / Standard Deviation
    ROUND(
        SAFE_DIVIDE(
            (revenue_lost_to_returns - rolling_30d_avg_loss), 
            rolling_30d_stddev_loss
        ), 2
    ) AS z_score,
    
    -- THE KILL SHOT: If the Z-Score is > 2, the system officially broke that day.
    CASE 
        WHEN SAFE_DIVIDE((revenue_lost_to_returns - rolling_30d_avg_loss), rolling_30d_stddev_loss) >= 2.0 THEN 'HIGH VARIANCE ANOMALY'
        WHEN SAFE_DIVIDE((revenue_lost_to_returns - rolling_30d_avg_loss), rolling_30d_stddev_loss) <= -2.0 THEN 'HIGH EFFICIENCY DAY'
        ELSE 'NORMAL OPERATIONS' 
    END AS operational_status

FROM Rolling_Physics
ORDER BY order_date DESC;


SELECT * FROM uk_retail_audit.daily_return_anomalies LIMIT 20

CREATE OR REPLACE TABLE `uk_retail_audit.customer_rfm_churn_matrix` AS

WITH Global_Context AS (
    -- Pre-calculating the 'current date' of the dataset to avoid subquery errors
    SELECT MAX(order_date) AS max_dataset_date 
    FROM `uk_retail_audit.cleaned_transactions`
),

Customer_Base AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_purchase_date,
        MAX(order_date) AS last_purchase_date,
        
        -- FREQUENCY: Count of distinct valid orders
        COUNT(DISTINCT CASE WHEN is_return = 0 THEN invoice_no END) AS frequency_total_orders,
        
        -- MONETARY: Gross vs Returns
        SUM(CASE WHEN is_return = 0 THEN line_total ELSE 0 END) AS gross_lifetime_value,
        SUM(CASE WHEN is_return = 1 THEN ABS(line_total) ELSE 0 END) AS total_return_value
        
    FROM `uk_retail_audit.cleaned_transactions`
    WHERE is_registered_user = 1 
    GROUP BY customer_id
)

SELECT 
    c.customer_id,
    c.first_purchase_date,
    c.last_purchase_date,
    
    -- RECENCY: Now using the pre-calculated date from Global_Context
    DATE_DIFF(g.max_dataset_date, c.last_purchase_date, DAY) AS recency_days,
    
    c.frequency_total_orders,
    c.gross_lifetime_value,
    c.total_return_value,
    (c.gross_lifetime_value - c.total_return_value) AS net_lifetime_value,
    
    CASE 
        WHEN c.gross_lifetime_value > 0 THEN ROUND((c.total_return_value / c.gross_lifetime_value) * 100, 2) 
        ELSE 0 
    END AS personal_return_rate_pct,
    
    -- THE KILL SHOT FLAG: "Sleeping Whales"
    CASE 
        WHEN c.gross_lifetime_value >= 2000 
         AND DATE_DIFF(g.max_dataset_date, c.last_purchase_date, DAY) >= 60 
         AND SAFE_DIVIDE(c.total_return_value, c.gross_lifetime_value) > 0.10 
        THEN 1 ELSE 0 
    END AS is_sleeping_whale

FROM Customer_Base c
CROSS JOIN Global_Context g
WHERE c.gross_lifetime_value > 0
ORDER BY net_lifetime_value DESC;

CREATE OR REPLACE TABLE `uk_retail_audit.cohort_retention` AS

WITH First_Purchase AS (
    -- Step 1: Find the month each customer made their first ever purchase
    SELECT 
        customer_id, 
        DATE_TRUNC(MIN(order_date), MONTH) AS cohort_month
    FROM `uk_retail_audit.cleaned_transactions`
    WHERE is_registered_user = 1 AND is_return = 0
    GROUP BY 1
),

Activity AS (
    -- Step 2: Track which subsequent months they came back to buy again
    SELECT 
        t.customer_id,
        f.cohort_month,
        -- Calculate the 'Month Number' (Month 0, Month 1, Month 2...)
        DATE_DIFF(DATE_TRUNC(t.order_date, MONTH), f.cohort_month, MONTH) AS month_number
    FROM `uk_retail_audit.cleaned_transactions` t
    JOIN First_Purchase f ON t.customer_id = f.customer_id
    WHERE is_return = 0
    GROUP BY 1, 2, 3
)

-- Step 3: Aggregate to see the decay of each cohort over time
SELECT 
    cohort_month,
    month_number,
    COUNT(DISTINCT customer_id) AS active_users
FROM Activity
GROUP BY 1, 2
ORDER BY 1, 2;