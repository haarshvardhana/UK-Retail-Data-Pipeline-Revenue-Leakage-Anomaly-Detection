-- Purpose: RFM Analysis to identify high-value customers at risk of churning.
CREATE OR REPLACE TABLE `uk_retail_audit.customer_rfm_churn_matrix` AS
WITH Base AS (
    SELECT 
        customer_id,
        DATE_DIFF((SELECT MAX(order_date) FROM `uk_retail_audit.cleaned_transactions`), MAX(order_date), DAY) AS recency,
        COUNT(DISTINCT CASE WHEN is_return = 0 THEN invoice_no END) AS frequency,
        SUM(CASE WHEN is_return = 0 THEN line_total ELSE 0 END) AS gmv,
        SUM(CASE WHEN is_return = 1 THEN ABS(line_total) ELSE 0 END) AS returns
    FROM `uk_retail_audit.cleaned_transactions`
    WHERE is_registered_user = 1
    GROUP BY customer_id
)
SELECT 
    *,
    (gmv - returns) AS net_ltv,
    ROUND((returns/gmv)*100, 2) AS return_rate_pct,
    CASE WHEN gmv >= 2000 AND recency >= 60 AND (returns/gmv) > 0.10 THEN 1 ELSE 0 END AS is_sleeping_whale
FROM Base WHERE gmv > 0;