-- Purpose: Calculate rolling Z-Scores to isolate high-variance return days.
CREATE OR REPLACE TABLE `uk_retail_audit.daily_return_anomalies` AS
WITH Daily_Metrics AS (
    SELECT 
        order_date,
        SUM(CASE WHEN is_return = 0 THEN line_total ELSE 0 END) AS expected_revenue,
        SUM(CASE WHEN is_return = 1 THEN ABS(line_total) ELSE 0 END) AS revenue_lost_to_returns
    FROM `uk_retail_audit.cleaned_transactions`
    GROUP BY order_date
),
Rolling_Physics AS (
    SELECT 
        *,
        AVG(revenue_lost_to_returns) OVER (ORDER BY order_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS rolling_30d_avg,
        STDDEV(revenue_lost_to_returns) OVER (ORDER BY order_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS rolling_30d_stddev
    FROM Daily_Metrics
)
SELECT 
    *,
    ROUND(SAFE_DIVIDE((revenue_lost_to_returns - rolling_30d_avg), rolling_30d_stddev), 2) AS z_score,
    CASE WHEN SAFE_DIVIDE((revenue_lost_to_returns - rolling_30d_avg), rolling_30d_stddev) >= 2.0 THEN 'ANOMALY' ELSE 'NORMAL' END AS status
FROM Rolling_Physics;