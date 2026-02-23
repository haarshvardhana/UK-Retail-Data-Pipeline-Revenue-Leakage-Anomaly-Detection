-- Purpose: Monthly Cohort Analysis to track user retention and decay.
CREATE OR REPLACE TABLE `uk_retail_audit.cohort_retention` AS
WITH First_Purchase AS (
    SELECT customer_id, DATE_TRUNC(MIN(order_date), MONTH) AS cohort_month
    FROM `uk_retail_audit.cleaned_transactions`
    WHERE is_registered_user = 1 AND is_return = 0
    GROUP BY 1
),
Activity AS (
    SELECT 
        t.customer_id,
        f.cohort_month,
        DATE_DIFF(DATE_TRUNC(t.order_date, MONTH), f.cohort_month, MONTH) AS month_number
    FROM `uk_retail_audit.cleaned_transactions` t
    JOIN First_Purchase f ON t.customer_id = f.customer_id
    WHERE is_return = 0
    GROUP BY 1, 2, 3
)
SELECT 
    cohort_month,
    month_number,
    COUNT(DISTINCT customer_id) AS active_users
FROM Activity
GROUP BY 1, 2
ORDER BY 1, 2;