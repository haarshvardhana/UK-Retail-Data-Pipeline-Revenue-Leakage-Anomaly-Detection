#UK RETAIL DATA PIPELINE: REVENUE LEAKAGE & ANOMALY DETECTION
##📋 PROJECT OVERVIEW
Led a 3-person collaborative team to audit a large-scale dataset of 541,000+ transactions from a UK-based retailer. The project focused on transforming raw, messy transaction logs into a high-integrity financial model to identify "Revenue Leakage"—the delta between potential gross sales and actual retained revenue.

##🛠️ TECHNICAL STACK
Platform: Google BigQuery (SQL)

#Scale: 541,000+ Rows

##Concepts: Feature Engineering, Statistical Outlier Detection (Z-Score), Data Cleaning, Financial Auditing.

##🧮 THE "PHYSICS" OF THE DATA (CORE LOGIC)
As the lead for the Logic & Engineering phase, I developed the scripts to solve three critical business problems:

- THE NET REVENUE PARADOX: Engineered logic to handle return transactions (negative values) to calculate True Net Revenue without double-counting losses.

- STATISTICAL OUTLIER DETECTION: Applied a Z-Score algorithm to return frequencies. By calculating the standard deviation of return rates, we flagged "Serial Returners" whose behavior was >3 standard deviations from the mean.

- REVENUE LEAKAGE QUANTIFICATION: Created a "Friction Coefficient" (Leakage %) to quantify margin loss from shipping and restocking.

##📂 REPOSITORY STRUCTURE
- 01_cleaning_logic.sql: Initial data staging, handling NULLs, and currency formatting.
- 02_revenue_modeling.sql: The logic for Gross vs. Net revenue calculations.
- 03_anomaly_detection.sql: Statistical scripts identifying high-risk customer behavior.

##🚀 KEY RESULTS
- SCALE: Optimized SQL queries to process 0.5M+ rows in seconds.
- INSIGHT: Identified that ~7% of total revenue was lost through a specific segment of high-frequency returners.
- AUTOMATION: Developed a repeatable framework for detecting financial anomalies in raw transaction logs.
- SCALE: Optimized SQL queries to process 0.5M+ rows in seconds.
- INSIGHT: Identified that ~7% of total revenue was lost through a specific segment of high-frequency returners.
- AUTOMATION: Developed a repeatable framework for detecting financial anomalies in raw transaction logs.
