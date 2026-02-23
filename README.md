📋 Project Overview
Led a 3-person collaborative team to audit a large-scale dataset of 541,000+ transactions from a UK-based retailer. The project focused on transforming raw, messy transaction logs into a high-integrity financial model to identify "Revenue Leakage"—the delta between potential gross sales and actual retained revenue.

🛠️ Technical Stack
Platform: Google BigQuery (SQL)

Scale: 541,000+ Rows

Concepts: Feature Engineering, Statistical Outlier Detection (Z-Score), Data Cleaning, Financial Auditing.

🧮 The "Physics" of the Data (Core Logic)
As the lead for the Logic & Engineering phase, I developed the scripts to solve three critical business problems:

The Net Revenue Paradox: Engineered logic to handle return transactions (negative values) to calculate True Net Revenue without double-counting losses.

Statistical Outlier Detection: Applied a Z-Score algorithm to return frequencies. By calculating the standard deviation of return rates, we flagged "Serial Returners" whose behavior was >3 standard deviations from the mean.

Revenue Leakage Quantification: Created a "Friction Coefficient" (Leakage %) to quantify margin loss from shipping and restocking.

📂 Repository Structure
01_cleaning_logic.sql: Initial data staging, handling NULLs, and currency formatting.

02_revenue_modeling.sql: The logic for Gross vs. Net revenue calculations.

03_anomaly_detection.sql: Statistical scripts identifying high-risk customer behavior.

🚀 Key Results
Scale: Optimized SQL queries to process 0.5M+ rows in seconds.

Insight: Identified that ~7% of total revenue was lost through a specific segment of high-frequency returners.

Automation: Developed a repeatable framework for detecting financial anomalies in raw transaction logs.
