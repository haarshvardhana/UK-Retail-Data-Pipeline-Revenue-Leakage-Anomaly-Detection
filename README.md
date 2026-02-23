# UK RETAIL DATA PIPELINE: REVENUE LEAKAGE & ANOMALY DETECTION

## 📋 PROJECT OVERVIEW
Led a 3-person collaborative team to audit a large-scale dataset of 541,000+ transactions from a UK-based retailer. The project focused on transforming raw, messy transaction logs into a high-integrity financial model to identify "Revenue Leakage", the delta between potential gross sales and actual retained revenue.

**Data Source:** UCI Machine Learning Repository - Online Retail Dataset (541,909 rows of transactions).

---

## 🛠️ TECHNICAL STACK
- **Platform:** Google BigQuery (SQL)  
- **Scale:** 541,000+ Rows  
- **Concepts:** ELT Pipeline, Statistical Outlier Detection (Z-Score), RFM Segmentation, Cohort Analysis  
- **Architecture:** Dedicated ELT (Extract, Load, Transform) framework using CTE-heavy (Common Table Expressions) modular design  

---

## 🧮 THE "PHYSICS" OF THE DATA (CORE LOGIC)

As the lead for the Logic & Engineering phase, I developed the scripts to solve three critical business problems:

- **THE NET REVENUE PARADOX:** Engineered logic in the ELT phase to handle return transactions (negative values) to calculate True Net Revenue and isolate "Bleed" without double-counting losses.

- **STATISTICAL OUTLIER DETECTION:** Applied a Rolling Z-Score algorithm to daily return volumes. By calculating the standard deviation over a 30-day moving window, we flagged "Anomaly Days" where losses were >2σ from the mean.  
  (See Anomaly Detection screenshot for the 3-sigma flagging in action).

 ## Z-Score Formula Used:  Z = (X - μ) / σ

Where:  
X = Daily Return Volume  
μ = 30-day Moving Average  
σ = 30-day Rolling Standard Deviation  

- **REVENUE LEAKAGE QUANTIFICATION:** Created an Operational Friction Coefficient (Leakage %) within an RFM matrix to identify "Sleeping Whales"—high-value customers whose return behavior exceeds 10% of their Lifetime Value.

---

## 📂 REPOSITORY STRUCTURE

- `📂 sql_scripts.sql`: A comprehensive master script consolidating the end-to-end engineering logic.
  
-- `01_ELT_Cleaning_Pipeline.sql`: The Foundation. Handles multi-format timestamp parsing and data integrity checks.  
> Implemented a recursive COALESCE parsing strategy to resolve 5+ inconsistent date/time formats common in Excel-exported retail logs.

-- `02_Revenue_and_RFM_Modeling.sql`: The Business Logic. Segments customers and calculates the "Leakage" metrics.

-- `03_Statistical_Anomaly_Detection.sql`: The Math. Implements a 30-day rolling window Z-Score to flag financial outliers.

-- `04_Retention_Cohort_Analysis.sql`: The Strategy. Tracks the 12-month lifecycle and churn patterns.

- `📂 dev_and_research/`: Contains raw scratchpad scripts and initial exploratory queries used during the R&D phase of the audit.

---

## 🔁 How to Reproduce

> 1. Upload the UCI Online Retail CSV to Google BigQuery.  
> 2. Run the scripts in the sql_scripts folder in sequential order (01 to 04).  
> 3. The final audit tables will be generated in your dataset.  

**Note:** This project utilizes BigQuery-specific syntax such as SAFE.PARSE_TIMESTAMP and WINDOW functions for optimized columnar processing.

---

## 🚀 KEY RESULTS

- **SCALE:** Optimized SQL queries to process 0.5M+ rows in seconds using BigQuery's columnar storage.  
- **INSIGHT:** Identified that ~7% of total revenue was lost through a specific segment of high-frequency returners.  
- **AUTOMATION:** Developed a repeatable framework for detecting financial anomalies and "Sleeping Whales" in raw logs.

  Z-Score Formula Used:
