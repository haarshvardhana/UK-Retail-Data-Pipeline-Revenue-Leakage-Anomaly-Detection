# UK RETAIL DATA PIPELINE: REVENUE LEAKAGE & ANOMALY DETECTION

## 📋 PROJECT OVERVIEW
Led a 3-person collaborative team to audit a large-scale dataset of 541,000+ transactions from a UK-based retailer. The project focused on transforming raw, messy transaction logs into a high-integrity financial model to identify "Revenue Leakage"—the delta between potential gross sales and actual retained revenue.

**Data Source:** UCI Machine Learning Repository - Online Retail Dataset (541,909 rows of transactions).

---

## 🛠️ TECHNICAL STACK
- **Platform:** Google BigQuery (SQL)  
- **Scale:** 541,000+ Rows  
- **Concepts:** ELT Pipeline, Statistical Outlier Detection (Z-Score), RFM Segmentation, Cohort Analysis  

---

## 🧮 THE "PHYSICS" OF THE DATA (CORE LOGIC)

As the lead for the Logic & Engineering phase, I developed the scripts to solve three critical business problems:

- **THE NET REVENUE PARADOX:** Engineered logic in the ELT phase to handle return transactions (negative values) to calculate True Net Revenue and isolate "Bleed" without double-counting losses.

- **STATISTICAL OUTLIER DETECTION:** Applied a Rolling Z-Score algorithm to daily return volumes. By calculating the standard deviation over a 30-day moving window, we flagged "Anomaly Days" where losses were >2σ from the mean.

- **REVENUE LEAKAGE QUANTIFICATION:** Created an Operational Friction Coefficient (Leakage %) within an RFM matrix to identify "Sleeping Whales"—high-value customers whose return behavior exceeds 10% of their Lifetime Value.

---

## 📂 REPOSITORY STRUCTURE

- `01_ELT_Cleaning_Pipeline.sql`: Handles multi-format timestamp parsing, NULL handling, and diagnostic flag creation.  
- `02_Revenue_and_RFM_Modeling.sql`: Calculates Net LTV, Frequency, and Recency to segment the customer base.  
- `03_Statistical_Anomaly_Detection.sql`: Implementation of the Rolling Z-Score physics logic to isolate high-variance loss days.  
- `04_Retention_Cohort_Analysis.sql`: Tracks monthly user decay and retention patterns across the 12-month period.  
- `99_full_pipeline_master.sql`: A comprehensive master script consolidating the end-to-end engineering logic.  

---

## 🚀 KEY RESULTS

- **SCALE:** Optimized SQL queries to process 0.5M+ rows in seconds using BigQuery's columnar storage.  
- **INSIGHT:** Identified that ~7% of total revenue was lost through a specific segment of high-frequency returners.  
- **AUTOMATION:** Developed a repeatable framework for detecting financial anomalies and "Sleeping Whales" in raw logs.
