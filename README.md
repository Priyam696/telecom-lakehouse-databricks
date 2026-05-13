# 🏗️ Telecom Revenue Analytics Lakehouse
### End-to-End Azure Databricks Data Engineering Project

![Azure](https://img.shields.io/badge/Azure-Databricks-orange)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-blue)
![CI/CD](https://img.shields.io/badge/CI/CD-GitHub_Actions-green)
![Unity Catalog](https://img.shields.io/badge/Unity-Catalog-purple)
![DLT](https://img.shields.io/badge/Delta_Live-Tables-red)

---

## 📌 Project Overview

A production-grade **Telecom Revenue Analytics Lakehouse** built on 
Azure Databricks using the **Medallion Architecture** 
(Bronze → Silver → Gold).

The pipeline ingests IBM Telco Customer Churn data, applies 
enterprise-grade transformations, enforces data quality gates, 
and delivers business KPIs to a Power BI dashboard — fully 
automated via Databricks Workflows and CI/CD.

**Dataset:** IBM Telco Customer Churn (Kaggle) — 7,043 records  
**Domain:** Telecom — Subscriber Analytics, Churn Analysis, 
Revenue Reporting

---

## 🏛️ Architecture
Raw CSV (ADLS Bronze)
↓
01-bronze-ingestion.py     [ADF Triggered Daily 6AM]
↓
02-silver-transform.py     [PySpark ETL + Data Quality]
↓
06-data-quality-validation.py  [Quality Gate — 85% threshold]
↓
04-gold-aggregation.py     [Business KPIs + Aggregations]
↓
Power BI Dashboard         [Churn Rate, Revenue, Segments]

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Lake | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks + Apache Spark |
| Orchestration | Databricks Workflows + Azure Data Factory |
| Table Format | Delta Lake (ACID, Time Travel, Schema Evolution) |
| Governance | Unity Catalog (Lineage, Access Control) |
| Streaming/Auto | Delta Live Tables (DLT) |
| CI/CD | GitHub Actions + Databricks Asset Bundles |
| Visualization | Power BI |
| Language | Python, PySpark, SQL |

---

## 📁 Project Structure

telecom-lakehouse-databricks/
│
├── notebooks/
│   ├── 01-bronze-ingestion.py       # Raw CSV ingestion to ADLS Bronze
│   ├── 02-silver-transform.py       # PySpark cleaning + Delta write
│   ├── 04-gold-aggregation.py       # Business KPI aggregations
│   ├── 05-delta-lake-features.py    # ACID, Time Travel, ZORDER, MERGE
│   ├── 06-data-quality-validation.py# Quality gate + audit log
│   └── 10-dlt-telecom-pipeline.py   # Delta Live Tables pipeline
│
├── .github/
│   └── workflows/
│       └── deploy.yml               # GitHub Actions CI/CD
│
├── databricks.yml                   # Databricks Asset Bundle config
├── .gitignore
└── README.md

---

## 🔄 Pipeline Details

### Bronze Layer — Raw Ingestion
- Reads CSV from ADLS Gen2 Bronze container
- Stores raw data as-is (schema preserved)
- Triggered daily via Azure Data Factory at 6 AM IST

### Silver Layer — Transformation
- Deduplication on `customerID`
- Type casting (`TotalCharges` to Double)
- Null handling and categorical standardization
- `Churn_Flag` derived column (1=Churned, 0=Active)
- Written as **Delta Lake** with `overwriteSchema`

### Data Quality Gate
- 7 automated checks (nulls, duplicates, ranges, categoricals)
- **Quality Score** calculated (0-100%)
- Results logged to Delta `audit_log` table
- Pipeline halts if score < 85% — Gold NOT updated

### Gold Layer — Business KPIs
- Churn rate by Contract type and Internet Service
- Revenue analysis by Payment Method
- Overall churn KPI summary
- Written as Delta tables — Power BI ready

---

## ⚡ Delta Lake Features Implemented

| Feature | Implementation |
|---|---|
| ACID Transactions | MERGE upserts for subscriber reconciliation |
| Time Travel | `VERSION AS OF` for audit and rollback |
| Schema Evolution | `mergeSchema=true` for upstream changes |
| ZORDER | Optimized on `Contract` + `Churn` columns |
| OPTIMIZE | File compaction on Silver table |
| RESTORE TABLE | Disaster recovery from bad loads |

---

## 🏛️ Unity Catalog

- **Catalog:** `dbx_telecom_lakehouse`
- **Schemas:** bronze, silver, gold
- **External Locations:** ADLS Gen2 containers registered
- **Data Lineage:** Full notebook-to-table lineage tracked
- **Column Comments:** Business metadata on all key columns
- **Tags:** domain, layer, owner on all tables

---

## 🔁 Delta Live Tables Pipeline
bronze_raw_churn (7K records)
↓
silver_subscribers (7K records)
├── @expect: valid_customer_id
├── @expect_or_drop: valid_churn
└── @expect_or_drop: positive_charges
↓
┌────┴──────────────────────┐
↓                           ↓                    ↓
gold_churn_by_contract    gold_churn_summary   gold_revenue_by_payment
(9 records)              (1 record)           (4 records)

**Result:** 0 errors, 0 warnings — all nodes green ✅

---

## 🔄 CI/CD Pipeline

Every push to `main` triggers:
git push origin main
↓
GitHub Actions fires automatically
↓
Install Databricks CLI
↓
databricks bundle deploy --target dev
↓
Pipeline deployed to Databricks ✅

---

## 📊 Key Results

| Metric | Value |
|---|---|
| Total Customers | 7,043 |
| Overall Churn Rate | 26.5% |
| Month-to-month Churn | 42.7% |
| Two-year Contract Churn | 2.8% |
| Data Quality Score | 100% |
| Pipeline SLA | 98% |
| Query Time Improvement | 40% (via ZORDER) |

---

## 🎓 Certifications

- Databricks Certified Professional Data Engineer
- Databricks Certified Associate Data Engineer

---

## 👩‍💻 Author

**Priyam Sharma**  
Senior Data Engineer | 5.5 years TCS  
Azure Databricks + ADF + Delta Lake + Unity Catalog  

LinkedIn: linkedin.com/in/priyam-sharma-014744224  
GitHub: github.com/Priyam696