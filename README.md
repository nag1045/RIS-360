# RIS-360 – Retirement and Income Solution
## “The platform is built on a medallion architecture using S3 as the data lake. Data is processed using Glue PySpark jobs across bronze, silver, and gold layers. Query access is provided through Athena workgroups, and curated data is served from Redshift.
## Key Characteristics


# 🚀 RIS-360: AWS Data Lakehouse Pipeline

## 📌 Overview
RIS-360 is an end-to-end **data engineering project** that demonstrates how to build a **scalable batch data platform on AWS** using a modern lakehouse architecture.

This project is inspired by real-world enterprise systems, where data is ingested, processed in layers, and served for analytics and reporting.

---

## 🏗️ Architecture

### 🔄 High-Level Flow

Python Producer / External Sources → S3 (Bronze)  
→ AWS Glue (PySpark) → Silver → Gold  
→ AWS Lake Formation (Governance Layer)  
→ Athena / Redshift (Consumption)

---

## 🧱 Tech Stack

| Layer              | Technology |
|-------------------|-----------|
| Ingestion         | Python, APIs, Batch Loads |
| Storage           | Amazon S3 |
| Processing        | AWS Glue (PySpark) |
| Metadata Catalog  | AWS Glue Data Catalog |
| Governance        | AWS Lake Formation |
| Query Engine      | Amazon Athena |
| Data Warehouse    | Amazon Redshift |
| Orchestration     | Apache Airflow |
| Language          | Python, SQL |

---

## 🧩 Key Features

- ✅ End-to-end **batch data pipeline**
- ✅ **Medallion Architecture** (Bronze, Silver, Gold)
- ✅ Schema enforcement & data validation
- ✅ Incremental data processing
- ✅ Partitioned Parquet datasets for optimized querying
- ✅ Fine-grained access control using Lake Formation
- ✅ CI/CD using GitHub Actions

---

## 🪵 Data Lake Layers

### 🥉 Bronze Layer
- Raw data ingested from APIs / batch sources (JSON/CSV)
- Immutable, append-only storage

### 🥈 Silver Layer
- Data cleaning, schema enforcement, deduplication
- Structured and reliable datasets

### 🥇 Gold Layer
- Aggregated, business-ready datasets
- Designed using **Star Schema**

---

## 📊 Data Modelling

- **Fact Table:** Transactions  
- **Dimension Tables:** Customer, Product, Time  

Designed for:
- Fast analytical queries  
- BI tools (Athena / Redshift)  
- Simplified reporting  

---

## 🔐 Data Governance (Lake Formation)

AWS Lake Formation is implemented between the **Gold layer and consumption layer** to enable:

- Fine-grained access control (row/column level)
- Centralized data permissions
- Secure data sharing across services
- Integration with Athena and Redshift

---

## ⚙️ Pipeline Breakdown

### 🔹 Ingestion
- Data ingested using Python scripts / APIs / batch loads
- Stored in S3 Bronze layer

### 🔹 Transformation
- AWS Glue (PySpark) jobs process:
  - Bronze → Silver (cleaning, deduplication)
  - Silver → Gold (aggregation, modeling)

### 🔹 Serving
- Athena queries data directly from S3
- Redshift used for warehouse-level analytics

### 🔹 Orchestration
- Apache Airflow manages pipeline execution and dependencies

---

## 📈 Performance Optimizations

- Converted raw data to **Parquet format**
- Implemented **partitioning (date-based)**
- Used **predicate pushdown** for efficient queries
- Reduced Spark shuffle using:
  - Broadcast joins
  - Optimal partitioning

---

## 🔄 CI/CD

- GitHub Actions used for deployment automation
- Infrastructure and pipeline updates triggered on push to main branch

---

## 📂 Project Structure

ris-360/  
│  
├── ingestion/          # Python ingestion scripts  
├── glue_jobs/          # PySpark transformation scripts  
├── airflow_dags/       # Pipeline orchestration  
├── scripts/            # Utility scripts  
├── config/             # Configuration files  
└── README.md  

---

## 🚀 Key Learnings

- Designing scalable **data lakehouse architectures**
- Implementing **data governance using Lake Formation**
- Optimizing large-scale data processing with PySpark
- Building production-style pipelines on AWS

---

## 🔗 Reference

This project is a **prototype inspired by real-world enterprise data pipelines**, implemented using publicly available datasets to simulate production scenarios.

---

## 👤 Author

**Nagendra Vishwakarma**  
Data Engineer | AWS | PySpark | SQL  

- Infrastructure as Code using AWS CDK (Python)

- CI/CD via GitHub Actions

- Medallion Data Lake on Amazon S3 (Landing → Bronze → Silver → Gold)

- Distributed Processing using AWS Glue (PySpark)

- Query Layer using Amazon Athena with isolated workgroups per layer

- Serving Layer using Amazon Redshift

- Orchestration using Apache Airflow hosted on EC2

- Multi-environment support (dev / stage / prod)

                ┌────────────────────┐
                │   GitHub Actions   │
                │      (CI / CD)     │
                └─────────┬──────────┘
                          │
                   AWS CDK (Python)
                          │
┌──────────────────────────────────────────────────────────────┐
│                           AWS                                │
│                                                              │
│  ┌──────────────┐       ┌──────────────────────────────┐   │
│  │     S3       │       │            EC2               │   │
│  │  Landing     │◀──────│      Apache Airflow          │   │
│  │   Bucket     │       │        (Orchestration)       │   │
│  └──────┬───────┘       └───────────┬──────────────────┘   │
│         │                            │                      │
│         ▼                            ▼                      │
│  ┌──────────────┐       ┌──────────────────────────────┐   │
│  │     S3       │       │          AWS Glue             │   │
│  │   Bronze     │◀──────│      PySpark ETL Jobs         │   │
│  │   Bucket     │       └───────────┬──────────────────┘   │
│  └──────┬───────┘                   │                      │
│         ▼                            ▼                      │
│  ┌──────────────┐       ┌──────────────────────────────┐   │
│  │     S3       │       │          AWS Athena           │   │
│  │   Silver     │◀──────│        (4 Workgroups)         │   │
│  │   Bucket     │       │   Landing / Bronze /          │   │
│  └──────┬───────┘       │   Silver / Gold               │   │
│         ▼               └───────────┬──────────────────┘   │
│  ┌──────────────┐                   │                      │
│  │     S3       │                   ▼                      │
│  │    Gold      │        ┌────────────────────────────┐   │
│  │   Bucket     │ ─────▶ │        Amazon Redshift      │   │
│  └──────────────┘        │       (Serving Layer)       │   │
│                           └────────────────────────────┘   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
