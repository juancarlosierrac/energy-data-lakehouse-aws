# **AWS Energy Data Lakehouse - Data Engineering Project**

## **Introduction**
This project implements a complete end-to-end data engineering solution on AWS, following the Medallion Architecture. The use case simulates an energy trading company that ingests, processes, and serves data from multiple sources (providers, clients, transactions) in a unified Lakehouse platform. The goal is to build a scalable, secure, and governed data pipeline from raw CSV exports to business-ready analytics.

---

## **Project Overview**
The pipeline processes three main data sources exported as CSV:
- **Providers**: contractual terms, risk scores, pricing.
- **Clients**: residential, commercial, and industrial customer data.
- **Transactions**: energy volumes, prices, totals, dates.

---

### **Key Features**
- **Raw Layer**: Daily CSV ingestion into S3 (`raw/load_date=YYYY-MM-DD/`).
- **Bronze Layer**: Type normalization, duplicate removal, and base Parquet conversion via AWS Glue.
- **Silver Layer**: Enriched joins, data validation, and risk categorization.
- **Gold Layer**: Aggregated KPIs (revenue by region, risk summary, customer stats).
- **Automated Catalog**: Glue Crawlers to discover and catalog schemas.
- **Query & Exploration**: Amazon Athena and SparkSQL notebooks.
- **Orchestration**: EventBridge rules to schedule Glue jobs and Crawlers.

---

## **Architecture**
<div align="center">
    <img src="https://raw.githubusercontent.com/juancarlosierrac/energy-data-lakehouse-aws/main/images/energy_project.png" 
         alt="AWS Energy Data Lakehouse Architecture" 
         width="800px"/>
</div>

---

## **Objective**
To demonstrate a production-grade AWS Data Lakehouse solution for an energy trading business, focusing on:
- **Incremental Data Ingestion**  
- **Data Quality & Governance**  
- **Scalable ETL with AWS Glue**  
- **Medallion Layering (Raw, Bronze, Silver, Gold)**  
- **Serverless Analytics with Athena & SparkSQL**

---

## **Technologies Used**
- **Amazon S3** (raw, bronze, silver, gold layers)  
- **AWS Glue** (PySpark ETL jobs & Crawlers)  
- **AWS Lambda / EventBridge** (optional orchestration)  
- **Amazon Athena** (serverless interactive queries)  
- **AWS IAM & Lake Formation** (security & governance)  
- **Apache Spark** (SparkSQL notebooks)  

---

## **Folder Structure in S3**
```text
s3://datalake-<account-id>-energy/
├── raw/
│   └── load_date=YYYY-MM-DD/
│       ├── providers.csv
│       ├── clients.csv
│       └── transactions.csv
├── bronze/
│   └── ingest_date=YYYY-MM-DD/
│       ├── providers/
│       ├── clients/
│       └── transactions/
├── silver/
│   └── transform_date=YYYY-MM-DD/
│       └── transactions_enriched/
└── gold/
    └── publish_date=YYYY-MM-DD/
        ├── revenue_by_region/
        ├── risk_summary/
        └── customer_stats/

---