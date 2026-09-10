# Big Data SWOT Analytics Pipeline for Retail Banking

## Overview

This project implements a **Big Data SWOT Analytics Pipeline for Retail Banking**.

The pipeline combines internal retail banking datasets with external market sentiment data to help management identify:

- Strengths
- Weaknesses
- Opportunities
- Threats

The solution uses **HDFS, Spark, Hive, and HBase**, with each technology serving a specific role in the analytics pipeline.

---

## Project Objectives

The main objectives of this project are to:

- Store large-scale banking data using HDFS.
- Process and transform data using Apache Spark.
- Perform batch analytics using Hive.
- Support real-time lookups using HBase.
- Clean and validate internal banking datasets.
- Analyze external market sentiment.
- Generate SWOT insights from internal and external data.
- Apply performance optimization techniques.

---

## Datasets

The project uses four datasets.

| Dataset | Rows | Description |
|---|---:|---|
| `customer_data.csv` | 10,000 | Customer profiles and regional information |
| `transaction_data.csv` | 10,000 | Banking transaction information |
| `bank_data.csv` | 1,000 | Branch revenue, expenses, and profitability |
| `external_bank_sentiment.csv` | 300 | External competitor sentiment and market information |

The internal datasets are mainly used to identify **Strengths and Weaknesses**, while the external sentiment dataset is used to identify **Opportunities and Threats**.

---

## Big Data 4Vs

### Volume

The internal datasets contain a total of **21,000 records**, while real-world banking systems can generate millions of transactions. HDFS provides scalable distributed storage for this type of workload.

### Velocity

Banking data can be processed through daily batch jobs, while external sentiment and market signals may require more frequent monitoring.

### Variety

The project combines structured banking datasets with external sentiment data containing topics, comments, sentiment scores, competitors, and engagement information.

### Veracity

The datasets contain data quality issues such as missing values and potentially noisy external sentiment data. Data cleaning and validation are therefore performed before analysis.

---

## Business Processes

The pipeline supports the following business processes:

### Transaction Performance Analysis

Analyzes transaction values by region and account type to identify internal strengths and weaknesses.

**Technology:** Hive

### Branch Performance Analysis

Analyzes branch profitability and regional performance.

**Technology:** Hive / Spark

### Competitor Monitoring

Provides fast access to competitor, region, date, and topic information.

**Technology:** HBase

### Social Sentiment Monitoring

Analyzes sentiment and engagement by topic.

**Technology:** Spark / Hive

---

## System Architecture

```text
                    Retail Banking Data
                           |
             +-------------+-------------+
             |                           |
        Internal Data              External Data
             |                           |
            HDFS                        HDFS
             |                           |
           Spark                       Spark
             |                           |
      Cleaning & ETL          Sentiment Processing
             |                           |
            Hive                        HBase
             |                           |
     Strength / Weakness       Opportunity / Threat
             |                           |
             +-------------+-------------+
                           |
                    SWOT Synthesis
                           |
                    Management Report
