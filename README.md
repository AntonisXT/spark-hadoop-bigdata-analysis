# ⚡ Spark & Hadoop Big Data Processing

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.x-orange)]()
[![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.3.x-yellow)]()
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)]()

> A distributed data processing and performance analysis project using **Apache Spark** and **Apache Hadoop (HDFS)** — implemented as part of an academic assignment on Big Data systems.

---

## 🎯 Objective

This project demonstrates how to design, implement, and analyze **large-scale data processing pipelines** using **Hadoop** and **Spark** on a distributed cluster.  
It aims to compare the performance of different data representations, APIs, and join strategies within Spark.

### 🔑 Key Features
- Distributed environment using **HDFS**, **YARN**, and **Spark 3.5.x**
- Data ingestion pipeline (**CSV → Parquet**)
- Comparative analysis of:
  - **RDD API vs Spark SQL / DataFrames**
  - **CSV vs Parquet performance**
  - **Join strategies:** Broadcast, Repartition, and Catalyst Optimizer (BroadcastHashJoin vs SortMergeJoin)
- Execution time visualizations and performance insights

---

## 📂 Project Structure

```
spark-hadoop-bigdata-analysis/
├── data_ingestion/          # CSV → Parquet converters
├── queries/                 # RDD & SQL queries (CSV + Parquet)
├── joins/                   # Broadcast, Repartition, Catalyst ON/OFF
├── visualizations/          # Execution time plots
├── images/                  # Screenshots & charts for README
├── README.md
├── output.md
└── requirements.txt
```

---

## ⚙️ Setup

### 🧩 Environment
Developed and tested on **Ubuntu-based Virtual Machines** configured with:
- **Java 11**
- **Hadoop 3.3.x** (HDFS + YARN + Job History Server)
- **Spark 3.5.x** (running in YARN mode)
- **Python 3.8+**

### 🐍 Python Dependencies
Defined in `requirements.txt`:
```txt
pyspark>=3.5.0
matplotlib>=3.7.0
numpy>=1.24.0
```

Install dependencies:
```bash
pip install -r requirements.txt
```

### 📦 Dataset
A subset of the **Common Crawl** dataset (WARC, WAT, WET) plus small relational datasets (**employees**, **departments**).

Download and load into HDFS:
```bash
wget http://www.cslab.ece.ntua.gr/~ikons/bigdata/project2024.tar.gz
tar -xzf project2024.tar.gz

hadoop fs -mkdir -p /home/user/csv_files
hadoop fs -put warc.csv wat.csv wet.csv employees.csv departments.csv /home/user/csv_files
```

---

## 🔄 CSV → Parquet Conversion

Convert CSV datasets to **Parquet** and store them in HDFS:
```bash
spark-submit data_ingestion/warc_parquet.py
spark-submit data_ingestion/wat_parquet.py
spark-submit data_ingestion/wet_parquet.py
spark-submit data_ingestion/employees_parquet.py
spark-submit data_ingestion/departments_parquet.py
```

📸 Uploaded CSV files (HDFS):  
![CSV files in HDFS](images/csv_files.png)

📸 Converted Parquet files (HDFS):  
![Parquet files in HDFS](images/parquet_files.png)

---

## 🔎 Queries (Part 1)

The project implements **five analytical queries**, each executed using **RDD API**, **Spark SQL on CSV**, and **Spark SQL on Parquet** to compare execution times and performance.

| **Query ID** | **Description** |
|---------------|-----------------|
| **Q1** | For the time range between *2017-03-22 22:00* and *2017-03-22 23:00*, find the **top 5 most used servers**, in **descending order of usage**. |
| **Q2** | For the target URL `http://1001.ru/articles/post/ai-da-tumin-443`, find the **metadata length** (from WAT) and the **HTML DOM size** (from WARC). |
| **Q3** | Find the top **5 (warc_record_id, target_url, content_length)** with the **largest content length**, where the server is *Apache*. |
| **Q4** | For each server, compute the **average WARC content length** and the **average WAT metadata length**, then return the **top 5 servers** by average WARC content length. |
| **Q5** | Find the **most popular target URL**, i.e., the URL that appears most often inside the HTML DOM of other records. |

---

### 🚀 Example Usage

**RDD API:**
```bash
spark-submit queries/rdd_q1.py
```
📸 RDD Execution (Hadoop Job History UI):  
![Execution RDD](images/exec_rdd.png)

**Spark SQL (Parquet):**
```bash
spark-submit queries/df_q1.py
```
📸 Spark SQL on Parquet:  
![Execution SQL Parquet](images/exec_sql_parquet.png)

**Spark SQL (CSV):**
```bash
spark-submit queries/df_csv_q1.py
```
📸 Spark SQL on CSV:  
![Execution SQL CSV](images/exec_sql_csv.png)

---

## 🔗 Joins (Part 2)

Different **join strategies** were evaluated using the *employees* and *departments* datasets.

### 🔸 Broadcast Join (RDD API)
Broadcasts the small `departments` dataset to all executors for efficient join.
```bash
spark-submit joins/joins_broadcast_rdd.py
```
<p align="center">
  <img src="images/broadcast_join_50.png" width="49%">
  <img src="images/broadcast_join_100.png" width="49%">
</p>

### 🔸 Repartition Join (RDD API)
Repartitions both datasets by department ID and joins using `cogroup`.
```bash
spark-submit joins/joins_repartition_rdd.py
```
<p align="center">
  <img src="images/repartition_join_50.png" width="49%" height="400px">
  <img src="images/repartition_join_100.png" width="49%" height="400px">
</p>

### 🔸 Catalyst Optimizer (Spark SQL)
Compares Catalyst’s **BroadcastHashJoin** (enabled) vs **SortMergeJoin** (disabled).

```bash
spark-submit joins/join_broadcast_vs_sortmerge.py Y   # Disable broadcast
spark-submit joins/join_broadcast_vs_sortmerge.py N   # Enable broadcast
```

📸 Catalyst enabled (BroadcastHashJoin):  
![Catalyst Enabled Plan](images/catalyst_enabled.png)

📸 Catalyst disabled (SortMergeJoin):  
![Catalyst Disabled Plan](images/catalyst_disabled.png)

---

## 📈 Results & Performance Analysis

### ⏱️ Execution Times

| Query | RDD | SQL (CSV) | SQL (Parquet) |
|-------|-----|-----------|---------------|
| Q1    | 26  | 72        | 54            |
| Q2    | 24  | 41        | 36            |
| Q3    | 14  | 32        | 30            |
| Q4    | 31  | 34        | 33            |
| Q5    | 29  | 36        | 33            |

📊 Execution time comparison:  
![Execution Times](images/execution_times.png)

#### 🔍 Observations
- **RDD API:** Fastest overall, ideal for low-level transformations.  
- **Spark SQL (CSV):** Slower due to schema inference and parsing.  
- **Spark SQL (Parquet):** Faster than CSV thanks to Parquet’s columnar storage and optimized metadata.

---

### ⚙️ Catalyst Optimizer Impact
- **BroadcastHashJoin (enabled):** Automatically detects and broadcasts small datasets — highest performance.  
- **SortMergeJoin (disabled):** Requires sorting and shuffling — slower for medium datasets.

📊 Catalyst Join Comparison:  
![Catalyst Comparison](images/catalyst_times.png)

---

## 🧠 Insights & Discussion

- **DataFrames + Parquet + Catalyst Optimizer** provide the best trade-off between simplicity and performance.  
- **RDDs** offer more control and are slightly faster for pure transformations.  
- **Parquet** outperforms CSV consistently due to schema-on-read and compression.  
- **Broadcast joins** dramatically reduce shuffling overhead when one dataset is small.

---

## 🔮 Future Work
- Integrate **Apache Airflow** for orchestration of Spark jobs.  
- Experiment with **Spark Streaming** for real-time analysis.  
- Deploy the pipeline on **AWS EMR** or **Google Dataproc** for cloud benchmarking.  
- Add **Docker Compose** for automated local deployment.

---

## 📝 Notes
- All computations were executed on **Ubuntu VMs** with Hadoop + Spark in YARN mode.  
- Data was stored and processed in **HDFS**.  
- The project compares formats (CSV vs Parquet), APIs (RDD vs SQL), and join strategies in distributed systems.

---

## 👤 Author

**Antonis Tsiakiris**  
🎓 Student | Big Data & AI Enthusiast  
🔗 [LinkedIn](https://www.linkedin.com/in/antonis-tsiakiris-880114359)

---

## 📜 License
**MIT License**
