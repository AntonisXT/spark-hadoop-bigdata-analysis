# 📊 Spark & Hadoop Big Data Processing  

## 📌 Overview  

This project was developed as part of an **academic assignment** on Big Data systems.  
The goal was to implement and evaluate data processing workflows using **Apache Hadoop** and **Apache Spark** in a distributed environment.  

The assignment required implementing:  
- Set up and use Hadoop & Spark on **virtual machines (VMs)** with HDFS.  
- Upload and transform datasets (CSV → Parquet).  
- Implement queries using both **RDD API** and **Spark SQL / DataFrames** (on CSV and Parquet).  
- Compare execution times and analyze performance.  
- Explore join strategies: **Broadcast Join**, **Repartition Join**, and **Catalyst Optimizer** (BroadcastHashJoin vs SortMergeJoin).  

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

### Environment  
The project was implemented and tested on **Ubuntu-based Virtual Machines** configured with:  
- **Java 11**  
- **Hadoop 3.3.x** (HDFS + YARN + Job History Server)  
- **Spark 3.5.x** (running in YARN mode)  
- **Python 3.8+**  

### Python Dependencies  
Python requirements are listed in `requirements.txt`:  

```txt
pyspark>=3.5.0
matplotlib>=3.7.0
numpy>=1.24.0
```  

Install them with:  
```bash
pip install -r requirements.txt
```

### Dataset  
The project uses a subset of the **Common Crawl** dataset (WARC, WAT, WET files) plus small **employees/departments** CSV files.  

Download and load into HDFS:  

```bash
wget http://www.cslab.ece.ntua.gr/~ikons/bigdata/project2024.tar.gz
tar -xzf project2024.tar.gz

hadoop fs -mkdir -p /home/user/csv_files
hadoop fs -put warc.csv wat.csv wet.csv employees.csv departments.csv /home/user/csv_files
```

---

## 🔄 CSV → Parquet Conversion  

Run the following to convert CSV files into Parquet format and store them in HDFS:  

```bash
spark-submit data_ingestion/warc_parquet.py
spark-submit data_ingestion/wat_parquet.py
spark-submit data_ingestion/wet_parquet.py
spark-submit data_ingestion/employees_parquet.py
spark-submit data_ingestion/departments_parquet.py
```

📸 Uploaded CSV files (**Hadoop HDFS**):  
![CSV files in HDFS](images/csv_files.png)  

📸 Converted Parquet files (**Hadoop HDFS**):  
![Parquet files in HDFS](images/parquet_files.png)  

---

## 🔎 Queries (Part 1)  

The project answers five main queries using both **RDD API** and **Spark SQL (CSV & Parquet)**:  

- **Q1:** For the time range between *2017-03-22 22:00 and 2017-03-22 23:00*, find the **top 5 most used servers**, in descending order of usage.  
- **Q2:** For the target URL `http://1001.ru/articles/post/ai-da-tumin-443`, find the **metadata length** (from WAT) and the **HTML DOM size** (from WARC).  
- **Q3:** Find the top **5 (warc_record_id, target_url, content_length)** with the **largest content length**, where the server is Apache.  
- **Q4:** For each server, compute the **average WARC content length** and the **average WAT metadata length**, then return the top 5 servers by average WARC content length.  
- **Q5:** Find the **most popular target URL**, i.e. the URL that appears most often inside the HTML DOM of other records.  

### 🚀 Usage  

**RDD example:**  
```bash
spark-submit queries/rdd_q1.py
```  
📸 RDD — **Hadoop Job History UI**:  
![Execution RDD](images/exec_rdd.png)  

**Spark SQL (Parquet):**  
```bash
spark-submit queries/df_q1.py
```  
📸 Spark SQL on Parquet — **Hadoop Job History UI**:  
![Execution SQL Parquet](images/exec_sql_parquet.png)  

**Spark SQL (CSV):**  
```bash
spark-submit queries/df_csv_q1.py
```  
📸 Spark SQL on CSV — **Hadoop Job History UI**:  
![Execution SQL CSV](images/exec_sql_csv.png)  

---

## 🔗 Joins (Part 2)  

The project evaluates different join strategies on the **employees.csv** and **departments.csv** datasets:  

- **Broadcast Join (RDD API):** The small `departments` dataset is broadcasted to all executors and joined with `employees`.  

  ```bash
  spark-submit joins/joins_broadcast_rdd.py
  ```  
  📸 Example results (first 100 rows):  

  <p align="center">
    <img src="images/broadcast_join_50.png" width="49%">
    <img src="images/broadcast_join_100.png" width="49%">
  </p>

- **Repartition Join (RDD API):** Both datasets are repartitioned by department id, grouped with `cogroup`, and joined.  

  ```bash
  spark-submit joins/joins_repartition_rdd.py
  ```  
  📸 Example results (first 100 rows):  

  <p align="center">
    <img src="images/repartition_join_50.png" width="49%" height="400px">
    <img src="images/repartition_join_100.png" width="49%" height="400px">
  </p>

- **Catalyst Optimizer (Spark SQL):** Compare execution times with broadcast threshold **enabled** (BroadcastHashJoin) vs **disabled** (SortMergeJoin).  

  ```bash
  spark-submit joins/join_broadcast_vs_sortmerge.py Y   # disable broadcast
  spark-submit joins/join_broadcast_vs_sortmerge.py N   # enable
  ```  

  📸 Physical plan (Catalyst enabled):  
  *Performs a series of joins between employees and departments using the Broadcast HashJoin method to optimize performance, while applying filters and data size restrictions to reduce the processing overhead.*  
  ![Catalyst Enabled Plan](images/catalyst_enabled.png)  

  📸 Physical plan (Catalyst disabled):  
  *Performs joins between employees and departments using the Sort-Merge Join method, which requires data sorting. For this specific query and dataset, it is less efficient compared to the Broadcast HashJoin chosen by Catalyst.*  
  ![Catalyst Disabled Plan](images/catalyst_disabled.png)  

---

## 📈 Results & Analysis  

### Execution Times  

| Query | RDD | SQL (CSV) | SQL (Parquet) |
|-------|-----|-----------|---------------|
| Q1    | 26  | 72        | 54            |
| Q2    | 24  | 41        | 36            |
| Q3    | 14  | 32        | 30            |
| Q4    | 31  | 34        | 33            |
| Q5    | 29  | 36        | 33            |

📊 **Execution time comparison:**  
![Execution Times](images/execution_times.png)  

#### Observations:  
- **RDD API (Map/Reduce):** Fastest execution times across all queries. Optimized for large-scale transformations.  
- **Spark SQL on CSV:** Slowest, due to schema inference and raw text parsing overhead.  
- **Spark SQL on Parquet:** Faster than CSV, thanks to Parquet’s columnar format and no schema inference.  

---

### Catalyst Optimizer Analysis  

- **Enabled (BroadcastHashJoin):** Spark automatically broadcasts the smaller dataset, achieving much faster execution.  
- **Disabled (SortMergeJoin):** Requires sorting of large datasets before joining, resulting in slower performance.  

📊 Catalyst join comparison:  
![Catalyst Comparison](images/catalyst_times.png)  

---

## 📝 Notes  
- All computations run on **VMs** configured with Hadoop + Spark.  
- Data is stored and processed in **HDFS**.  
- The project highlights performance differences between formats (CSV vs Parquet), APIs (RDD vs SQL), and join strategies.  

---

## 📜 License  
MIT License.

---

## 🖥️ Environment Setup (How this was installed)

The project runs on **Ubuntu-based VMs** with **Java 11**, **Hadoop 3.3.x**, **Spark 3.5.x**, and **HDFS**.  
Below are the exact high-level steps I used to set it up.

### 1) Install Java
```bash
sudo apt update
sudo apt install -y openjdk-11-jdk
java -version
```

### 2) Install Hadoop (3.3.x)
Download & extract Hadoop, then set environment variables:
```bash
sudo adduser hadoop --disabled-password --gecos ""
sudo usermod -aG sudo hadoop
su - hadoop

wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 ~/hadoop

echo 'export HADOOP_HOME=$HOME/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

Configure **HDFS** to use `hdfs://master:9000` (adjust the hostname if needed):

`$HADOOP_HOME/etc/hadoop/core-site.xml`
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9000</value>
  </property>
</configuration>
```

`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/hadoop/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/hadoop/hdfs/datanode</value>
  </property>
</configuration>
```

Format NameNode and start HDFS:
```bash
hdfs namenode -format
start-dfs.sh
# (Optional) Web UI: NameNode http://master:9870
```

Create HDFS folders and upload data:
```bash
hdfs dfs -mkdir -p /home/user/csv_files
hdfs dfs -put warc.csv wat.csv wet.csv employees.csv departments.csv /home/user/csv_files
hdfs dfs -ls /home/user/csv_files
```

### 3) Enable YARN & Job History
`$HADOOP_HOME/etc/hadoop/mapred-site.xml`
```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>master:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>master:19888</value>
  </property>
</configuration>
```

`$HADOOP_HOME/etc/hadoop/yarn-site.xml`
```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
```

Start YARN & Job History:
```bash
start-yarn.sh
mr-jobhistory-daemon.sh start historyserver
# Web UIs: ResourceManager http://master:8088  |  JobHistory http://master:19888
```

### 4) Install Spark (3.5.x)
```bash
wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
mv spark-3.5.1-bin-hadoop3 ~/spark

echo 'export SPARK_HOME=$HOME/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

(Optional) Spark defaults (YARN mode):
`$SPARK_HOME/conf/spark-defaults.conf` (create if missing)
```properties
spark.master                     yarn
spark.driver.memory              2g
spark.executor.memory            2g
spark.ui.port                    4040
spark.sql.shuffle.partitions     8
```

### 5) Python dependencies
```bash
pip install -r requirements.txt
```

### 6) Run examples
Run conversions (CSV → Parquet) and queries:
```bash
spark-submit data_ingestion/warc_parquet.py
spark-submit queries/df_q1.py
spark-submit queries/df_csv_q1.py
spark-submit queries/rdd_q1.py
```

> The **HDFS screenshots** in the README come from **Hadoop HDFS** web UI, and the **execution screenshots** come from the **Hadoop Job History UI**.
