#!/bin/bash

# Σταματάει την εκτέλεση του script αν υπάρξει κάποιο σφάλμα (fail-fast)
set -e

echo "🚀 Starting Big Data Pipeline Execution..."

echo "======================================"
echo "📦 STEP 1: Data Ingestion (CSV to Parquet)"
echo "======================================"
spark-submit data_ingestion/departments_parquet.py
spark-submit data_ingestion/employees_parquet.py
spark-submit data_ingestion/warc_parquet.py
spark-submit data_ingestion/wat_parquet.py
spark-submit data_ingestion/wet_parquet.py

echo "======================================"
echo "🔎 STEP 2: Executing RDD Queries"
echo "======================================"
for i in {1..5}; do
    echo "Running RDD Query $i..."
    spark-submit queries/rdd/rdd_q${i}.py
done

echo "======================================"
echo "📊 STEP 3: Executing SparkSQL Queries on CSV"
echo "======================================"
for i in {1..5}; do
    echo "Running SparkSQL CSV Query $i..."
    spark-submit queries/sparksql_csv/df_csv_q${i}.py
done

echo "======================================"
echo "⚡ STEP 4: Executing SparkSQL Queries on Parquet"
echo "======================================"
for i in {1..5}; do
    echo "Running SparkSQL Parquet Query $i..."
    spark-submit queries/sparksql_parquet/df_q${i}.py
done

echo "======================================"
echo "🔗 STEP 5: Executing Join Strategies & Catalyst Optimizer"
echo "======================================"
echo "Running Broadcast Join (RDD)..."
spark-submit joins/joins_broadcast_rdd.py

echo "Running Repartition Join (RDD)..."
spark-submit joins/joins_repartition_rdd.py

echo "Running Catalyst Optimizer test (Disabled/SortMergeJoin)..."
spark-submit joins/join_catalyst_on_off.py Y

echo "Running Catalyst Optimizer test (Enabled/BroadcastHashJoin)..."
spark-submit joins/join_catalyst_on_off.py N

echo "======================================"
echo "📈 STEP 6: Generating Visualizations"
echo "======================================"
# Τρέχουμε τα python scripts για τα γραφήματα. Απαιτείται η ύπαρξη του 'images' folder.
python3 visualizations/catalyst_time.py
python3 visualizations/times.py

echo "======================================"
echo "✅ Pipeline Execution Completed Successfully!"
echo "======================================"
