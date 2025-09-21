from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

# Δημιουργία Spark Session
spark = SparkSession \
    .builder \
    .appName("DF_Parquet Query 1 Execution") \
    .getOrCreate()

warc_df = spark.read.parquet("hdfs://master:9000/home/user/parquet_files/warc.parquet")

# Δημιουργία προσωρινού πίνακα
warc_df.createOrReplaceTempView("warc")

# Φιλτράρισμα εγγραφών
filtered_warc_df = spark.sql("""
    SELECT *
    FROM warc
    WHERE date >= '2017-03-22T22:00:00Z'
    AND date <= '2017-03-22T23:00:00Z'
    AND server IS NOT NULL """)

filtered_warc_df.createOrReplaceTempView("filtered_warc")

# Ομαδοποίηση κατά διακομιστή (server) και αποθήκευση των 5 που χρησιμοποιούνται περισσότερο
server_usage_df = spark.sql("""
    SELECT server, COUNT(*) as usage_count
    FROM filtered_warc
    GROUP BY server
    ORDER BY usage_count DESC
    LIMIT 5 """)
server_usage_df.show()
