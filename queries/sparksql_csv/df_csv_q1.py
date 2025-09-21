from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("DF-CSV-Q1").getOrCreate()

warc_schema = StructType([
    StructField("date",   StringType()),
    StructField("id",     StringType()),
    StructField("type",   StringType()),
    StructField("length", IntegerType()),
    StructField("ip",     StringType()),
    StructField("url",    StringType()),
    StructField("server", StringType()),
    StructField("html",   StringType()),
])

warc = (spark.read.format("csv")
    .option("header", "false").option("escapeQuotes", "true").option("escape", "\"")
    .schema(warc_schema)
    .load("hdfs://master:9000/home/user/csv_files/warc.csv"))

warc.createOrReplaceTempView("warc_raw")

# Παράγουμε timestamp για φίλτρο
spark.sql("""
    CREATE OR REPLACE TEMP VIEW warc AS
    SELECT
      to_timestamp(date, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS ts,
      server
    FROM warc_raw
    WHERE date IS NOT NULL AND server IS NOT NULL
""")

# 2017-03-22 22:00 — 23:00 (UTC όπως στο πεδίο)
res = spark.sql("""
    SELECT server, COUNT(*) AS uses
    FROM warc
    WHERE ts >= timestamp('2017-03-22 22:00:00') AND ts < timestamp('2017-03-22 23:00:00')
    GROUP BY server
    ORDER BY uses DESC
    LIMIT 5
""")

res.show(truncate=False)
spark.stop()
