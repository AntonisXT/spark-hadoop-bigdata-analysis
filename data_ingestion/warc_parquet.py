from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()

warc_schema = StructType([
        StructField("date", StringType()),
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("length", IntegerType()),
        StructField("ip", StringType()),
        StructField("url", StringType()),
        StructField("server", StringType()),
        StructField("html", StringType()),
])

warc_df = spark.read.format('csv') \
        .options(header='false') \
        .option("escapeQuotes", "true") \
        .option('escape', "\"") \
        .schema(warc_schema) \
        .load("hdfs://master:9000/home/user/csv_files/warc.csv")

warc_df.write.parquet("hdfs://master:9000/home/user/parquet_files/warc.parquet")
spark.stop()
