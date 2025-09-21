from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()

wat_schema = StructType([
        StructField("id", StringType()),
        StructField("length", IntegerType()),
        StructField("url", StringType()),
])

wat_df = spark.read.format('csv') \
        .options(header='false') \
        .option("escapeQuotes", "true") \
        .option('escape', "\"") \
        .schema(wat_schema) \
        .load("hdfs://master:9000/home/user/csv_files/wat.csv")

wat_df.write.parquet("hdfs://master:9000/home/user/parquet_files/wat.parquet")
spark.stop()
