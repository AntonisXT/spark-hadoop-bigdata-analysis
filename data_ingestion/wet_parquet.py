from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()

wet_schema = StructType([
        StructField("id", StringType()),
        StructField("plaintext", StringType()),
])

wet_df = spark.read.format('csv') \
        .options(header='false') \
        .option("escapeQuotes", "true") \
        .option('escape', "\"") \
        .schema(wet_schema) \
        .load("hdfs://master:9000/home/user/csv_files/wet.csv")

wet_df.write.parquet("hdfs://master:9000/home/user/parquet_files/wet.parquet")
spark.stop()
