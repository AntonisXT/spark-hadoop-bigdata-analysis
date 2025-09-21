from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()

departments_schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
])

departments_df = spark.read.format('csv') \
        .options(header='false') \
        .option("escapeQuotes", "true") \
        .option('escape', "\"") \
        .schema(departments_schema) \
        .load("hdfs://master:9000/home/user/csv_files/departments.csv")

departments_df.write.parquet("hdfs://master:9000/home/user/parquet_files/departments.parquet")
spark.stop()
