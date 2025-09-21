
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()

employees_schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("dep_id", IntegerType()),
])

employees_df = spark.read.format('csv') \
        .options(header='false') \
        .option("escapeQuotes", "true") \
        .option('escape', "\"") \
        .schema(employees_schema) \
        .load("hdfs://master:9000/home/user/csv_files/employees.csv")

employees_df.write.parquet("hdfs://master:9000/home/user/parquet_files/employees.parquet")
spark.stop()
