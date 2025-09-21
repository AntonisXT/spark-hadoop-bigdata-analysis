from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("DF-CSV-Q3").getOrCreate()

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

warc.createOrReplaceTempView("warc")

# Όπως στο report: φίλτρο για Apache και ταξινόμηση κατά length DESC, LIMIT 5
res = spark.sql("""
    SELECT id, url, length
    FROM warc
    WHERE server IS NOT NULL
      AND lower(server) LIKE 'apache%%'
      AND length IS NOT NULL
    ORDER BY length DESC
    LIMIT 5
""")

res.show(truncate=False)
spark.stop()
