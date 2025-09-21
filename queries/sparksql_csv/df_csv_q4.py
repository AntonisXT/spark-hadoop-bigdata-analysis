from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("DF-CSV-Q4").getOrCreate()

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
wat_schema = StructType([
    StructField("id",     StringType()),
    StructField("length", IntegerType()),
    StructField("url",    StringType()),
])

warc = (spark.read.format("csv")
    .option("header", "false").option("escapeQuotes", "true").option("escape", "\"")
    .schema(warc_schema)
    .load("hdfs://master:9000/home/user/csv_files/warc.csv"))
wat = (spark.read.format("csv")
    .option("header", "false").option("escapeQuotes", "true").option("escape", "\"")
    .schema(wat_schema)
    .load("hdfs://master:9000/home/user/csv_files/wat.csv"))

warc.createOrReplaceTempView("warc")
wat.createOrReplaceTempView("wat")

res = spark.sql("""
    SELECT
      w.server,
      AVG(w.length) AS avg_warc_length,
      AVG(t.length) AS avg_wat_length
    FROM warc w
    JOIN wat t ON w.id = t.id
    WHERE w.server IS NOT NULL AND w.length IS NOT NULL AND t.length IS NOT NULL
    GROUP BY w.server
    ORDER BY avg_warc_length DESC
    LIMIT 5
""")

res.show(truncate=False)
spark.stop()
