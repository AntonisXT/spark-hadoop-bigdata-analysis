from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("DF-CSV-Q2").getOrCreate()

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

target = "http://1001.ru/articles/post/ai-da-tumin-443"

res = spark.sql(f"""
    WITH filtered_warc AS (
      SELECT id, html
      FROM warc
      WHERE url = '{target}' AND type = 'request' AND id IS NOT NULL AND html IS NOT NULL
    ),
    filtered_wat AS (
      SELECT id, length AS metadata_length
      FROM wat
      WHERE id IS NOT NULL AND length IS NOT NULL
    )
    SELECT w.id,
           length(w.html)          AS html_size,
           f.metadata_length       AS metadata_length
    FROM filtered_warc w
    JOIN filtered_wat f ON w.id = f.id
""")

res.show(truncate=False)
spark.stop()
