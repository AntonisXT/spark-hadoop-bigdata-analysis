import re
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import udf, explode, col, array, size

spark = SparkSession.builder.appName("DF-CSV-Q5").getOrCreate()

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

# UDF: εξαγωγή URLs από HTML με regex
url_regex = re.compile(r"""(?i)\b(?:https?://)[^\s"'<>]+""")

def extract_urls(html):
    if not html:
        return []
    return url_regex.findall(html)

def simplify_url(u):
    try:
        p = urlparse(u)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}"
        return None
    except Exception:
        return None

extract_urls_udf = udf(extract_urls)
simplify_url_udf = udf(simplify_url)

warc.createOrReplaceTempView("warc_raw")

# Κρατάμε μόνο έγκυρα url & html
clean = warc.select("url", "html").where(col("url").isNotNull() & col("html").isNotNull())

# Λίστα URLs που υπάρχουν στο HTML DOM
with_urls = clean.withColumn("urls_in_html", extract_urls_udf(col("html"))).where(size(col("urls_in_html")) > 0)

# Απλοποίηση (schema+host) και καταμέτρηση συχνοτήτων
exploded = with_urls.select(col("url").alias("target_url"), explode(col("urls_in_html")).alias("ref_url"))
simplified = exploded.withColumn("ref_simple", simplify_url_udf(col("ref_url"))).where(col("ref_simple").isNotNull())

popular = (simplified
    .groupBy("ref_simple")
    .count()
    .orderBy(col("count").desc())
    .limit(1))

popular.show(truncate=False)
spark.stop()
