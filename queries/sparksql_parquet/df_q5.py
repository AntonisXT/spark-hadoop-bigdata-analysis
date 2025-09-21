import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.types import ArrayType, StringType

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("DF_Parquet Query 5 Execution") \
    .getOrCreate()

# Διαβάζουμε το αρχείο Parquet από το HDFS
df = spark.read.parquet("hdfs://master:9000/home/user/parquet_files/warc.parquet")

# Αφαίρεση των γραμμών με null ή κενές τιμές στις στήλες 'html' και 'url'
df = df.filter((col("html").isNotNull()) & ((col("html")) != "") &
               (col("url").isNotNull()) & ((col("url")) != ""))

# Δημιουργία προσωρινού πίνακα
df.createOrReplaceTempView("warc_table")

# Δημιουργία συνάρτησης για εξαγωγή URLs και απλοποίηση
def extract_urls(html_content):
    urls = re.findall(r'https?://[^\s<>"]+|www\.[^\s<>"]+', html_content)
    simplified_urls = [re.sub(r'(https?://[^/]+).*', r'\1', url) for url in urls]
    return simplified_urls

# Εφαρμογή της συνάρτησης
spark.udf.register("extract_urls", extract_urls, ArrayType(StringType()))


# query για εξαγωγή URLs και απλοποίηση
exploded_df = spark.sql("""
    SELECT url, explode(extract_urls(html)) as simplified_url
    FROM warc_table
""")

exploded_df.createOrReplaceTempView("exploded_warc_table")

# Εκτέλεση query για εύρεση της πιο δημοφιλούς διεύθυνσης target URL
query = spark.sql("""
    SELECT simplified_url as url, COUNT(*) as url_count
    FROM exploded_warc_table
    GROUP BY simplified_url
    ORDER BY url_count DESC
    LIMIT 1
""")
query.show()

spark.catalog.clearCache()

# Κλείσιμο του SparkSession
spark.stop()
