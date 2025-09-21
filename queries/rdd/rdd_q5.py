from pyspark.sql import SparkSession
import re

sc = SparkSession \
    .builder \
    .appName("RDD query 5 execution") \
    .getOrCreate() \
    .sparkContext

# Φόρτωση αρχείων csv  από HDFS στο rdd
warc_rdd = sc.textFile("hdfs://master:9000/home/user/csv_files/warc.csv")

warc_rdd = warc_rdd.filter(lambda line: line.strip() != "")

# Χωρίζει τα δεδομένα σε πεδία
warc_rdd = warc_rdd.map(lambda line: line.split(","))

# Συνάρτηση για εξαγωγή URLs από το HTML DOM
def extract_urls(html_content):
    urls = re.findall(r'https?://[^\s<>"]+|www\.[^\s<>"]+', html_content)
    simplified_urls = [re.sub(r'(https?://[^/]+).*', r'\1', url) for url in urls]
    return simplified_urls

# Δημιουργία του RDD με το target URL και τα  URLs που περιέχονται στο HTML DOM
warc_urls_rdd = warc_rdd.map(lambda x: (x[5], extract_urls(x[7])))

# Δημιουργία ενός tuple (target_url, url_in_html_dom)
flat_warc_urls_rdd = warc_urls_rdd.flatMapValues(lambda urls: urls)

# Εύρεση του δημοφιλέστερου URL
popular_urls = flat_warc_urls_rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
most_popular_url = popular_urls.reduce(lambda a, b: a if a[1] > b[1] else b)

# Εμφάνιση αποτελέσματος
print()
print(f"Most popular URL: {most_popular_url[0]} - Occurrences: {most_popular_url[1]}")
print()

sc.stop()
