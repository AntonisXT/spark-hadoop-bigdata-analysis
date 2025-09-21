from pyspark.sql import SparkSession

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("DF_Parquet Query 3 Execution") \
    .getOrCreate()

# Καθαρισμός της cache για αποφυγή hot caches
spark.catalog.clearCache()

# Φόρτωση των αρχείων Parquet
warc_df = spark.read.parquet("hdfs://master:9000/home/user/parquet_files/warc.parquet")

# Δημιουργία πίνακα warc
warc_df.createOrReplaceTempView("warc")

# Εύρεση των 5 εγγραφών με το μεγαλύτερο length που χρησιμοποιούν Apache
warc_top5_df = spark.sql("""
SELECT id, url, length
FROM warc
WHERE server LIKE 'Apache%' AND length IS NOT NULL
ORDER BY length DESC
LIMIT 5
""")

# Δημιουργία προσωρινού πίνακα για τα αποτελέσματα
warc_top5_df.createOrReplaceTempView("warc_top5")

# Ταξινόμηση των αποτελεσμάτων σε αύξουσα σειρά
warc_sorted_df = spark.sql("""
SELECT id, url, length
FROM warc_top5
ORDER BY length ASC
""")

# Εμφάνιση των αποτελεσμάτων
warc_sorted_df.show()
