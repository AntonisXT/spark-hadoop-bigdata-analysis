from pyspark.sql import SparkSession

# Δημιουργία SparkSession
spark = SparkSession \
    .builder \
    .appName("DF_Parquet Query 4 Execution") \
    .getOrCreate()

# Φόρτωση των αρχείων Parquet
warc_df = spark.read.parquet("hdfs://master:9000/home/user/parquet_files/warc.parquet")
wat_df = spark.read.parquet("hdfs://master:9000/home/user/parquet_files/wat.parquet")

# Δημιουργία προσωρινών πινάκων
warc_df.createOrReplaceTempView("warc")
wat_df.createOrReplaceTempView("wat")

# Query για εύρεση του μέσου μήκους περιεχομένου των εγγραφών warc καθώς και του μέσου μήκους περιεχομένου των μεταδεδομένων τους
query = """ SELECT
    warc.server,
    AVG(warc.length) as avg_warc_length,
    AVG(wat.length) as avg_wat_length FROM
    warc JOIN
    wat ON
    warc.id = wat.id WHERE
    warc.length IS NOT NULL AND wat.length IS NOT NULL GROUP BY
    warc.server ORDER BY
    avg_warc_length DESC LIMIT 5 """

result_df = spark.sql(query)
result_df.show()
