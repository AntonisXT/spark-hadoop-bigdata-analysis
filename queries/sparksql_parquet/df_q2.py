from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

# Δημιουργία του SparkSession
spark = SparkSession \
    .builder \
    .appName("DF_Parquet Query 2 Execution") \
    .getOrCreate()

# Διάβασμα των parquet αρχείων απο το hdfs
warc_df = spark.read.parquet('hdfs://master:9000/home/user/parquet_files/warc.parquet')
wat_df = spark.read.parquet('hdfs://master:9000/home/user/parquet_files/wat.parquet')


# Καθαρίζουμε την cache
spark.catalog.clearCache()

# Δημιουργούμε προσωρινές πίνακες
warc_df.createOrReplaceTempView("warc")
wat_df.createOrReplaceTempView("wat")

# Φιλτράρουμε τα δεδομένα του warc για τη συγκεκριμένη διεύθυνση URL και το warc type ως request
warc_query = """ SELECT id, html
        FROM warc
        WHERE url = 'http://1001.ru/articles/post/ai-da-tumin-443'
        AND type = 'request' """

warc_result = spark.sql(warc_query)
warc_result.createOrReplaceTempView("filtered_warc")

# Επιλέγουμε το id, content length των metadata από το wat και δημιουργόυμε τον νέο πίνακα 
wat_query = """ SELECT id, length AS metadata_length
        FROM wat """
wat_result = spark.sql(wat_query)
wat_result.createOrReplaceTempView("filtered_wat")

# Ενώνουμε τα αποτελέσματα των δύο queries
final_query = """ SELECT w.id, len(w.html), t.metadata_length
FROM filtered_warc w
JOIN filtered_wat t ON w.id == t.id """
final_result = spark.sql(final_query)
final_result.show()

# Τερματίζουμε το SparkSession
spark.stop()
