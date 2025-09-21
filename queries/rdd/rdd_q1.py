from pyspark.sql import SparkSession
from datetime import datetime

sc = SparkSession \
    .builder \
    .appName("RDD query 1 execution") \
    .getOrCreate() \
    .sparkContext

# Φόρτωση των δεδομένων από το HDFS
warc = sc.textFile("hdfs://master:9000/home/user/csv_files/warc.csv")

# Χρονικό εύρος
start_time = datetime.strptime("2017-03-22T22:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
end_time = datetime.strptime("2017-03-22T23:00:00Z", "%Y-%m-%dT%H:%M:%SZ")

# Κρατάμε τις εγγραφές που ανήκουν στο χρονικό εύρος
q1_filter = warc.filter(lambda line: line.split(",")[0] and start_time <= datetime.strptime(line.split(",")[0], "%Y-%m-%dT%H:%M:%SZ") <= end_time)
# Ελέγχουμε για null τιμές
q1_filter = q1_filter.filter(lambda line: line.split(",")[6] and line.split(",")[6].lower() != "null")
#  Μέτρηση των εμφανίσεων των server
server_counts = q1_filter.map(lambda line: (line.split(",")[6], 1)).reduceByKey(lambda a, b: a + b)

# Ταξινόμηση των server με βάση τη συχνότητα εμφάνισης σε φθίνουσα σειρά και επιλογή των 5 πρώτων
top_servers = server_counts.sortBy(lambda x: -x[1]).take(5)

# Εμφάνιση αποτελεσμάτων
for server, count in top_servers:
    print(f"{server}: {count}")

# Σταματήστε το SparkContext
sc.stop()
