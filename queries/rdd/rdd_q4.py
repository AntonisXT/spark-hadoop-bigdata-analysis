# Εισαγωγή του SparkContext και των κατάλληλων βιβλιοθηκών
from pyspark.sql import SparkSession

sc = SparkSession \
    .builder \
    .appName("RDD query 4 execution") \
    .getOrCreate() \
    .sparkContext

# Φόρτωση των αρχείων csv σε RDDs
warc_rdd = sc.textFile("hdfs://master:9000/home/user/csv_files/warc.csv")
wat_rdd = sc.textFile("hdfs://master:9000/home/user/csv_files/wat.csv")

# Διαχωρισμός των δεδομένων σε πεδία και έλεγχος των μηδενικών τιμών
warc_rdd = warc_rdd.map(lambda line: line.split(",")).filter(lambda x: x[3] and x[6])
wat_rdd = wat_rdd.map(lambda line: line.split(",")).filter(lambda x: x[1])

# Δημιουργία tuple (warc_record_id, (content_length, server))
warc_mapped = warc_rdd.map(lambda x: (x[1], (int(x[3]), x[6])))

# Δημιουργία tuple (warc_record_id, content_length_metadata)
wat_mapped = wat_rdd.map(lambda x: (x[0], int(x[1])))

# Join των RDDs με βάση το warc_record_id
joined_rdd = warc_mapped.join(wat_mapped)

# Δημιουργία tuple (server, (content_length_warc, content_length_metadata))
server_content_lengths = joined_rdd.map(lambda x: (x[1][0][1], (x[1][0][0], x[1][1])))

# Υπολογισμός του μέσου μήκους περιεχομένου για κάθε διακομιστή
server_avg_lengths = server_content_lengths \
    .mapValues(lambda x: (x[0], x[1], 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
    .mapValues(lambda x: (x[0] / x[2], x[1] / x[2]))

# Ταξινόμηση με βάση το μέσο μήκος περιεχομένου και λήψη των κορυφαίων 5
top_5_servers = server_avg_lengths \
    .takeOrdered(5, key=lambda x: -(x[1][0]))

# Εμφάνιση αποτελεσμάτων
for server, lengths in top_5_servers:
    print()
    print(f"Server: {server}, Avg Warc Content Length: {lengths[0]}, Avg Metadata Content Length: {lengths[1]}")

sc.stop()
