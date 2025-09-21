from pyspark.sql import SparkSession

sc = SparkSession \
    .builder \
    .appName("RDD query 3 execution") \
    .getOrCreate() \
    .sparkContext

# Διάβασμα του αρχείου CSV και δημιουργία ενός RDD
q3 = sc.textFile("hdfs://master:9000/home/user/csv_files/warc.csv")

# Φιλτράρισμα εγγραφών με μηδενικό μήκος περιεχομένου
q3_filtered = q3.filter(lambda line: line.split(',')[3] != '0')

# Φιλτράρισμα εγγραφών με Apache ως διακομιστή
apache_lines = q3_filtered.filter(lambda line: "Apache" in line.split(',')[6])

# Κάθε γραμμή μετατρέπεται σε ένα ζευγάρι (record id,target url, length)
content_lengths = apache_lines.map(lambda line: (line.split(',')[1],line.split(',')[5], int(line.split(',')[3])))

# Κρατάμε τις 5 εγγραφές με το μεγαλύτερο content length
top_5 = content_lengths.takeOrdered(5, key=lambda x: -x[2])

top_5.reverse()

# Επιστροφή των συνδυασμών των warc record ids/target urls με το μεγαλύτερο μήκος περιεχομένου σε αύξουσα σειρά
for record_id, target_url, length in top_5:
    print("Warc Record ID:", record_id, " - Target URL:", target_url, " - Content Length:", length)
    print()


# Κλείσιμο του SparkContext
sc.stop()

