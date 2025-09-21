from pyspark.sql import SparkSession

sc = SparkSession \
    .builder \
    .appName("RDD query 2 execution") \
    .getOrCreate() \
    .sparkContext

# Διαβάζουμε το αρχείο CSV απο το hdfs σε ένα RDD
warc_rdd  = sc.textFile("hdfs://master:9000/home/user/csv_files/warc.csv")
wat_rdd  = sc.textFile("hdfs://master:9000/home/user/csv_files/wat.csv")

# Χωρίζει την κάθε γραμμή βάσει του delimiter ","
warc_split = warc_rdd.map(lambda line: line.split(","))
# Δημιουργεί tuple (record id, metadata lenght)
wat_split = wat_rdd.map(lambda line: (line.split(",")[0],line.split(",")[1]))

# Επιλογή μόνο των γραμμών με το target URL και warc type 'request'
warc_filtered = warc_split.filter(lambda x: 'http://1001.ru/articles/post/ai-da-tumin-443' in x[5] and 'request' in x[2])

# Δημιουργεί tuple (record id, (target url, hdom))
warc_info = warc_filtered.map(lambda line: (line[1],(line[5],(len(line[7])))))

print("warc_info : ", warc_info.collect())
info = warc_info.join(wat_split)
print("info join : ", info.collect())
print()
for id,result in info.collect():
        print("Target url: ", result[0][0],"Metadata Length: ",result[1],"Html size: ",result[0][1])
print()

sc.stop()
