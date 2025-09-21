from pyspark.sql import SparkSession
from pyspark import StorageLevel

def main():
    spark = SparkSession.builder.appName("RDD Repartition Join").getOrCreate()
    sc = spark.sparkContext

    # Φόρτωση CSV από HDFS
    employees_rdd = sc.textFile("hdfs://master:9000/home/user/csv_files/employees.csv")
    departments_rdd = sc.textFile("hdfs://master:9000/home/user/csv_files/departments.csv")

    # Διαμόρφωση (key,value) με key = dep_id
    # employees: (dep_id, (emp_id, emp_name))
    emp_kv = (
        employees_rdd
        .filter(lambda line: line.strip() != "")
        .map(lambda line: line.split(","))
        .filter(lambda x: len(x) >= 3 and x[0] and x[1] and x[2])
        .map(lambda x: (int(x[2]), (int(x[0]), x[1])))
    )

    # departments: (dep_id, dep_name)
    dep_kv = (
        departments_rdd
        .filter(lambda line: line.strip() != "")
        .map(lambda line: line.split(","))
        .filter(lambda x: len(x) >= 2 and x[0] and x[1])
        .map(lambda x: (int(x[0]), x[1]))
    )

    # Προαιρετικό: επιλέγουμε αριθμό partitions (μπορείς να τον προσαρμόσεις)
    num_partitions = 8
    emp_part = emp_kv.partitionBy(num_partitions).persist(StorageLevel.MEMORY_ONLY)
    dep_part = dep_kv.partitionBy(num_partitions).persist(StorageLevel.MEMORY_ONLY)

    # cogroup: key -> (Iterable[employees], Iterable[departments])
    grouped = emp_part.cogroup(dep_part)

    # Συνδυασμός για κάθε dep_id
    # αποτέλεσμα: (dep_id, emp_id, emp_name, dep_name)
    joined = grouped.flatMap(
        lambda kv: (
            (kv[0], e[0], e[1], d)           # dep_id, emp_id, emp_name, dep_name
            for e in kv[1][0]                # employees iterable
            for d in kv[1][1]                # departments iterable (συνήθως 1 στοιχείο)
        )
    )

    # Εμφάνιση πρώτων Ν
    for row in joined.take(100):
        print(f"(dep_id={row[0]}, emp_id={row[1]}, emp_name={row[2]}, dep_name={row[3]})")

    spark.stop()