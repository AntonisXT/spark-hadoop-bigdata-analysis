from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("RDD Broadcast Join").getOrCreate()
    sc = spark.sparkContext

    # Φόρτωση CSV από HDFS
    employees_rdd = sc.textFile("hdfs://master:9000/home/user/csv_files/employees.csv")
    departments_rdd = sc.textFile("hdfs://master:9000/home/user/csv_files/departments.csv")

    # Καθαρισμός/διάσπαση
    # employees: id,name,dep_id
    employees_kv = (
        employees_rdd
        .filter(lambda line: line.strip() != "")
        .map(lambda line: line.split(","))
        .filter(lambda x: len(x) >= 3 and x[0] and x[1] and x[2])
        .map(lambda x: (int(x[2]), (int(x[0]), x[1])))   # key: dep_id
    )

    # departments: id,name
    departments_map = (
        departments_rdd
        .filter(lambda line: line.strip() != "")
        .map(lambda line: line.split(","))
        .filter(lambda x: len(x) >= 2 and x[0] and x[1])
        .map(lambda x: (int(x[0]), x[1]))                # (dep_id, dep_name)
        .collectAsMap()
    )

    # Broadcast του μικρού πίνακα
    bc_depts = sc.broadcast(departments_map)

    # Join μέσω lookup στο broadcast dictionary
    # αποτέλεσμα: (dep_id, employee_id, employee_name, department_name)
    joined = (
        employees_kv
        .map(lambda kv: (
            kv[0],                        # dep_id
            kv[1][0],                     # emp_id
            kv[1][1],                     # emp_name
            bc_depts.value.get(kv[0])     # dep_name (None αν δεν υπάρχει)
        ))
        .filter(lambda t: t[3] is not None)  # κράτα μόνο όσα υπάρχουν στο departments
    )

    # Εμφάνιση πρώτων Ν
    for row in joined.take(100):
        print(f"(dep_id={row[0]}, emp_id={row[1]}, emp_name={row[2]}, dep_name={row[3]})")

    spark.stop()