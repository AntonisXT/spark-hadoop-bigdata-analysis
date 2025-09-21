import sys, time
from pyspark.sql import SparkSession

disabled = sys.argv[1] if len(sys.argv) == 2 else None
if disabled not in ('Y', 'N'):
    print("Usage: spark-submit catalyst_sql.py [Y|N]   (Y=disable broadcast, N=enable)")
    sys.exit(1)

spark = SparkSession.builder.appName('query1-sql').getOrCreate()

# Catalyst tweak
if disabled == 'Y':
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df = spark.read.format("parquet")
df1 = df.load("hdfs://master:9000/home/user/parquet_files/departments.parquet")
df2 = df.load("hdfs://master:9000/home/user/parquet_files/employees.parquet")

df1.registerTempTable("departments")
df2.registerTempTable("employees")

# employees(id,name,dep_id), departments(id,name)
sqlString = \
"SELECT * " + \
"FROM " + \
" (SELECT * FROM employees LIMIT 10000) as e JOIN departments as d ON e.dep_id = d.id" + \
" JOIN departments d2 ON d2.id = d.id ;"

t1 = time.time()
spark.sql(sqlString).show()
t2 = time.time()
spark.sql(sqlString).explain()
print("Time with choosing join type %s is %.4f sec." % ("disabled" if disabled=='Y' else "enabled", t2 - t1))

spark.stop()