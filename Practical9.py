# ✅ Import Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ✅ Create Spark Session (no Hadoop required)
spark = SparkSession.builder \
    .appName("Big Data Practical 9 - Simple Mode") \
    .master("local[*]") \
    .getOrCreate()

# ✅ Create simple DataFrame
data = [
    (1, "Alice", "HR", 40000),
    (2, "Bob", "IT", 50000),
    (3, "Charlie", "IT", 55000),
    (4, "David", "Finance", 45000),
    (5, "Eve", "HR", 48000),
    (6, "Frank", "Finance", 52000)
]
columns = ["Emp_ID", "Name", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

print("\n✅ Original DataFrame:")
df.show()

# ✅ Run SQL Query directly (no Parquet needed)
df.createOrReplaceTempView("employees")

high_salary = spark.sql("""
SELECT Name, Department, Salary
FROM employees
WHERE Salary > 48000
""")
print("\n✅ Employees with Salary > 48000:")
high_salary.show()

# ✅ Window Function Example
windowSpec = Window.partitionBy("Department").orderBy(F.desc("Salary"))
ranked_df = df.withColumn("Rank", F.rank().over(windowSpec))

print("\n✅ Ranking employees by salary within each department:")
ranked_df.show()

