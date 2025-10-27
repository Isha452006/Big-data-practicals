# Practical 8: PySpark DataFrames and Data Analysis

# Step 1: Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count

# Step 2: Create Spark Session
spark = SparkSession.builder \
    .appName("PySpark DataFrame Analysis") \
    .getOrCreate()

# Step 3: Load Dataset (CSV)
# Make sure students.csv is in the same folder as this script
df = spark.read.csv("C:/Users/HP/OneDrive/Desktop/Big data prac/students.csv", header=True, inferSchema=True)

print("✅ Original DataFrame:")
df.show()

# Step 4: Perform Transformations
print("✅ Selected Columns (Name and Score):")
df.select("name", "score").show()

print("✅ Filtered Rows (Score > 50):")
df.filter(col("score") > 50).show()

print("✅ Group By Score Range and Aggregate:")

# Creating a new column for score category
from pyspark.sql.functions import when

df = df.withColumn(
    "Category",
    when(col("score") >= 60, "Excellent")
    .when((col("score") >= 40) & (col("score") < 60), "Average")
    .otherwise("Poor")
)

df.groupBy("Category").agg(
    avg("score").alias("Avg_Score"),
    max("score").alias("Max_Score"),
    min("score").alias("Min_Score"),
    count("id").alias("Total_Students")
).show()

# Step 5: Register Temporary View and Query with Spark SQL
df.createOrReplaceTempView("students")

print("✅ SQL Query: Students scoring above 50")
sql_result = spark.sql("SELECT id, name, score FROM students WHERE score > 50 ORDER BY score DESC")
sql_result.show()

# Step 6: Stop Spark Session
spark.stop()
