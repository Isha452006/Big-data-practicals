# ---------------------------------------------------
# ✅ PySpark MLlib Data Preprocessing Example
# ---------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

# ✅ Create Spark Session
spark = SparkSession.builder \
    .appName("MLlib Data Preprocessing") \
    .master("local[*]") \
    .getOrCreate()

# ---------------------------------------------------
# Step 1: Load Dataset
# ---------------------------------------------------
# For demo, let's create a small sample dataset
data = [
    (1, "Male", "HR", 35000.0),
    (2, "Female", "IT", 45000.0),
    (3, "Female", "Finance", None),
    (4, None, "IT", 50000.0),
    (5, "Male", "Finance", 42000.0)
]
columns = ["ID", "Gender", "Department", "Salary"]

df = spark.createDataFrame(data, columns)

print("✅ Original DataFrame:")
df.show()

# ---------------------------------------------------
# Step 2: Clean Null Values
# ---------------------------------------------------
# Drop rows having null values
df_clean = df.na.drop()

print("✅ After Removing Null Values:")
df_clean.show()

# ---------------------------------------------------
# Step 3: Encode Categorical Columns
# ---------------------------------------------------
# Encode 'Gender' and 'Department' using StringIndexer
gender_indexer = StringIndexer(inputCol="Gender", outputCol="GenderIndex")
dept_indexer = StringIndexer(inputCol="Department", outputCol="DeptIndex")

df_encoded = gender_indexer.fit(df_clean).transform(df_clean)
df_encoded = dept_indexer.fit(df_encoded).transform(df_encoded)

print("✅ After Encoding Categorical Columns:")
df_encoded.show()

# ---------------------------------------------------
# Step 4: Assemble Features into a Single Vector
# ---------------------------------------------------
assembler = VectorAssembler(
    inputCols=["GenderIndex", "DeptIndex", "Salary"],
    outputCol="features"
)

df_assembled = assembler.transform(df_encoded)
print("✅ After Feature Assembly:")
df_assembled.select("ID", "features").show(truncate=False)

# ---------------------------------------------------
# Step 5: Feature Scaling using StandardScaler
# ---------------------------------------------------
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaledFeatures",
    withMean=True,
    withStd=True
)

scaler_model = scaler.fit(df_assembled)
df_scaled = scaler_model.transform(df_assembled)

print("✅ After Feature Scaling:")
df_scaled.select("ID", "scaledFeatures").show(truncate=False)

# ---------------------------------------------------
# Step 6: Split Dataset into Training and Testing Sets
# ---------------------------------------------------
train_df, test_df = df_scaled.randomSplit([0.8, 0.2], seed=42)

print("✅ Training Data:")
train_df.show()

print("✅ Test Data:")
test_df.show()

# ---------------------------------------------------
# Stop Spark Session
# ---------------------------------------------------
spark.stop()
print("🏁 Spark session stopped successfully.")
