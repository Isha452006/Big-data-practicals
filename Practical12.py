# -------------------------------------------------------
# PRACTICAL 12 – PYSPARK STRUCTURED STREAMING (WINDOWS SAFE)
# -------------------------------------------------------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# -------------------------------------------------------
# WINDOWS FIX – Disable native Hadoop IO to prevent error
# -------------------------------------------------------
os.environ["HADOOP_HOME_WARN_SUPPRESS"] = "1"
os.environ["HADOOP_OPTIONAL_TOOLS"] = "hadoop-aws"
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"
os.environ["JAVA_LIBRARY_PATH"] = ""
os.environ["SPARK_LOCAL_DIRS"] = "C:\\temp"

# -------------------------------------------------------
# Create SparkSession
# -------------------------------------------------------
spark = SparkSession.builder \
    .appName("StructuredStreamingExample") \
    .config("spark.sql.streaming.checkpointLocation", "C:/tmp/checkpoint/") \
    .config("spark.hadoop.io.nativeio.native", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------
# Read streaming data from socket
# -------------------------------------------------------
# Make sure you run this in another terminal before starting:
#   nc -lk 9999
# (You can use 'ncat' on Windows or download 'Netcat for Windows')
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# -------------------------------------------------------
# Split the lines into words
# -------------------------------------------------------
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# -------------------------------------------------------
# Count occurrences of each word
# -------------------------------------------------------
wordCounts = words.groupBy("word").count()

# -------------------------------------------------------
# Output the results to console
# -------------------------------------------------------
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# -------------------------------------------------------
# Keep the query running until terminated
# -------------------------------------------------------
query.awaitTermination()
