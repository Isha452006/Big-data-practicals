import os
from pyspark import SparkContext
import tempfile

# Java & Python environment
os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk17.0.17_10\jdk17.0.17_10"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + r"C:\hadoop\bin;" + os.environ["PATH"]
os.environ["SPARK_LOCAL_DIRS"] = tempfile.mkdtemp()

# Initialize Spark
sc = SparkContext("local", "PySpark RDD Example")

# RDD Operations
data = [1,2,3,4,5,6,7,8,9]
rdd = sc.parallelize(data)
print("RDD Elements:", rdd.collect())
mapped_rdd = rdd.map(lambda x: x*2)
print("Mapped RDD:", mapped_rdd.collect())
filtered_rdd = mapped_rdd.filter(lambda x: x%4==0)
print("Filtered RDD:", filtered_rdd.collect())

pair_rdd = sc.parallelize([("apple",2),("banana",3),("apple",4),("orange",5)])
reduced_rdd = pair_rdd.reduceByKey(lambda a,b: a+b)
print("Reduced RDD:", reduced_rdd.collect())

sc.stop()
