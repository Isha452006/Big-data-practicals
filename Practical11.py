from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
spark = SparkSession.builder.appName("Model_Training_No_CSV").getOrCreate()
data = [
    ("Alice", "Delhi", 25, 40000, 0),
    ("Bob", "Mumbai", 30, 50000, 1),
    ("Cathy", "Delhi", 27, 42000, 0),
    ("David", "Chennai", 35, 60000, 1),
    ("Eva", "Mumbai", 28, 48000, 0),
    ("Frank", "Delhi", 40, 70000, 1),]
columns = ["Name", "City", "Age", "Salary", "Purchased"]
df = spark.createDataFrame(data, columns)
df.show()
label_col_regression = "Salary"
label_col_classification = "Purchased"
categorical_cols = ["Name", "City"]
numeric_cols = ["Age"]
indexers = [StringIndexer(inputCol=c, outputCol=c + "_index", handleInvalid="keep") for c in categorical_cols]
encoders = [OneHotEncoder(inputCols=[c + "_index"], outputCols=[c + "_vec"]) for c in categorical_cols]
encoded_features = [c + "_vec" for c in categorical_cols]
feature_cols = encoded_features + numeric_cols
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
pipeline = Pipeline(stages=indexers + encoders + [assembler])
processed_data = pipeline.fit(df).transform(df)
print("\n========== Linear Regression ==========")
final_data_lr = processed_data.select("features", label_col_regression)
train_data, test_data = final_data_lr.randomSplit([0.8, 0.2], seed=42)
lr = LinearRegression(featuresCol="features", labelCol=label_col_regression)
lr_model = lr.fit(train_data)
predictions = lr_model.transform(test_data)
evaluator = RegressionEvaluator(labelCol=label_col_regression, predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Linear Regression RMSE: {rmse:.2f}")
print("\n========== Logistic Regression ==========")
final_data_logr = processed_data.select("features", label_col_classification)
train_data, test_data = final_data_logr.randomSplit([0.8, 0.2], seed=42)
logr = LogisticRegression(featuresCol="features", labelCol=label_col_classification)
logr_model = logr.fit(train_data)
logr_predictions = logr_model.transform(test_data)
logr_predictions.show()
class_eval = MulticlassClassificationEvaluator(labelCol=label_col_classification, predictionCol="prediction")
accuracy = class_eval.evaluate(logr_predictions, {class_eval.metricName: "accuracy"})
precision = class_eval.evaluate(logr_predictions, {class_eval.metricName: "weightedPrecision"})
recall = class_eval.evaluate(logr_predictions, {class_eval.metricName: "weightedRecall"})
print(f"Accuracy: {accuracy:.2f}")
print(f"Precision: {precision:.2f}")
print(f"Recall: {recall:.2f}")
spark.stop()
