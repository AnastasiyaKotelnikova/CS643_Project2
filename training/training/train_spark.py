from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main():
    # Create Spark Session
    spark = SparkSession.builder.appName("CS643_Project2_Training").getOrCreate()

    # Placeholder dataset path (S3 or local) – will update once dataset is confirmed
    data_path = "s3://your-bucket-name/dataset.csv"  # <-- Replace later

    print(" Loading dataset...")
    data = spark.read.csv(data_path, header=True, inferSchema=True)

    print(" Preparing data...")
    feature_columns = data.columns[:-1]   # assume last column is label
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    final_data = assembler.transform(data).select("features", data.columns[-1].alias("label"))

    # Train-Test Split
    train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

    print(" Training Logistic Regression model...")
    lr = LogisticRegression()
    lr_model = lr.fit(train_data)

    print(" Evaluating model...")
    predictions = lr_model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"Model Accuracy: {accuracy}")

    # Save trained model to S3 (will update path later)
    model_output_path = "s3://your-bucket-name/model_output/"
    lr_model.save(model_output_path)

    print(" Training complete. Model saved.")
    spark.stop()

if __name__ == "__main__":
    main()
