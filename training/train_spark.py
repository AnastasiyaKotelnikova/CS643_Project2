#!/usr/bin/env python3
"""
CS643 Project 2 - Parallel Training Script (Spark / PySpark)

Updated version with:
✔ Local dataset support
✔ CSV loading with schema inference
✔ Data preview (rows + columns)
✔ Placeholder for S3 path switch later
"""

import argparse
from pyspark.sql import SparkSession

# -----------------------------
# Argument parsing
# -----------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="CS643 Project 2 - Spark training job (template)"
    )
    parser.add_argument(
        "--input",
        required=False,
        default="data/TrainingDataset.csv",   # 👈 Local dataset path (new)
        help="Input data path (local CSV or s3:// path).",
    )
    parser.add_argument(
        "--output",
        required=False,
        default="output/model_metrics/",       # 👈 Local output for now
        help="Output path for model or metrics.",
    )
    parser.add_argument(
        "--num-epochs",
        type=int,
        default=5,
        help="Number of training epochs (placeholder).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )
    args = parser.parse_args()
    return args

# -----------------------------
# Spark session
# -----------------------------
def create_spark(app_name: str = "CS643_Project2_Training") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark

# -----------------------------
# Data loading (updated)
# -----------------------------
def load_data(spark: SparkSession, input_path: str):
    """
    🚀 Updated: Loads LOCAL CSV file first.
    Will switch to S3 path once uploaded to bucket.
    """

    print(f"[INFO] Loading data from: {input_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    print(f"[INFO] Dataframe loaded: {df.count()} rows, {len(df.columns)} columns.")
    print("[INFO] Feature columns preview:")
    df.printSchema()  # 👈 Shows column types

    return df

# -----------------------------
# Placeholder Model Training
# -----------------------------
def build_and_train_model(df, num_epochs: int, seed: int):
    print(f"[INFO] Starting dummy training for {num_epochs} epoch(s).")
    row_count = df.count()

    model = {
        "dummy_model": True,
        "num_epochs": num_epochs,
        "seed": seed,
        "row_count_used_for_training": row_count,
    }

    print(f"[INFO] Training placeholder completed. Rows used: {row_count}")
    return model

# -----------------------------
# Save model / metrics
# -----------------------------
def save_model_or_metrics(model, output_path: str):
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession for saving output.")

    rows = [model]
    df_out = spark.createDataFrame(rows)

    print(f"[INFO] Saving placeholder results to: {output_path}")
    (
        df_out
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(output_path)
    )

    print("[INFO] Save completed.")

# -----------------------------
# Main function
# -----------------------------
def main():
    args = parse_args()

    print("[INFO] Starting Spark training job.")
    print(f"[INFO] Arguments: {args}")

    spark = create_spark()

    try:
        df = load_data(spark, args.input)
        model = build_and_train_model(df, args.num_epochs, args.seed)
        save_model_or_metrics(model, args.output)
        print("[INFO] Training job completed successfully.")
    finally:
        print("[INFO] Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
