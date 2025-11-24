#!/usr/bin/env python3
"""
CS643 Project 2 - Parallel Training Script (Spark / PySpark)

This is a starter template. It:
- creates a SparkSession
- parses command-line arguments
- shows where to load data from S3
- shows where to build / train / save a model

TODOs are clearly marked and will be filled once the dataset is confirmed.
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

    # NOTE: We’ll adjust these once we know the real dataset paths.
    parser.add_argument(
        "--input",
        required=True,
        help="Input data path (e.g., s3://my-bucket/path/to/data/ or HDFS path).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path for model or metrics (e.g., s3://my-bucket/output/).",
    )
    parser.add_argument(
        "--num-epochs",
        type=int,
        default=5,
        help="Number of training epochs (placeholder for now).",
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
# Spark session helper
# -----------------------------
def create_spark(app_name: str = "CS643_Project2_Training") -> SparkSession:
    """
    Create and return a SparkSession.

    On EMR this will automatically connect to the cluster’s master node.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


# -----------------------------
# Data loading (placeholder)
# -----------------------------
def load_data(spark: SparkSession, input_path: str):
    """
    TODO: Replace with real data loading logic once dataset is known.

    For now this just tries to read a generic CSV (header + inferSchema).
    If the final dataset is JSON / Parquet / something else, we will change
    this function only.
    """
    print(f"[INFO] Loading data from: {input_path}")

    # EXAMPLE for CSV – safe placeholder:
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    print(f"[INFO] Loaded dataframe with {df.count()} rows and {len(df.columns)} columns.")
    return df


# -----------------------------
# Model building / training (placeholder)
# -----------------------------
def build_and_train_model(df, num_epochs: int, seed: int):
    """
    TODO: Implement real Spark ML pipeline here once we know:
          - target column
          - feature columns
          - task type (classification / regression)

    For now this function just returns a dummy 'model' object
    (you can think of it as a placeholder dictionary).
    """
    print(f"[INFO] Starting training for {num_epochs} epoch(s) with seed={seed}.")

    # Placeholder: compute simple stats as a stand-in for training.
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
# Save model / metrics (placeholder)
# -----------------------------
def save_model_or_metrics(model, output_path: str):
    """
    TODO: When we have a real Spark ML model, we’ll call model.write().save(output_path)
          or save metrics as JSON/CSV to S3.

    For now we just write a small text file with basic info so we can verify
    the EMR job worked end-to-end.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found when trying to save output.")

    # Convert the dictionary to a single-row DataFrame and write it out.
    rows = [model]
    df_out = spark.createDataFrame(rows)

    print(f"[INFO] Writing placeholder metrics to: {output_path}")
    (
        df_out
        .coalesce(1)  # single output file (small metrics)
        .write
        .mode("overwrite")
        .json(output_path)
    )

    print("[INFO] Save completed.")


# -----------------------------
# Main entry point
# -----------------------------
def main():
    args = parse_args()

    print("[INFO] Starting Spark training job (template).")
    print(f"[INFO] Arguments: {args}")

    spark = create_spark()

    try:
        df = load_data(spark, args.input)
        model = build_and_train_model(df, args.num_epochs, args.seed)
        save_model_or_metrics(model, args.output)
        print("[INFO] Training job finished successfully.")
    finally:
        print("[INFO] Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
