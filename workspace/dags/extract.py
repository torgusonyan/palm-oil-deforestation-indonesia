"""
Final Project - Data Extraction Script

Author : Team #1
Batch  : CODA-RMT-023

Description:
    Ingests raw palm oil supply chain and annual deforestation CSV datasets from the dags directory using PySpark, filters records strictly for
    the Kalimantan region, standardizes column names, and writes intermediate outputs as Parquet files for downstream transformation.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Resolve paths dynamically relative to the current script directory (/workspace/dags)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "temp_data", "raw")

# Input raw CSV file paths
RAW_FILES = {
    "palm_oil": os.path.join(BASE_DIR, "palm_oil_data_raw.csv"),
    "deforestation": os.path.join(BASE_DIR, "palm_oil_deforestation_annual_province_raw.csv"),
    }


def clean_column_names(df):
    """
    Standardize column names to snake_case and remove special characters (spaces, hyphens, slashes, and parentheses) to ensure Parquet compatibility.
    """
    new_names = [
        c.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        for c in df.columns
    ]
    return df.toDF(*new_names)


def extract(spark):
    """
    Read raw CSV datasets into Spark DataFrames and filter for Kalimantan provinces.

    Args:
        spark (SparkSession): Active PySpark session.

    Returns:
        dict[str, DataFrame]: Dictionary containing filtered Spark DataFrames.
    """
    # Filter main supply chain dataset where production province is in Kalimantan
    df_palm_oil = spark.read.csv(
        RAW_FILES["palm_oil"], header=True, inferSchema=True).filter(F.col("province_of_production").contains("KALIMANTAN"))

    # Filter supporting deforestation dataset where parent region is Kalimantan
    df_deforestation = spark.read.csv(
        RAW_FILES["deforestation"], header=True, inferSchema=True).filter(F.col("parent_region") == "KALIMANTAN")

    data = {
        "palm_oil": df_palm_oil,
        "deforestation": df_deforestation,
    }

    # Log extracted row counts for monitoring
    for name, df in data.items():
        print(f"[EXTRACT] {name} extracted rows: {df.count()}")

    return data


def save(data):
    """
    Persist extracted Spark DataFrames into intermediate Parquet files.

    Args:
        data (dict[str, DataFrame]): Dictionary of extracted Spark DataFrames.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for name, df in data.items():
        out_path = os.path.join(OUTPUT_DIR, name)
        clean_column_names(df).write.mode("overwrite").parquet(out_path)
        print(f"[EXTRACT] Saved Parquet to: {out_path}")


if __name__ == "__main__":
    # Initialize SparkSession and suppress verbose INFO logs
    spark = SparkSession.builder.appName("palm-oil-extract").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    extracted_data = extract(spark)
    save(extracted_data)

    # Release Spark driver resources and free port 4040
    spark.stop()
