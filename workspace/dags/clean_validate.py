"""
Final Project - Data Transformation & Validation Script

Author : Team !
Batch  : CODA-RMT-023

Description:
    Reads intermediate raw Parquet files, selects relevant features, removes
    unused columns and duplicates using PySpark, validates data quality against
    pre-configured Great Expectations checkpoints, and writes cleaned Parquet files.
"""

import os
import great_expectations as gx
from pyspark.sql import SparkSession

# Resolve directory paths (/workspace/dags and /workspace)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_DIR = os.path.dirname(BASE_DIR)
INPUT_DIR = os.path.join(BASE_DIR, "temp_data", "raw")
OUTPUT_DIR = os.path.join(BASE_DIR, "temp_data", "clean")


def cleaning_data(spark):
    """
    Clean and reshape both datasets according to project data dictionary rules.

    Args:
        spark (SparkSession): Active PySpark session.

    Returns:
        tuple[DataFrame, DataFrame]: Cleaned palm oil and deforestation DataFrames.
    """
    df_palm = spark.read.parquet(os.path.join(INPUT_DIR, "palm_oil"))
    df_def = spark.read.parquet(os.path.join(INPUT_DIR, "deforestation"))

    # 1. Whitelist 11 core analytical columns for the main palm oil dataset
    keep_columns = [
        "year",
        "province_of_production",
        "province_of_production_trase_id",
        "kabupaten_of_production",
        "product_type",
        "exporter_group",
        "zero_deforestation_indonesia_palm_oil",
        "country_of_first_import",
        "palm_oil_deforestation_annual_exposure",
        "volume",
        "total_emission_exposure",
    ]
    df_palm_clean = df_palm.dropDuplicates().select(*keep_columns)

    # 2. Drop redundant geographic columns and standardize metric column name
    drop_columns = [
        "country",
        "country_iso2",
        "parent_region",
        "parent_region_trase_id",
    ]
    df_def_clean = (
        df_def.drop(*drop_columns)
        .withColumnRenamed(
            "annual_palm_deforestation_industrial_hectares",
            "annual_palm_deforestation_hectares",
        )
        .dropDuplicates()
    )

    print(f"[TRANSFORM] palm_oil cleaned rows: {df_palm_clean.count()}")
    print(f"[TRANSFORM] deforestation cleaned rows: {df_def_clean.count()}")

    return df_palm_clean, df_def_clean


def validate_gx(df_palm_clean, df_def_clean):
    """
    Run existing Great Expectations checkpoints stored in /workspace/gx.
    Raises a ValueError to fail the Airflow task if any expectation fails.

    Args:
        df_palm_clean (DataFrame): Cleaned palm oil Spark DataFrame.
        df_def_clean (DataFrame): Cleaned deforestation Spark DataFrame.
    """
    # Load file-backed GX Data Context from the workspace root
    context = gx.get_context(mode="file", project_root_dir=WORKSPACE_DIR)

    # Execute validation checkpoints for both datasets
    res_palm = context.checkpoints.get("palm_oil_checkpoint").run(
        batch_parameters={"dataframe": df_palm_clean.toPandas()}
    )
    res_def = context.checkpoints.get("deforestation_checkpoint").run(
        batch_parameters={"dataframe": df_def_clean.toPandas()}
    )

    # Halt pipeline execution if either validation suite fails
    if not res_palm.success or not res_def.success:
        raise ValueError(
            "Great Expectations validation failed! Inspect Data Docs in /workspace/gx."
        )

    print("[VALIDATE] All Great Expectations checkpoints passed successfully!")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("palm-oil-clean").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df_palm_clean, df_def_clean = cleaning_data(spark)
    validate_gx(df_palm_clean, df_def_clean)

    # Save validated datasets as cleaned Parquet files for the load stage
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_palm_clean.write.mode("overwrite").parquet(
        os.path.join(OUTPUT_DIR, "palm_oil")
    )
    df_def_clean.write.mode("overwrite").parquet(
        os.path.join(OUTPUT_DIR, "deforestation")
    )
    print(f"[CLEANING] Cleaned Parquet files saved to: {OUTPUT_DIR}")

    spark.stop()