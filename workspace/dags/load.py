"""
Final Project - Data Loading & Quality Check Script (Staging)

Author : Ryan Farid Pratama
Batch  : CODA-RMT-023

Description:
    Reads validated Parquet datasets from temp_data/clean using PySpark,
    writes them into PostgreSQL staging tables (stg_palm_deforestation and
    stg_palm_oil) via JDBC overwrite mode, and verifies non-empty row counts.
"""

import os
import psycopg2
from pyspark.sql import SparkSession

# Resolve cleaned Parquet input path (/workspace/dags/temp_data/clean)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "temp_data", "clean")

env = os.environ
JDBC_URL = f"jdbc:postgresql://{env['PG_HOST']}:{env['PG_PORT']}/{env['PG_DB']}"
JDBC_PROPS = {
    "user": env["PG_USER"],
    "password": env["PG_PASSWORD"],
    "driver": "org.postgresql.Driver",
    "batchsize": "10000",
}

def load_staging(spark):
    """
    Read cleaned Parquet files and write them into PostgreSQL staging tables.

    Args:
        spark (SparkSession): Active PySpark session.
    """
    df_def = spark.read.parquet(os.path.join(INPUT_DIR, "deforestation"))
    df_palm = spark.read.parquet(os.path.join(INPUT_DIR, "palm_oil"))

    # Load supporting deforestation dataset into staging table
    df_def.write.jdbc(
        JDBC_URL,
        "stg_palm_deforestation",
        mode="overwrite",
        properties=JDBC_PROPS,
    )
    print(f"[LOAD] stg_palm_deforestation loaded: {df_def.count()} rows")

    # Load main palm oil dataset into staging table
    df_palm.write.jdbc(
        JDBC_URL,
        "stg_palm_oil",
        mode="overwrite",
        properties=JDBC_PROPS,
    )
    print(f"[LOAD] stg_palm_oil loaded: {df_palm.count()} rows")

def quality_check():
    """
    Verify that both staging tables exist and contain more than zero rows.
    Raises a ValueError to mark the Airflow task as failed if any table is empty.
    """
    conn = psycopg2.connect(
        host=env["PG_HOST"],
        port=env["PG_PORT"],
        dbname=env["PG_DB"],
        user=env["PG_USER"],
        password=env["PG_PASSWORD"],
        connect_timeout=5,
    )
    with conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stg_palm_deforestation")
        def_rows = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM stg_palm_oil")
        palm_rows = cur.fetchone()[0]
    conn.close()

    print(
        f"[CHECK] Row counts -> stg_palm_deforestation: {def_rows}, stg_palm_oil: {palm_rows}")
    if not def_rows or not palm_rows:
        raise ValueError(
            "Quality check failed: one or more staging tables are empty!")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("palm-oil-load").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    load_staging(spark)
    quality_check()

    spark.stop()