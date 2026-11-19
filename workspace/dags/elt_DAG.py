"""
Final Project - Airflow DAG Definition

Author : Team 1
Batch  : CODA-RMT-023

Description:
    Automates and schedules the daily ELTL pipeline for Kalimantan palm oil
    supply chain and deforestation data: Extract -> Clean & Validate -> Load.
"""

from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Directory where pipeline scripts are mounted inside the Airflow container
SCRIPTS_DIR = "/workspace/dags"

# Default task retry and ownership settings
default_args = {
    "owner": "ryan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define DAG with max_active_runs=1 to prevent concurrent Spark sessions
# from exceeding the container's 3GB memory limit
with DAG(
    dag_id="palm_oil_deforestation_etl",
    default_args=default_args,
    description="ELTL pipeline for Kalimantan palm oil and deforestation data",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["palm_oil", "deforestation", "eltl"],
) as dag:

    # Task 1: Extract raw CSVs, filter for Kalimantan, and write raw Parquet files
    extract_task = BashOperator(
        task_id="extract", bash_command=f"python {SCRIPTS_DIR}/extract.py",)

    # Task 2: Clean columns, run Great Expectations validation, and write clean Parquet files
    clean_validate_task = BashOperator(
        task_id="clean_validate", bash_command=f"python {SCRIPTS_DIR}/clean_validate.py",)

    # Task 3: Load cleaned Parquet data into PostgreSQL staging tables and run row checks
    load_task = BashOperator(
        task_id="load", bash_command=f"python {SCRIPTS_DIR}/load.py",)

    # Define sequential task dependencies
    extract_task >> clean_validate_task >> load_task