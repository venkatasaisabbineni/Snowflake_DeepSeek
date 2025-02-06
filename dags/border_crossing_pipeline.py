from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# importing src functions
# from src.download_data import download_data
# from src.data_preprocessing import data_preprocessing

def download_data():
    os.system("python3 src/download_data.py")

def data_preprocessing():
    os.system("python3 src/data_preprocessing.py")


default_args = {
    "owner": "venkat",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "border_crossing_pipeline",
    default_args=default_args,
    description="Pipeline to download, process border crossing data",
    schedule_interval="@daily",
)

# Task 1: Download Data
download_task = PythonOperator(
    task_id="download_data",
    python_callable=download_data,
    dag=dag,
)

# Task 2: Process Data with Apache Spark
process_task = PythonOperator(
    task_id="process_data_pyspark",
    python_callable=data_preprocessing,
    dag=dag,
)

# # Task 3: Upload Data to Snowflake
# upload_task = PythonOperator(
#     task_id="upload_to_snowflake",
#     python_callable=upload_to_snowflake,
#     dag=dag,
# )

# # Task 4: Analyze Data with DeepSeek
# analyze_task = PythonOperator(
#     task_id="analyze_with_deepseek",
#     python_callable=analyze_with_deepseek,
#     dag=dag,
# )

# Define task dependencies
download_task >> process_task #>> upload_task >> analyze_task
