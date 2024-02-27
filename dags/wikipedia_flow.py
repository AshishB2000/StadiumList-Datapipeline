from airflow import DAG
import os
import sys
from datetime import datetime

from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipeline import extract_wikipedia_data, transformed_wikipedia_data, write_wikipedia_data

dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "Ashish_DE",
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval=None,
    catchup=False

)

# Extraction

extract_data_from_wikipedia = PythonOperator(
    task_id="extract_wikipedia_data",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_U.S._stadiums_by_capacity"},
    dag=dag
)

# preprocessing

transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    python_callable=transformed_wikipedia_data,
    provide_context=True,
    dag=dag
)
# Write


write_wikipedia_data = PythonOperator(
    task_id='write_wikipedia_data',
    python_callable=write_wikipedia_data,
    provide_context=True,
    dag=dag
)

extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data
