from airflow.models import DAG
from airflow.contrib.file_sensor import FileSensor
from airflow.operators.python import PythonOperator
from datetime import date, datetime
from source.scripts import extract_table_to_csv

dag = DAG(dag_id='etl_flow', default_args={'start_date': datetime(2023, 4, 10)})

sensor = FileSensor(task_id='sense_csv_files',
                    filepath="../resources/*.csv",
                    poke_interval=5,
                    timeout=15,
                    dage=dag)

python_task = PythonOperator(
    task_id="extract_table_to_csv",
    python_callable=extract_table_to_csv.write_table_data_to_csv,
    dag=dag
)

sensor >> python_task
