import airflow
from airflow import DAG,macros
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import S3KeySensor, ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from airflow.models import TaskInstance
from airflow.utils.state import State
from functools import partial
from datetime import date, datetime, timedelta
import json
import sqlite3
import pandas as pd

dag_name = 'upwork_assessment'

'''
This airflow DAG is responsible for 
conversion of data from parquet to JSON and CSV.

Flow requires to read data from sqlite
and take out all the pending status records.

Later it reads those files in pandas and 
convert them as per mentioned target format.

In between to handle exceptions it stores the status.
P -> S -> C
P = Pending
S = Started transforamation
C = Transformation complete
'''

args = {
    'owner': 'sheruwala',
    'depends_on_past': True,
    'wait_for_downstream' : True,
    'email': 'sam.heruwala@gmail.com',
    'start_date' : datetime(2021,11,12),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(dag_name, default_args=args, max_active_runs=1, schedule_interval='0 0 * * *')
DATABASE = "transfer"
TABLE = "filesData"

###################################################################

# Create table and Load data to sqlite.
def createTable(**kwargs):
    conn = sqlite3.connect(DATABASE) 
    c = conn.cursor()
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE} (
        source_file_path TEXT,
        source_file_format TEXT,
        target_file_path TEXT,
        target_file_format TEXT,
        status TEXT) ''')
    c.execute(f'''
        INSERT INTO {TABLE} (source_file_path,source_file_format,target_file_path,target_file_format,status)
        VALUES
        ('data/inputData.parquet','parquet','output/jsonData.json','json','P'),
        ('data/inputData.parquet','parquet','output/csvData.csv','csv','P') ''')
    conn.commit()
    conn.close()

# Update status of the job when done.
def updateStatus(from_status, to_status, **kwargs):
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute(f"update {TABLE} set status='{from_status}' where status='{to_status}'")
    conn.commit()
    conn.close()

# Read data from status S and then transform.
# Currently it is supporting parquet to (Json,CSV) conversion.
def transform(status, **kwargs):
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    data = c.execute(f"SELECT * FROM {TABLE} where status='S'").fetchall()
    conn.commit()
    conn.close()
    for (source_file_path,source_file_format,target_file_path,target_file_format,status) in data:
    df = pd.read_parquet(source_file_path)
    if(target_file_format=='csv'):
        df.to_csv(target_file_path)
    if(target_file_format=='json'):
        df.to_json(target_file_path)

# Function to update all step status in case of failure
def _finally(**kwargs):
    tasks = kwargs['dag_run'].get_task_instances()
    for task_instance in tasks:
        if task_instance.current_state() == State.FAILED and task_instance.task_id != kwargs['task_instance'].task_id:
            [i.set_state("failed") for i in tasks]
            raise Exception("Task "+task_instance.task_id+" failed. Failing this DAG run.")

###################################################################

create_TABLE = PythonOperator(
    task_id="create_TABLE",
    python_callable=createTable,
    provide_context=True,
    dag=dag)

update_status_S = PythonOperator(
    task_id="update_status_S",
    python_callable=updateStatus,
    op_kwargs={"from_status":"P","to_status":"S"},
    trigger_rule=TriggerRule.ALL_SUCCESS,
    provide_context=True,
    dag=dag)

transform = PythonOperator(
    task_id="transform",
    python_callable=transform,
    op_kwargs={"status":"S"},
    trigger_rule=TriggerRule.ALL_SUCCESS,
    provide_context=True,
    dag=dag)

update_status_C = PythonOperator(
    task_id="update_status_C",
    python_callable=updateStatus,
    op_kwargs={"from_status":"S","to_status":"C"},
    trigger_rule=TriggerRule.ALL_SUCCESS,
    provide_context=True,
    dag=dag)

# Last step to make sure all step ran successfully
finally_ = PythonOperator(
    task_id="finally",
    python_callable=_finally,
    trigger_rule=TriggerRule.ALL_DONE,
    provide_context=True,
    dag=dag)

# Job flow
create_TABLE >> update_status_S >> transform >> update_status_C >> finally_
