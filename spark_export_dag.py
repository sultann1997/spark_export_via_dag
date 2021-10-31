from builtins import range
from datetime import timedelta, date
import datetime as dt
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.bash_sensor import BashSensor

dbase = "{{dag_run.conf['database']}}"
table_hdfs = "{{dag_run.conf['table_hdfs']}}"
table_ora = "{{dag_run.conf['table_ora']}}"
start_date = "{{dag_run.conf['start_date']}}"
end_date = "{{dag_run.conf['end_date']}}"

args = {
    'email': ['my_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'owner': 'airflow_user',
    'start_date': dt.datetime(2021, 8, 10),
}

dag = DAG(
    dag_id='spark_export_preprod',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['spark', 'ETL'],
    catchup=False
)

spark_export = BashOperator(
    task_id='spark_export',
    bash_command=f'python spark_export.py \"{dbase}\" \"{table_hdfs}\" \"{table_ora}\" \"{start_date}\" \"{end_date}\"',
    dag=dag,
)


spark_export

if __name__ == "__main__":
    dag.cli()








    


