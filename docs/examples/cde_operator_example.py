from datetime import timedelta
from airflow import DAG
from cloudera.airflow.operators.cde import CdeRunJobOperator
import pendulum

default_args = {
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
}

example_dag = DAG(
    'example-cdeoperator',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False
)

ingest_step1 = CdeRunJobOperator(
    connection_id='cde_runtime_api',
    task_id='ingest',
    retries=3,
    dag=example_dag,
    job_name='example-scala-pi'
)

prep_step2 = CdeRunJobOperator(
    task_id='data_prep',
    dag=example_dag,
    job_name='example-scala-pi'
)

ingest_step1 >> prep_step2