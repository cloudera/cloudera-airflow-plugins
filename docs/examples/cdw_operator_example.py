from airflow import DAG
from cloudera.airflow.providers.operators.cdw import CdwExecuteQueryOperator
import pendulum


default_args = {
    'owner': 'dag_owner',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
}

example_dag = DAG(
    'example-cdwoperator',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False
)

cdw_query = """
USE default;
select 'a', 'b', 1
"""

cdw_step = CdwExecuteQueryOperator(
    task_id='cdw-test',
    dag=example_dag,
    cli_conn_id='cdw-beeline',
    hql=cdw_query,
    # The following values `schema`, `query_isolation`
    # are the default values, just presented here for the example.
    schema='default',
    query_isolation=True
)


cdw_step