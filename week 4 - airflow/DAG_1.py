from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(dag_id='dummy_dag',
          start_date=dag_start_date,
          schedule_interval='@once')

dummy_task1 = DummyOperator(task_id='dummy_operator1', dag=dag)
