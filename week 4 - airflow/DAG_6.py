from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(dag_id='task_dag',
          start_date=datetime(2019, 12, 30),
          schedule_interval='@once')

task1 = DummyOperator(task_id='task1', dag=dag)
task2 = DummyOperator(task_id='task2', dag=dag)
task3 = DummyOperator(task_id='task3', dag=dag)
task4 = DummyOperator(task_id='task4', dag=dag)
task5 = DummyOperator(task_id='task5', dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)

task1 >> [task2, task3, task4]
[task2, task3, task4] >> task5
