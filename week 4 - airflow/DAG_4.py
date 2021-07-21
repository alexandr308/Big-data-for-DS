from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


def hello():
    return 'Hello world!'


dag = DAG(dag_id='hi_dag',
          start_date=datetime(2019, 11, 30),
          schedule_interval='@once')

task1 = PythonOperator(task_id='task1', 
                       python_callable=hello, 
                       dag=dag)
