from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

dag = DAG(dag_id='trigger_dag',
          start_date=datetime(2019, 12, 1),
          schedule_interval='@once')

trigger_job1 = TriggerDagRunOperator(task_id='trigger_job1',
                                     trigger_dag_id='job1_dag',
                                     dag=dag)

trigger_job2 = TriggerDagRunOperator(task_id='trigger_job2',
                                     trigger_dag_id='job2_dag',
                                     dag=dag)
