from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

dag = DAG(dag_id='spark_submit_dag',
          start_date=datetime(2019, 12, 1),
          schedule_interval='@daily')

submit_operator = SparkSubmitOperator(task_id='spark_submit',
                                      application='PySparkJob.py',
                                      application_args=['input.csv', 'output.csv'],
                                      conn_id='spark_default',
                                      dag=dag)
