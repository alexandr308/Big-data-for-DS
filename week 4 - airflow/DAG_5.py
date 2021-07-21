from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(dag_id='prepare_dag',
          start_date=datetime(2019, 11, 21),
          schedule_interval='@once')

init_task = BashOperator(task_id='init',
                         bash_command='/usr/bin/init.sh',
                         dag=dag)

prepare_train_task = BashOperator(task_id='prepare_train',
                                  bash_command='/usr/bin/prepare_train.sh',
                                  dag=dag)

prepare_test_task = BashOperator(task_id='prepare_test',
                                 bash_command='/usr/bin/prepare_test.sh',
                                 dag=dag)

init_task >> [prepare_train_task, prepare_test_task]
