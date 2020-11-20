from custom_operator.hello_operator import HelloOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago

args = {'owner': 'ollie',
        'start_date': days_ago(n=1),
        'depends_on_past': True,
        'wait_for_downstream': True
        }

dag = DAG(dag_id='hello_operator_dag',
          default_args=args,
          schedule_interval='@daily')

t5 = HelloOperator(task_id='task_1',
                   provide_context=True,
                   name='GUR',
                   dag=dag)


