import sys

sys.path.insert(1, '~/airflow')
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
from postgres.database import init_db
from custom_operator.crawl_operator import CrawlOperator

# 모든 태스크들에 설정할 default args
args = {'owner': 'ollie',
        'start_date': days_ago(n=1),
        'depends_on_past': True,
        'wait_for_downstream': True
        }

dag = DAG(dag_id='web_crawl_dag',
          default_args=args,
          schedule_interval='@daily')

t1 = PythonOperator(task_id='t1',
                    provide_context=False,
                    python_callable=init_db,
                    dag=dag)

t2 = CrawlOperator(task_id='t2',
                   provide_context=False,
                   dag=dag)

t1 > t2
