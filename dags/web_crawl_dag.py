import sys
sys.path.insert(1, '~/airflow')
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
from postgres.database import init_db
from custom_operator.crawl_operator import CrawlOperator
from sub_dag import sub_dag
from airflow.operators.subdag_operator import SubDagOperator

apple_url = 'https://namu.wiki/w/%EC%82%AC%EA%B3%BC'

create_table_sql = """
CREATE TABLE IF NOT EXISTS wiki_page_tb(
    id SERIAL PRIMARY KEY,
    title varchar(200) NOT NULL,
    raw_html TEXT,
    updated_at TIMESTAMP NOT NULL,
    UNIQUE (title)
)
"""

insert_row_sql = """
INSERT INTO wiki_page_tb(title, raw_html, updated_at)
VALUES (%s, %s, %s)
ON CONFLICT (title) DO UPDATE 
    SET raw_html = excluded.raw_html,
        updated_at = excluded.updated_at
"""

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

t2 = SubDagOperator(task_id='t2',
                    subdag=sub_dag(dag.dag_id, 't2', args),
                    default_args=args,
                    dag=dag)

t1 >> t2
