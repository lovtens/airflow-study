from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import time
from pprint import pprint

args = {'owner': 'ollie',
        'start_date': days_ago(n=1),
        'depends_on_past': True,
        'wait_for_downstream': True
        }

dag = DAG(dag_id='my_python_dag',
          default_args=args,
          schedule_interval='@daily')

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

def print_fruit(fruit_name, **kwargs):
    print('=' * 60)
    print('fruit_name:', fruit_name)
    print('=' * 60)
    pprint(kwargs)
    print('=' * 60)
    return 'print complete!!!'

def sleep_seconds(seconds, **kwargs):
    print('=' * 60)
    print('seconds:' + str(seconds))
    print('=' * 60)
    pprint(kwargs)
    print('=' * 60)
    print('sleeping...')
    time.sleep(seconds)
    return 'sleep well!!!'


t1 = PythonOperator(task_id='task_1',
                    provide_context=True,
                    python_callable=print_fruit,
                    op_kwargs={'fruit_name': 'apple'},
                    dag=dag)

t2 = PythonOperator(task_id='task_2',
                    provide_context=True,
                    python_callable=print_fruit,
                    op_kwargs={'fruit_name': 'banana'},
                    dag=dag)

t3 = PythonOperator(task_id='task_3',
                    provide_context=True,
                    python_callable=sleep_seconds,
                    op_kwargs={'seconds': 10},
                    dag=dag)

t4 = PythonOperator(task_id='task_4',
                    provide_context=True,
                    python_callable=print_fruit,
                    op_kwargs={'fruit_name': 'cherry'},
                    dag=dag)

t5 = BashOperator(task_id='task_5', 
                    provide_context=True,
                    bash_command=templated_command,
                    params={'my_param': 'Parameter I passed in'},
                    dag=dag)


dag.doc_md = """
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

t5.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

# apple
# banana, sleep_seconds
# banana, sleep_seconds
# cherry

t5 >> [t2, t3]
[t2, t3] >> t4