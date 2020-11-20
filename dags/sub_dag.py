from airflow.models import DAG
from custom_operator.crawl_operator import CrawlOperator
from postgres.database import db_session, as_dict
from postgres.models import WikiPage


def sub_dag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        # schedule_interval="* * * * *",
        schedule_interval='@daily'
    )
    result = db_session.query(WikiPage).all()
    if not result:
        docs = {
            '사과': 'https://namu.wiki/w/사과'
        }
    else:
        result = db_session.query(WikiPage) \
            .filter(WikiPage.state == False).all()

        rows = [as_dict(x) for x in result]
        docs = dict()
        for row in rows:
            title = row.get('title')
            url = 'https://namu.wiki/w/{}'.format(title)
            docs[title] = url
    
    from pprint import pprint
    pprint(docs)

    # for i, (k, v) in enumerate(docs.items()):
    #     CrawlOperator(task_id='%s-task-%s' % (child_dag_name, i + 1),
    #                   provide_context=True,
    #                   url=v,
    #                   title=k,
    #                   dag=dag_subdag)

    return dag_subdag
