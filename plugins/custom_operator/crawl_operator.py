from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
import requests
from datetime import datetime
from postgres.database import db_session, as_dict
from postgres.models import WikiPage, WikiPageRelation
from bs4 import BeautifulSoup
from urllib.parse import unquote


def insert_to_wiki_page(title, state):
    target = db_session.query(WikiPage) \
        .filter(WikiPage.title == title).first()
    if not target:
        args = {
            'title': title,
            'state': state
        }
        new_row = WikiPage(**args)
        db_session.add(new_row)
        db_session.flush()
        return new_row.id
    else:
        target.state = state
        return target.id


def is_doc_exists(title):
    target = db_session.query(WikiPage) \
        .filter(WikiPage.title == title).first()

    if not target:
        return False, None
    else:
        return True, target.id


def insert_to_wiki_page_relation(parent_id, child_id, updated_at):
    target = db_session.query(WikiPageRelation) \
        .filter(WikiPageRelation.parent_id == parent_id) \
        .filter(WikiPageRelation.child_id == child_id).first()

    if not target:
        args = {
            'parent_id': parent_id,
            'child_id': child_id,
            'updated_at': updated_at
        }
        new_row = WikiPageRelation(**args)
        db_session.add(new_row)
    else:
        target.updated_at = updated_at


def crawl_doc(title, url):
    parent_title = title
    resp = requests.get(url)
    doc_text = resp.text
    soup = BeautifulSoup(doc_text, 'html.parser')
    a_list = list(
        filter(lambda x: 'class' in x.attrs and x.attrs['class'] == ['wiki-link-internal'], soup.find_all("a")))
    # a_list = list(filter(lambda x: unquote(x.attrs['href']) != "/w/"+str(x.attrs['title']), a_list))
    a_list = list(set(a_list))

    # # 부모 + 자식을 wiki_page insert
    parent_id = insert_to_wiki_page(parent_title, True)
    updated_at = datetime.now()
    for x in a_list:
        attrs = x.attrs
        title = attrs.get('title')
        is_exists, child_id = is_doc_exists(title)
        if not is_exists:
            child_id = insert_to_wiki_page(title, False)

        insert_to_wiki_page_relation(parent_id, child_id, updated_at)

    db_session.commit()


class CrawlOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        for i in range(1):
            result = db_session.query(WikiPage).first()
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
                    url = f'https://namu.wiki/w/{title}'
                    docs[title] = url

            print(f'{i}번째 depth docs 갯수: {len(docs.keys())}\n')
            for i, (k, v) in enumerate(docs.items()):
                if i % 10 == 0:
                    print(f'{i}/{len(docs.keys())}')
                crawl_doc(title=k, url=v)
