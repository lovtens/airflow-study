from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
import requests
from datetime import datetime
from postgres.database import db_session
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
        target.parent_id = parent_id
        target.child_id = child_id
        target.updated_at = updated_at


class CrawlOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            url: str,
            title: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.url = url
        self.title = title

    def execute(self, context):
        url = self.url
        parent_title = self.title
        print(parent_title)
        resp = requests.get(url)
        apple_doc = resp.text
        soup = BeautifulSoup(apple_doc, 'html.parser')
        a_list = list(filter(lambda x: 'class' in x.attrs and x.attrs['class'] == ['wiki-link-internal'], soup.find_all("a")))
        # a_list = list(filter(lambda x: unquote(x.attrs['href']) != "/w/"+str(x.attrs['title']), a_list))
        a_list = list(set(a_list))

        # # 부모 + 자식을 WikiPage에 insert
        parent_id = insert_to_wiki_page(parent_title, True)
        updated_at = datetime.now()
        for x in a_list:
            attrs = x.attrs
            title = attrs.get('title')
            child_id = insert_to_wiki_page(title, False)
            insert_to_wiki_page_relation(parent_id, child_id, updated_at)

        db_session.commit()
