from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
from postgres.database import db_session, as_dict
from postgres.models import WikiPage, WikiPageRelation
from bs4 import BeautifulSoup
import requests
from mongo.mongo import MongoConnector
import re

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


def crawl_doc(key, finder):
    doc = finder(key)
    if not doc:
        return
    p_child = re.compile(r"\[\[[^\[\]]*\]\]")
    p_external = re.compile(r".*https?://")

    child_list = []
    iterator = p_child.finditer(doc['text'])
    for match in iterator:
        match_str = match[0][2:-2]
        # 외부 문서 필터링
        match_str_split = match_str.split('|')
        if len(match_str_split) > 0:
            key = match_str_split[0]
            m = p_external.match(key)
            if m:
                continue
            if not finder(key):
                continue
            child_list.append(match_str_split[0])

    # # 부모 + 자식을 wiki_page insert
    parent_id = insert_to_wiki_page(key, True)
    updated_at = datetime.now()
    for x in child_list:
        is_exists, child_id = is_doc_exists(x)
        if not is_exists:
            child_id = insert_to_wiki_page(x, False)

        insert_to_wiki_page_relation(parent_id, child_id, updated_at)

    db_session.commit()


class CrawlOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        # mongo connector
        mc = MongoConnector()
        finder = mc.make_finder()
        i = 0
        while True:
            i += 1
            result = db_session.query(WikiPage).first()
            if not result:
                docs = ['사과']
            else:
                result = db_session.query(WikiPage) \
                    .filter(WikiPage.state == False).all()
                if not result:
                    break
                rows = [as_dict(x) for x in result]
                docs = [row.get('title') for row in rows]

            print(f'{i}번째 depth docs 갯수: {len(docs)}\n')
            for i, k in enumerate(docs):
                if i % 10 == 0:
                    print(f'{i}/{len(docs)}')
                crawl_doc(key=k, finder=finder)
