from sqlalchemy import Column, String, Text, DateTime, BIGINT, UniqueConstraint, JSON, ForeignKey, BOOLEAN
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class WikiPage(Base):
    __tablename__ = 'wiki_page'
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    state = Column(BOOLEAN, nullable=False)
    __table_args__ = (
        {"schema": "wiki"}
    )


class WikiPageRelation(Base):
    __tablename__ = 'wiki_page_relation'
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    parent_id = Column(BIGINT, ForeignKey(WikiPage.id))
    child_id = Column(BIGINT, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    __table_args__ = (
        {"schema": "wiki"}
    )
