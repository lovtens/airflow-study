import sys
sys.path.insert(1, '~/airflow')
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
from postgres.models import Base

# url = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
#         "gur", "1234", "localhost", "5432", "airflow")

url = "mysql+pymysql://{}:{}@{}:{}/{}".format(
        "gur", "1234", "localhost", "3306", "wiki")

engine = create_engine(url, convert_unicode=True, echo=False, encoding="utf8")
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))


def init_db():
    print('init_db')
    meta = MetaData()
    Base.query = db_session.query_property()
    Base.metadata.create_all(engine)


def handle_datatype(value):
    if type(value) == datetime:
        return str(value)
    else:
        return value


def as_dict(model):
    return {c.name: handle_datatype(getattr(model, c.name)) for c in model.__table__.columns}


