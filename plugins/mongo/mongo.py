import pymongo
from bson.objectid import ObjectId
import urllib.parse
import re


class MongoConnector:
    username: str
    password: str
    db_name: str
    collection_name: str

    def __init__(self):
        self.username = urllib.parse.quote_plus('gur')
        self.password = urllib.parse.quote_plus('1234')
        self.db_name = 'wiki'
        self.collection_name = 'namu'
        self.client = pymongo.MongoClient(
            'localhost:16010',
            username=self.username,
            password=self.password,
            authSource=self.db_name,
            authMechanism='SCRAM-SHA-256'
        )

    @property
    def collection(self):
        return self.client[self.db_name][self.collection_name]

    def make_finder(self, protection=None):
        def finder(title):
            # 없으면 result None
            result = self.collection.find_one({"title": title}, protection)
            return result
        return finder

