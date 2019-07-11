import json
import traceback

from os import environ
from threading import Thread
from pymongo import MongoClient
from redis import Redis
from utils.work_distributer.requester import RefreshRequester



class MetricCollector(Thread):

    def __init__(self):
        self.queue = MetricQueue()
        self.connector = MongoConnector()
        Thread.__init__(self)

    def run(self):
        while True:
            try:
                metric = self.queue.get_new_metric()
                print(metric)
                self.connector.save_metric(metric)
            except:
                traceback.print_exc()
                pass


class MetricQueue(object):

    QUEUE = 'mezin:metrics'

    def __init__(self):
        self.redis = Redis(host='redis')

    def get_new_metric(self):
        key, value = self.redis.brpop(self.QUEUE)
        data = json.loads(value.decode())
        return data


class MongoConnector(object):

    def __init__(self):
        username = environ.get('MONGO_USER')
        password = environ.get('MONGO_PASSWORD')
        self.client = MongoClient('mongodb://mongo:27017/', username=username,
                                                       password=password)

    def save_metric(self, metric):
        db = self.client['mezin']
        db = db['metrics']
        db.insert_one(metric)

    def get_metrics(self):
        db = self.client['mezin']
        db = db['metrics']
        return db.find()
