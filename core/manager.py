import json

from core.loader import DataLoader
from core.elastic_connector import Elastic_Connector
from core.analyzer import Analyzer


class Manager:

    def __init__(self):
        self.loader = DataLoader()
        self.es = Elastic_Connector()
        self.analyzer = Analyzer()

    def run(self):
        data = self.loader.load_data()
        self.es.mapping_and_index_data(data,index_name ="tweets")








