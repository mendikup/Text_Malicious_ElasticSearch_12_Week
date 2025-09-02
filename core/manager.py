import json
from utils.utils import Utils

from core.loader import DataLoader
from core.elastic_connector import Elastic_Connector
from core.analyzer import Analyzer


class Manager:

    def __init__(self):
        self.loader = DataLoader()
        self.es = Elastic_Connector()
        self.analyzer = Analyzer()

    def run(self):
        df = self.loader.load_data()
        # Apply date parsing correctly
        df["CreateDate"] = df["CreateDate"].apply(Utils.parsar_date)
        # Add empty sentiment + weapons
        df["sentiment"] = ""
        df["weapons"] = [[] for _ in range(len(df))]
        self.es.mapping_and_index_data(df,index_name ="tweets")


        # data = self.get_all_data()
        # data = self.find_sentiment_in_document(data)
        # self.es.update_document(sentiment, weapons_detected)
        # self.es.delete_irrelevant_documents()

    def get_all_data(self):
        data = self.es.get_all_data()
        return data

    def find_sentiment_in_document(self,data):
        for doc in data:
            sentiment = self.analyzer.find_sentiment(doc["text"])
            doc["sentiment"] = sentiment





    def get_antisemitic_tweets_with_weapons(self):
        pass
        # self.es.get_antisemitic_tweets_with_weapons()


    def get_sensitive_tweets_with_two_weapons(self):
        pass








m = Manager()
m.run()

