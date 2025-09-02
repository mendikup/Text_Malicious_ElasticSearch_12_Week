import json
from utils.utils import Utils

from core.loader import DataLoader
from core.dal import Dal
from core.analyzer import Analyzer
from core.preprocessing import Preprocessing


class Manager:
    """
    Orchestrator class that connects all components:
    Loader -> Preprocessing -> DAL -> Analyzer.
    """

    def __init__(self):
        """Initialize Loader, Preprocessing, DAL, and Analyzer objects."""
        self.loader = DataLoader()
        df = self.loader.load_data()
        self.preprocessing = Preprocessing(df)
        self.dal = Dal()
        self.analyzer = Analyzer()

    def run(self):
        """
        Run the main data pipeline:
        . Preprocess the data
        . Index the data into Elasticsearch
        . Retrieve and analyze documents
        . Update documents with new fields
        """
        # Preprocess
        self.preprocessing.prepare_data()
        df = self.preprocessing.get_preprocessed_data()

        # Create mapping & index data
        self.dal.map_and_index_data(df, index_name="tweets")

        # Retrieve all docs & analyze sentiment
        docs = self.dal.get_all_data()
        docs = self.analyzer.find_sentiments_in_documents(docs)

        # Step 4: Update Elasticsearch with new sentiment field
        self.dal.update_sentiment_field(docs)

        results = self.analyzer.find_weapons_ids()
        self.dal.update_weapons_field(results)

        data =  self.dal.get_all_data()
        print(json.dumps(data, indent=4))




        # self.dal.delete_irrelevant_documents()

    def get_antisemitic_tweets_with_weapons(self):
        """Proxy to DAL method for retrieving antisemitic tweets with weapons."""
        self.dal.get_antisemitic_tweets_with_weapons()

    def get_sensitive_tweets_with_two_weapons(self):
        """Proxy to DAL method for retrieving sensitive tweets with two weapons."""
        self.dal.get_sensitive_tweets_with_two_weapons()


# Run the pipeline
if __name__ == "__main__":
    m = Manager()
    m.run()
