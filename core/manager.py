import json
import time

from utils.utils import Utils

from core.loader import DataLoader
from core.dal import DAL
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
        print(f"len of the df1:  {len(df)}")

        self.preprocessing = Preprocessing(df)
        self.dal = DAL()
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
        print(f"len of the df: {len(df)}")

        # Index data
        self.dal.map_and_index_data(df, index_name="tweets")

        # 🔧 תיקון: המתן ו-refresh

        self.dal.es.indices.refresh(index="tweets")

        # Check indexing success
        indexed_count = self.dal.es.count(index="tweets")["count"]
        print(f"Actually indexed: {indexed_count} documents")

        # אם לא כל המסמכים נכנסו - יש בעיה!
        if indexed_count != len(df):
            print(f"⚠️ WARNING: {len(df) - indexed_count} documents missing!")
        # Retrieve all docs & analyze sentiment
        docs = self.dal.get_all_data()
        print(f"len of the docs:  {len(docs)}")

        docs = self.analyzer.find_sentiments_in_documents(docs)

        # Step 4: Update Elasticsearch with new sentiment field
        self.dal.update_sentiment_field(docs)
        self.dal.es.indices.refresh(index="tweets")

        results = self.analyzer.find_weapons_ids()
        self.dal.update_weapons_field(results)

        data =  self.dal.get_all_data()
        print(f"len of the data:  {len(data)}")
        print(json.dumps(data,indent=4))

        irrelevant_documents=self.dal.find_irrelevant_documents()
        self.dal.delete_irrelevant_documents(irrelevant_documents)

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
