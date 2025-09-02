import json
import os
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from utils.utils import Utils
from core.dal import Dal




class Analyzer:
    """Provide basic NLP utilities for enrichment."""

    def __init__(self):
        self.dal =Dal()


    def find_sentiment(self ,txt) -> str:
        """Return sentiment label for *txt* using VADER."""

        score = SentimentIntensityAnalyzer().polarity_scores(txt)
        compound = score["compound"]
        if compound <= -0.5:
            return "negative"

        elif compound >= 0.5:
            return "positive"
        else:
            return "neutral"


    def find_sentiments_in_documents(self, docs):
        for doc in docs:
            text = doc["text"]
            sentiment =self.find_sentiment(text)
            doc["sentiment"] = sentiment
        return docs


    def  find_weapons_ids(self):
        """
        Build a dict {weapon: [ids]} for all weapons in blacklist.
        """
        weapons_list = Utils.load_black_list()
        print("loaded successful")
        results = {}

        for weapon in weapons_list:
            ids = self.dal.get_doc_ids_with_weapon(weapon)
            if ids:
                results[weapon] = ids
        return results






