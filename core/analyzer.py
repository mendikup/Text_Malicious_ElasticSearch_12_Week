import os
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re
import pandas as pd



# load black list once at module import
# black_list = load_black_list()


class Analyzer:
    """Provide basic NLP utilities for enrichment."""

    @staticmethod
    def find_sentiment(txt) -> str:
        """Return sentiment label for *txt* using VADER."""

        score = SentimentIntensityAnalyzer().polarity_scores(txt)
        compound = score["compound"]
        if compound <= -0.5:
            return "negative"

        elif compound >= 0.5:
            return "positive"
        else:
            return "neutral"


