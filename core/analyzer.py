import json
import os
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from utils.utils import Utils
from core.dal import Dal
from utils.simple_logger import logger


class Analyzer:
    """Provide basic NLP utilities for enrichment."""

    def __init__(self):
        self.dal = Dal()

    def find_sentiment(self, txt) -> str:
        """Return sentiment label for *txt* using VADER."""

        # בדיקה פשוטה של הטקסט
        if not txt or txt.strip() == "":
            logger.warning(f"Empty text found: {repr(txt)}")
            return "neutral"

        try:
            score = SentimentIntensityAnalyzer().polarity_scores(txt)
            compound = score["compound"]
            if compound <= -0.5:
                return "negative"
            elif compound >= 0.5:
                return "positive"
            else:
                return "neutral"
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            return "neutral"

    def find_sentiments_in_documents(self, docs):
        logger.info(f"Starting sentiment analysis for {len(docs)} documents")

        processed = 0
        errors = 0

        for doc in docs:
            try:
                text = doc.get("text", "")
                sentiment = self.find_sentiment(text)
                doc["sentiment"] = sentiment
                processed += 1

                # לוג כל 500 מסמכים
                if processed % 500 == 0:
                    logger.info(f"Processed {processed} documents so far...")

            except Exception as e:
                errors += 1
                doc["sentiment"] = "neutral"
                logger.error(f"Error processing document: {e}")

        logger.info(f"Sentiment analysis done: {processed} processed, {errors} errors")
        return docs

    def find_weapons_ids(self):
        """Build a dict {weapon: [ids]} for all weapons in blacklist."""
        weapons_list = Utils.load_black_list()
        logger.info(f"Loaded {len(weapons_list)} weapons from blacklist")

        results = {}
        total_found = 0

        for weapon in weapons_list:
            ids = self.dal.get_doc_ids_with_weapon(weapon)
            if ids:
                results[weapon] = ids
                total_found += len(ids)
                logger.info(f"Found {len(ids)} docs with weapon: {weapon}")

        logger.info(f"Weapons search done: {len(results)} weapons found, {total_found} total matches")
        return results