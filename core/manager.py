import json
import time
from utils.utils import Utils
from core.loader import DataLoader
from core.dal import Dal
from core.analyzer import Analyzer
from core.preprocessing import Preprocessing
from utils.simple_logger import logger


class Manager:
    """Orchestrator class that connects all components."""

    def __init__(self):
        logger.info("=== Starting Manager ===")

        self.loader = DataLoader()
        df = self.loader.load_data()
        logger.info(f"Loaded {len(df)} records")

        self.preprocessing = Preprocessing(df)
        self.dal = Dal()
        self.analyzer = Analyzer()

        logger.info("Manager initialized successfully")

    def run(self):
        """Run the main data pipeline."""
        logger.info("=== STARTING PIPELINE ===")

        # Step 1: Preprocess
        logger.info("Step 1: Preprocessing data")
        self.preprocessing.prepare_data()
        df = self.preprocessing.get_preprocessed_data()
        logger.info(f"Preprocessing done: {len(df)} documents")

        # Step 2: Index data
        logger.info("Step 2: Indexing to Elasticsearch")
        self.dal.map_and_index_data(df, index_name="tweets")
        time.sleep(2)  # חכה שהindexing יסתיים

        # Step 3: Get data and analyze sentiment
        logger.info("Step 3: Sentiment analysis")
        docs = self.dal.get_all_data()
        docs = self.analyzer.find_sentiments_in_documents(docs)

        # בדיקה אחרי הניתוח
        with_sentiment_after = len([d for d in docs if d.get("sentiment")])
        logger.info(f"After analysis: {with_sentiment_after} docs have sentiment")

        # Step 4: Update sentiment in Elasticsearch
        logger.info("Step 4: Updating sentiment field")
        self.dal.update_sentiment_field(docs)
        time.sleep(2)

        # Step 5: Weapons
        logger.info("Step 5: Processing weapons")
        results = self.analyzer.find_weapons_ids()
        self.dal.update_weapons_field(results)
        time.sleep(2)

        # Final check
        logger.info("=== FINAL VERIFICATION ===")
        final_data = self.dal.get_all_data()
        print(json.dumps(final_data, indent=4))

        total = len(final_data)
        with_sentiment = len([d for d in final_data if d.get("sentiment")])
        missing_sentiment = total - with_sentiment

        logger.info(f"FINAL RESULTS:")
        logger.info(f"  Total documents: {total}")
        logger.info(f"  With sentiment: {with_sentiment}")
        logger.info(f"  Missing sentiment: {missing_sentiment}")

        if missing_sentiment > 0:
            logger.error("=== DOCUMENTS MISSING SENTIMENT ===")
            count = 0
            for doc in final_data:
                if not doc.get("sentiment"):
                    count += 1
                    if count <= 5:  # הצג רק 5 ראשונים
                        text_preview = doc.get('text', '')[:50]
                        logger.error(f"Missing ID: {doc.get('_id')}, Text: {text_preview}...")

            if count > 5:
                logger.error(f"... and {count - 5} more missing sentiment")

        logger.info("=== PIPELINE COMPLETED ===")
        return final_data


if __name__ == "__main__":
    logger.info("Starting application")
    try:
        m = Manager()
        m.run()
        logger.info("Application completed successfully")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise