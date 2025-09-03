import json
from utils.utils import Utils
from utils.simple_logger import logger


class Preprocessing:

    def __init__(self, df):
        self.df = df
        logger.info(f"Preprocessing initialized with {len(df)} documents")

    def prepare_data(self):
        logger.info("Starting data preparation")

        # Apply date parsing
        self.df["CreateDate"] = self.df["CreateDate"].apply(Utils.parsar_date)

        # Add empty sentiment + weapons
        self.df["sentiment"] = ""
        self.df["weapons"] = [[] for _ in range(len(self.df))]

        # בדיקה פשוטה
        text_count = len(self.df[self.df["text"].notna() & (self.df["text"] != "")])
        logger.info(f"Preparation done: {len(self.df)} total, {text_count} with text")

    def get_preprocessed_data(self):
        if self.df is not None:
            return self.df
        else:
            logger.error("No data available!")
            return None


# דוגמה לשימוש פשוט:
