import json

from utils.utils import Utils

class Preprocessing:

    def __init__(self,df):
        self.df = df

    def prepare_data(self):
        # Apply date parsing correctly
        self.df["CreateDate"] = self.df["CreateDate"].apply(Utils.parsar_date)
        # Add empty sentiment + weapons
        self.df["sentiment"] = ""
        self.df["weapons"] = [[] for _ in range(len(self.df))]


    def get_preprocessed_data(self):
        if self.df is not None:
            return self.df