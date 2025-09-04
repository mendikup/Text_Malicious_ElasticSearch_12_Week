import json
import os.path
import pandas as pd


class DataLoader:

    def __init__(self):
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.path = os.path.join(base_path ,"data","tweets_injected 3.csv")

    def load_data(self):
        df = pd.read_csv(self.path)
        df["Antisemitic"] = df["Antisemitic"].astype(bool)
        return df



