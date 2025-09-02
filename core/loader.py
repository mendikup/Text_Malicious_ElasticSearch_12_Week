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
        df =df.to_dict(orient= "records")
        return df

    @staticmethod
    def load_black_list():
        """Load a list of weapons from a text file."""
        base_path = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_path, "weapons_list.txt")
        with open(path, mode="r", encoding="utf-8") as f:
            data = f.read()
        return data.splitlines()


# d= DataLoader()
# d.load_data()