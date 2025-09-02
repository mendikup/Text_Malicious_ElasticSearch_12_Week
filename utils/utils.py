from dateutil import parser
import os
from pathlib import Path
class Utils:

    @staticmethod
    def parsar_date(date: str) -> str:
        dt = parser.parse(date)
        return dt.isoformat()




    @staticmethod
    def load_black_list():
        base_path = Path(__file__).resolve().parent.parent  # from utils/ go up to project root
        data_path = base_path / "data" / "weapon_list.txt"

        with open(data_path, mode="r", encoding="utf-8") as f:
            return f.read().splitlines()

# utils = Utils()
#     # load black list once at module import
# black_list =utils.load_black_list()
# print(black_list)

