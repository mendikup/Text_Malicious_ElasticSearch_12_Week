from dateutil import parser
class Utils:

    @staticmethod
    def parsar_date(date: str) -> str:
        dt = parser.parse(date)
        return dt.isoformat()