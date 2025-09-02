import json
from email import parser
from pprint import pprint
from dateutil import parser
from loader import DataLoader

from elasticsearch import Elasticsearch ,helpers



class Elastic_Connector:

    def __init__(self):
        self.es = Elasticsearch('http://localhost:9200')


    def mapping_and_index_data(self, data,index_name):

        # Delete index if it already exists
        self.es.indices.delete(index=index_name, ignore_unavailable=True)

        # Create index and mapping
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(
                index="tweets",
                mappings={
                    "properties": {
                        "CreateDate": {
                            "type": "date"
                        },
                        "Antisemitic": {
                            "type": "boolean"
                        },
                        "text": {
                            "type": "text",
                            "fields": {
                                "raw": {"type": "keyword"}
                            }
                        },
                        "sentiment": {
                            "type": "keyword"
                        },
                        "weapons": {
                            "type": "keyword"
                        }
                    }
                },
            )

        # creates nwe fields to use later
        for doc in data:
            doc['CreateDate'] = self._parsar_date(doc['CreateDate'])
            doc["sentiment"] = ""
            doc["weapons"] = []

        actions = [
            {
                "_op_type": "index",
                "_index": "tweets",
                "_source": doc
            }
            for doc in data
        ]

        helpers.bulk(self.es, actions)
        res =self.es.search(index="tweets", query={"match_all": {}}, size=300)
        docs = [hit["_source"] for hit in res["hits"]["hits"]]
        print(json.dumps(docs, indent=4))


    def _parsar_date( self,date: str) -> str:
        dt = parser.parse(date)
        return dt.isoformat()

    def get_all_data(self):
        res = self.es.search(index="tweets",
                             query={"match_all": {}},
                             size=300)
        docs = [hit["_source"] for hit in res["hits"]["hits"]]
        return docs

    def delete_irrelevant_documents(self):
        weapons_list = DataLoader.load_black_list()
        res = self.es.delete_by_query(
            index="tweets",
            body={
                "bool": {
                    "must": [
                        {"term":
                             {"Antisemitic": False}}
                    ],
                    "must_not": [
                        {
                            "bool": {
                                "should": [
                                    {"terms": {"weapons":weapons_list}}

                                ]
                            }
                        }
                    ]
                }
            }
        )
        return res

    def update_document(self,sentiment,weapons_detected):
        pass
