import json
from loader import DataLoader
from elasticsearch import Elasticsearch, helpers


class Dal:
    """
    Data Access Layer (DAL) for interacting with Elasticsearch.
    Provides methods for indexing, updating, deleting and retrieving documents.
    """

    def __init__(self):
        """Initialize connection to Elasticsearch server."""
        self.es = Elasticsearch('http://localhost:9200')

    def map_and_index_data(self, df, index_name: str):
        """
        Create mapping for the index and index the given dataframe.

        :param df: pandas DataFrame containing the data to index
        :param index_name: name of the Elasticsearch index
        """
        docs = df.to_dict(orient="records")

        # Delete index if it already exists (ignores errors if not found)
        self.es.indices.delete(index=index_name, ignore=[400, 404])

        # Create index with specific mapping
        self.es.indices.create(
            index=index_name,
            mappings={
                "properties": {
                    "CreateDate": {"type": "date"},
                    "Antisemitic": {"type": "boolean"},
                    "text": {
                        "type": "text",
                        "fields": {"raw": {"type": "keyword"}}
                    },
                    "sentiment": {"type": "keyword"},
                    "weapons": {"type": "keyword"}
                }
            },
        )

        # Bulk index documents
        actions = [
            {"_op_type": "index", "_index": index_name, "_source": doc}
            for doc in docs
        ]
        helpers.bulk(self.es, actions)

    def get_all_data(self):
        """
        Retrieve all documents from 'tweets' index.

        :return: list of documents with _id and fields
        """
        res = self.es.search(index="tweets", query={"match_all": {}}, size=10000)
        docs = [{"_id": hit["_id"], **hit["_source"]} for hit in res["hits"]["hits"]]
        return docs

    def delete_irrelevant_documents(self):
        """
        Delete documents that are NOT antisemitic and do not contain weapons.

        :return: Elasticsearch response
        """
        weapons_list = DataLoader.load_black_list()
        res = self.es.delete_by_query(
            index="tweets",
            body={
                "bool": {
                    "must": [{"term": {"Antisemitic": False}}],
                    "must_not": [{"bool": {"should": [{"terms": {"weapons": weapons_list}}]}}],
                }
            },
        )
        return res

    def get_doc_ids_with_weapon(self, weapon: str):
        """
        Return list of document IDs where 'text.raw' == weapon.
        """
        res = self.es.search(
            index="tweets",
            body={
                "query": {
                    "match": {"text": weapon}
                }
            },
            size=10000
        )
        return [hit["_id"] for hit in res["hits"]["hits"]]


    def update_sentiment_field(self, docs):
        """
        Bulk update documents with sentiment field.

        :param docs: list of documents with _id and sentiment
        """
        actions = [
            {
                "_op_type": "update",
                "_index": "tweets",
                "_id": doc["_id"],
                "doc": {"sentiment": doc["sentiment"]},
            }
            for doc in docs
        ]
        helpers.bulk(self.es, actions)
        print(f"Updated {len(docs)} documents successfully")



    def update_weapons_field(self, weapons_dict):
        """
        Update the 'weapons' field in each document
        with the detected weapons.
        """
        actions = []

        for weapon, ids in weapons_dict.items():
            for doc_id in ids:
                actions.append({
                    "_op_type": "update",
                    "_index": "tweets",
                    "_id": doc_id,
                    "script": {
                        "source": "ctx._source.weapons.add(params.weapon)",
                        "lang": "painless",
                        "params": {"weapon": weapon}
                    }
                })

        helpers.bulk(self.es, actions)
        print(f"Updated weapons for {sum(len(ids) for ids in weapons_dict.values())} documents")



    def get_antisemitic_tweets_with_weapons(self):
        """Retrieve antisemitic tweets that mention at least one weapon (TODO)."""
        pass

    def get_sensitive_tweets_with_two_weapons(self):
        """Retrieve sensitive tweets with at least two weapons (TODO)."""
        pass
