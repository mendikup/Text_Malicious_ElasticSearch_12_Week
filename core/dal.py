import json
from loader import DataLoader
from elasticsearch import Elasticsearch, helpers
from utils.simple_logger import logger


class Dal:
    """Data Access Layer (DAL) for interacting with Elasticsearch."""

    def __init__(self):
        """Initialize connection to Elasticsearch server."""
        self.es = Elasticsearch('http://localhost:9200')
        if self.es.ping():
            logger.info("Connected to Elasticsearch successfully")
        else:
            logger.error("Failed to connect to Elasticsearch")

    def map_and_index_data(self, df, index_name: str):
        """Create mapping for the index and index the given dataframe."""
        docs = df.to_dict(orient="records")
        logger.info(f"Indexing {len(docs)} documents to {index_name}")

        # Delete existing index
        self.es.indices.delete(index=index_name, ignore=[400, 404])
        logger.info(f"Deleted existing index: {index_name}")

        # Create index with mapping
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
        logger.info(f"Created index with mapping: {index_name}")

        # Bulk index documents
        actions = [
            {"_op_type": "index", "_index": index_name, "_source": doc}
            for doc in docs
        ]

        success, errors = helpers.bulk(self.es, actions, stats_only=False, raise_on_error=False)
        logger.info(f"Indexed {success} documents successfully")
        if errors:
            logger.error(f"Indexing had {len(errors)} errors")

    def get_all_data(self):
        """Retrieve all documents from 'tweets' index."""
        logger.info("Retrieving all documents from tweets index")

        res = self.es.search(index="tweets", query={"match_all": {}}, size=10000)
        docs = [{"_id": hit["_id"], **hit["_source"]} for hit in res["hits"]["hits"]]

        logger.info(f"Retrieved {len(docs)} documents")

        # simple check
        with_text = len([d for d in docs if d.get("text")])
        with_sentiment = len([d for d in docs if d.get("sentiment")])

        logger.info(f"Documents with text: {with_text}, with sentiment: {with_sentiment}")

        return docs

    def update_sentiment_field(self, docs):
        """Bulk update documents with sentiment field."""
        logger.info(f"Updating sentiment field for {len(docs)} documents")

        actions = []
        for doc in docs:
            if "_id" in doc and "sentiment" in doc:
                actions.append({
                    "_op_type": "update",
                    "_index": "tweets",
                    "_id": doc["_id"],
                    "doc": {"sentiment": doc["sentiment"]},
                })

        logger.info(f"Prepared {len(actions)} update actions")

        if actions:
            success, errors = helpers.bulk(self.es, actions, stats_only=False, raise_on_error=False)
            logger.info(f"Updated {success} documents successfully")
            if errors:
                logger.error(f"Update had {len(errors)} errors")
                # הצג כמה שגיאות לדוגמה
                for error in errors[:3]:
                    logger.error(f"Update error: {error}")

    def update_weapons_field(self, weapons_dict):
        """Update the 'weapons' field in each document with the detected weapons."""
        total_updates = sum(len(ids) for ids in weapons_dict.values())
        logger.info(f"Updating weapons field for {total_updates} documents")

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

        if actions:
            success, errors = helpers.bulk(self.es, actions, stats_only=False, raise_on_error=False)
            logger.info(f"Updated weapons for {success} documents")
            if errors:
                logger.error(f"Weapons update had {len(errors)} errors")

    def get_doc_ids_with_weapon(self, weapon: str):
        """Return list of document IDs where 'text' matches weapon."""
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