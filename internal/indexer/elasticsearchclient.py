from elasticsearch import Elasticsearch
from dataclasses import dataclass

PAGE_MAPPING = {
    "mappings": {
        "properties": {
            "url":       {"type": "keyword"},
            "title":     {"type": "text"},
            "body":      {"type": "text"},
            "timestamp": {"type": "date"},
        }
    }
}

class ESClient:
    def __init__(self, host, index, username=None, password=None):
        self.client = self.init_connection(host, username, password)
        self.index = index

    def init_connection(self, host, username, password):
        if username and password:
            return Elasticsearch(host, basic_auth=(username, password))
        return Elasticsearch(host)

    def ensure_index(self):
        if not self.client.indices.exists(index=self.index):
            self.client.indices.create(index=self.index, body=PAGE_MAPPING)
            print(f"Created index '{self.index}'")
        else:
            print(f"Index '{self.index}' already exists")
    
    def post_to_index(self, doc: "Indexed_Page") -> bool:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.client.index(index=self.index, body={
                    "url": doc.url,
                    "title": doc.title,
                    "body": doc.body,
                    "timestamp": doc.timestamp,
                })
                return True
            except Exception as e:
                print(f"Failed to index {doc.url} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    return False



@dataclass
class Indexed_Page:
    url: str
    title: str
    body: str
    timestamp: str