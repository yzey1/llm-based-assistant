import os
import logging
import yaml
from langchain_ollama import OllamaEmbeddings
from langchain_community.vectorstores import FAISS
from db_utils import DBOperator

# 设置日志
logging.basicConfig(level=logging.INFO)

class SemanticSearch:
    def __init__(self):
        self.config = self.load_config('config.yaml')
        self.top_k = self.config['semantic_search_k']
        self.db_manager = DBOperator()
        self.embeddings = OllamaEmbeddings(model=self.config['embed_model'])
        self.faiss_db = FAISS.load_local("faiss", self.embeddings, allow_dangerous_deserialization=True)

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def semantic_search(self, input_query, intent_table):
        tbl_pk = self.db_manager.get_pk(intent_table)

        if tbl_pk is None:
            logging.error(f"Primary Key of {intent_table} Not Found")
            return []

        retrieved_docs = self.faiss_db.similarity_search_with_relevance_scores(input_query, 
                                                         filter={"tbl_name": intent_table}, 
                                                         k=self.top_k)
        retrieved_id = [doc[0].metadata[tbl_pk] for doc in retrieved_docs if doc[1]>0.3]

        return retrieved_id

if __name__ == "__main__":
    # Initialize SemanticSearch
    semantic_search = SemanticSearch()

    # Examples
    input_query = "what did i plan to do tomorrow."
    intent_table = "Schedules"
    retrieved_ids = semantic_search.semantic_search(input_query, intent_table)

    if retrieved_ids:
        logging.info(f"Primary key of retrieved records: {retrieved_ids}")
    else:
        logging.info("No records found")