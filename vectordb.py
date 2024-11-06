import pandas as pd
import yaml
import os
import shutil
import logging
from langchain_ollama import OllamaEmbeddings
from langchain_core.documents import Document
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.vectorstores import FAISS
from sqldb import SQLDBOperator
import faiss
from langchain_chroma import Chroma

class BaseVectorDB:
    def add_documents(self, docs: list[Document]):
        raise NotImplementedError

    def delete_documents(self, doc_ids: list[int]):
        raise NotImplementedError
    
    def search_documents(self, query: str, k: int=5, filter: dict=None, score_type: str="relevance", score_threshold: float=None):
        raise NotImplementedError
    
    def save(self):
        raise NotImplementedError
    
    def reset(self):
        raise NotImplementedError


class FAISSVectorDB(BaseVectorDB):
    def __init__(self, embeddings, config):
        self.config = config
        index = faiss.IndexFlatL2(len(embeddings.embed_query("hello world")))
        self.embeddings = embeddings
        self.faiss_db = FAISS(
            embedding_function=embeddings,
            index=index,
            docstore=InMemoryDocstore(),
            index_to_docstore_id={}
        )
        # Load the faiss db if it exists
        if os.path.exists(config['paths']['faiss_db']):
            if "index.faiss" in os.listdir(config['paths']['faiss_db']):
                self.faiss_db = self.faiss_db.load_local(config['paths']['faiss_db'], embeddings=embeddings, allow_dangerous_deserialization=True)

    def add_documents(self, docs: list[Document]):
        return self.faiss_db.add_documents(docs)

    def delete_documents(self, doc_ids: list[str]):
        return self.faiss_db.delete(doc_ids)
    
    def search_documents(self, query: str, k: int = 5, filter: dict = None, score_type: str = "relevance", score_threshold: float = None):
        if score_type == "relevance":
            return self.faiss_db.similarity_search_with_relevance_scores(query, k=k, filter=filter, score_threshold=score_threshold)
        elif score_type == "distance":
            return self.faiss_db.similarity_search_with_score(query, k=k, filter=filter)
    
    def get_by_ids(self, doc_ids: list[str]):
        return [self.faiss_db.docstore._dict.get(id, None) for id in doc_ids]
    
    def save(self):
        self.faiss_db.save_local(self.config['paths']['faiss_db'])
    
    def reset(self):
        if os.path.exists(self.config['paths']['faiss_db']):
            shutil.rmtree(self.config['paths']['faiss_db'])
            os.mkdir(self.config['paths']['faiss_db'])
            print("Deleted existing faiss db")
        
        # Reinitialize the faiss db
        self.faiss_db = FAISS(
            embedding_function=self.embeddings,
            index=faiss.IndexFlatL2(len(self.embeddings.embed_query("hello world"))),
            docstore=InMemoryDocstore(),
            index_to_docstore_id={}
        )


class ChromaVectorDB(BaseVectorDB):
    def __init__(self, embeddings, config):
        self.config = config
        self.chroma_db = Chroma(persist_directory=config['paths']['chroma_db'], embedding_function=embeddings)
        
    def add_documents(self, docs: list[Document]):
        return self.chroma_db.add_documents(docs)

    def delete_documents(self, doc_ids: list[str]):
        return self.chroma_db.delete(doc_ids)
    
    def search_documents(self, query: str, k: int = 5, filter: dict = None, score_type: str = "relevance", score_threshold: float = None):
        if score_type == "relevance":
            res = self.chroma_db.similarity_search_with_relevance_scores(query, k=k, filter=filter, score_threshold=score_threshold)
        elif score_type == "distance":
            res = self.chroma_db.similarity_search_with_score(query, k=k, filter=filter)
        return res
    
    def get_doc_ids(self, filter: dict):
        res = self.chroma_db.get(where=filter)
        return res['ids']
    
    def get_by_ids(self, doc_ids: list[str]):
        return self.chroma_db.get_by_ids(doc_ids)
    
    def save(self):
        pass
    
    def reset(self):
        # clear the persistent directory
        if os.path.exists(self.config['paths']['chroma_db']):
            shutil.rmtree(self.config['paths']['chroma_db'])
            os.mkdir(self.config['paths']['chroma_db'])
            print("Deleted existing chroma db")
        self.chroma_db = Chroma(persist_directory=self.config['paths']['chroma_db'], embedding_function=self.embeddings)


class VectorDBOperator:
    def __init__(self, sql_operator: SQLDBOperator, vector_db_type="chroma"):
        self.sql_operator = sql_operator  # Store the SQLDBOperator instance
        self.config = self.load_config('config.yaml')
        self.embeddings = OllamaEmbeddings(model=self.config['embed_model'])
        self.top_k = self.config['semantic_search_k']
        self.tbl_name = 'item'
        
        # select the vector database type
        if vector_db_type == "faiss":
            self.vector_db = FAISSVectorDB(self.embeddings, self.config)
        elif vector_db_type == "chroma":
            self.vector_db = ChromaVectorDB(self.embeddings, self.config)
        
        # Set up logging
        logging.basicConfig(filename=self.config['paths']['logging_file'], level=logging.INFO)

    def load_config(self, file_path):
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def create_documents(self, data: list[dict]):
        """ Create documents from the data list. """
        docs = []
        for entry in data[::-1]:
            title = entry.get("title", "")
            content = entry.get("content", "")
            id = entry['item_id']
            
            # Create a Document instance with the current entry's data
            doc = Document(
                id = id,
                page_content=title+" "+content,
                metadata={"item_id": id}
            )
            docs.append(doc)
        return docs
    
    def get_id_by_doc(self, docs: list[Document]):
        return [str(doc.metadata['item_id']) for doc in docs]
    
    def get_doc_by_id(self, doc_ids: list[str]):
        return self.vector_db.get_by_ids(doc_ids)
    
    def init_vector_db(self):
        """Initialize the vector database with the data from the SQL database."""
        
        # Clear the vector database
        self.vector_db.reset()
        
        # Load data from sql database
        print(f"Loading data from item table in SQL database")
        result = self.sql_operator.select(self.tbl_name, '*', None)
        if result['status'] == 1:
            data = result['data']
            data = self.sql_operator.object_list_as_dict(data)
            docs = self.create_documents(data)
            self.vector_db.add_documents(docs)
            logging.info(f"Data from {self.tbl_name} inserted into the vector database")
        else:
            logging.error(f"Error fetching data from {self.tbl_name}: {result['message']}")
        
        self.vector_db.save()
        logging.info("Vector database initialized")
        print("Vector database initialized")
        
        
    def insert(self, docs: list[Document]):
        """Insert data into the vector database."""
        self.vector_db.add_documents(docs)
        self.vector_db.save()
        logging.info("Documents inserted into the vector database")
        print("Documents inserted into the vector database")
    
    def delete(self, doc_ids: list[str]):
        """Delete documents from the vector database."""
        self.vector_db.delete_documents(doc_ids)
        self.vector_db.save()
        logging.info("Documents deleted from the vector database")
        print("Documents deleted from the vector database")
    
    def update(self, doc_ids, new_data):
        """Update documents in the vector database."""
        self.delete(doc_ids)
        self.insert(new_data)
        self.vector_db.save()
        logging.info("Documents updated in the vector database")
        print("Documents updated in the vector database")
    
    def search(self, query: str):
        """Search for similar documents in the vector database."""
        results = self.vector_db.search_documents(query, k=self.top_k, score_threshold=0.3)
        return results
    
        
# Example usage
if __name__ == "__main__":
    
    db_operator = SQLDBOperator()
    vector_db_operator = VectorDBOperator(db_operator, vector_db_type="faiss")
    
    # test initialize
    # vector_db_operator.init_vector_db()
    
    # test search
    # query = "what are the meetings scheduled for tomorrow"
    # query = "meeting"
    # results = vector_db_operator.search(query)
    # print(results)
    
    # test delete
    # query = "meeting"
    # results = vector_db_operator.search(query)
    # doc_to_delete = results[0][0]
    # print(doc_to_delete)
    # doc_id_to_delete = vector_db_operator.get_doc_ids([doc_to_delete])
    # vector_db_operator.delete(doc_id_to_delete)
    
    # test update
    # query = "meeting"
    # results = vector_db_operator.search(query)
    # doc_to_update = results[0][0]
    # print(doc_to_update)
    # doc_id_to_update = vector_db_operator.get_doc_ids([doc_to_update])
    # print(doc_id_to_update)
    # new_data = [
    #     {
    #         "item_id": 1,
    #         "title": "Updated meeting",
    #         "content": "Updated meeting content"
    #     }
    # ]
    # vector_db_operator.update(doc_id_to_update, new_data)
    
    # test insert
    # new_data = [
    #     {
    #         "item_id": 3,
    #         "title": "New meeting",
    #         "content": "New meeting content"
    #     }
    # ]
    # docs = vector_db_operator.create_documents(new_data)
    # vector_db_operator.insert(docs)
    
    # test get by id
    doc_ids = ["1", "5", "10"]
    res = vector_db_operator.get_doc_by_id(doc_ids)
    print(res)