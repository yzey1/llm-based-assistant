import pandas as pd
import yaml
import os
import shutil
import logging
from langchain_ollama import OllamaEmbeddings
from langchain_core.documents import Document
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.vectorstores import FAISS
from sql_db_operator import SQLDBOperator
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


class FAISSVectorDB(BaseVectorDB):
    def __init__(self, embeddings, config):
        index = faiss.IndexFlatL2(len(embeddings.embed_query("hello world")))
        self.embeddings = embeddings
        self.faiss_db = FAISS(
            embedding_function=embeddings,
            index=index,
            docstore=InMemoryDocstore(),
            index_to_docstore_id={}
        )
        if os.path.exists(config['paths']['faiss_db']):
            if "index.faiss" in os.listdir(config['paths']['faiss_db']):
                self.faiss_db = self.faiss_db.load_local(config['paths']['faiss_db'], embeddings=embeddings, allow_dangerous_deserialization=True)
        self.config = config

    def add_documents(self, docs: list[Document]):
        return self.faiss_db.add_documents(docs)

    def delete_documents(self, doc_ids: list[str]):
        return self.faiss_db.delete(doc_ids)
    
    def search_documents(self, query: str, k: int = 5, filter: dict = None, score_type: str = "relevance", score_threshold: float = None):
        if score_type == "relevance":
            return self.faiss_db.similarity_search_with_relevance_scores(query, k=k, filter=filter)
        elif score_type == "distance":
            return self.faiss_db.similarity_search_with_score(query, k=k, filter=filter)
    
    def save(self):
        self.faiss_db.save_local(self.config['paths']['faiss_db'])
    
    def reset(self):
        if os.path.exists(self.config['paths']['faiss_db']):
            shutil.rmtree(self.config['paths']['faiss_db'])
            os.mkdir(self.config['paths']['faiss_db'])
            print("Deleted existing faiss db")
            
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
        # select the vector database type
        if vector_db_type == "faiss":
            self.vector_db = FAISSVectorDB(self.embeddings, self.config)
        elif vector_db_type == "chroma":
            self.vector_db = ChromaVectorDB(self.embeddings, self.config)
        
        self.top_k = self.config['semantic_search_k']
        # Set up logging
        logging.basicConfig(filename=self.config['paths']['logging_file'], level=logging.INFO)

    def load_config(self, file_path):
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def create_documents(self, data: list[dict], tbl_name: str):
        """ Create documents from the data list. """
        
        # Get the primary key for the specified table
        tbl_pk = self.sql_operator.get_pk(tbl_name)[0]
        docs = []

        for entry in data[::-1]:
            if "title" not in entry and "content" not in entry: continue
            title = entry.get("title", "")
            content = entry.get("content", "")
            pkid = entry.get(tbl_pk, None)
            doc_id = tbl_name + ',' + str(pkid)
            
            # Create a Document instance with the current entry's data
            doc = Document(
                id = doc_id,
                page_content=title+" "+content,
                metadata={"doc_id": doc_id, "tbl": tbl_name, tbl_pk: pkid}
            )
            
            # Append the created document to the list
            docs.append(doc)

        return docs
    
    def get_doc_ids(self, docs: list[Document]):
        return [doc.metadata['tbl'] + "," + str(doc.metadata[self.sql_operator.get_pk(tbl)[0]]) for doc in docs]
    
    def init_vector_db(self, tbl_names: list[str]):
        """Initialize the vector database with the data from the SQL database."""
        
        # Clear the vector database
        self.vector_db.reset()
        
        # Load data from sql database
        for tbl_name in tbl_names:
            print(f"Loading data from {tbl_name}")
            result = self.sql_operator.select(tbl_name, '*', None)
            if result['status'] == 1:
                data = result['data']
                data = self.sql_operator.object_list_as_dict(data)
                docs = self.create_documents(data, tbl_name)
                self.vector_db.add_documents(docs)
                logging.info(f"Data from {tbl_name} inserted into the vector database")
            else:
                logging.error(f"Error fetching data from {tbl_name}: {result['message']}")
        
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
    
    def update(self, docs: list[Document]):
        """Update documents in the vector database."""
        doc_ids = self.get_doc_ids(docs)
        self.delete(doc_ids)
        self.insert(docs)
        self.vector_db.save()
        logging.info("Documents updated in the vector database")
        print("Documents updated in the vector database")
    
    def search(self, query: str, table_name: str=None):
        """Search for similar documents in the vector database."""
        if table_name:
            results = self.vector_db.search_documents(query, k=self.top_k, filter={"tbl_name": table_name}, score_threshold=0.3)
        else:
            results = self.vector_db.search_documents(query, k=self.top_k, score_threshold=0.3)
        return results
    
        
# Example usage
if __name__ == "__main__":
    tbl_names = ["event", "memo"]
    
    db_operator = SQLDBOperator()
    vector_db_operator = VectorDBOperator(db_operator, vector_db_type="faiss")
    vector_db_operator.init_vector_db(tbl_names)
    
    # # Example search
    query = "what are the meetings scheduled for tomorrow"
    table_name = "event"
    results = vector_db_operator.search(query, table_name)
    print(results)
    doc_to_delete = results[0][0]
    print(doc_to_delete)
    vector_db_operator.delete([doc_to_delete])
    
    