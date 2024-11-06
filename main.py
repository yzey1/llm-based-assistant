import time
import os
import gradio as gr
from sqldb import SQLDBOperator
from vectordb import VectorDBOperator
from intent import IntentRecognizer
from text2sql import TextToSQL
from chat_llm import ChatLLM


# decorator for timing
def timing(func):
    def wrapper(*args, **kwargs):
        start_t = time.time()
        result = func(*args, **kwargs)
        end_t = time.time()
        print(f"{func.__name__} finished, time elapsed: {end_t - start_t}")
        return result
    return wrapper

@timing
def get_intent(input_query):
    recognizer = IntentRecognizer(input_query)
    task_type, operation_type, info = recognizer.get_intents()
    return task_type, operation_type, info

@timing
def semantic_search(input_query, vector_db_operator):
    retrieval = vector_db_operator.search(input_query)
    relevant_docs = [record[0] for record in retrieval]
    relevant_record_id = vector_db_operator.get_id_by_doc(relevant_docs)
    return relevant_record_id

@timing
def manipulate_database(operation_type: str, task_type: str, info: dict, relevant_record_id: list, db_operator: SQLDBOperator, vector_db_operator: VectorDBOperator):
    if operation_type == "insert":
        new_item = db_operator.create_item(info)
        new_doc = vector_db_operator.create_documents([new_item])
        vector_db_operator.insert(new_doc)
        return {"status": "success", "message": "Item inserted successfully"}
    else:
        search_filter = {"item_id": relevant_record_id, **info}
        if "content" in search_filter:
            search_filter.pop("content")
        sql_search_response = db_operator.get_items(**search_filter)
        match_items = sql_search_response['data']
        reponsed_items_id = [record['item_id'] for record in match_items]
        
        if operation_type == "delete":
            sql_response = db_operator.delete_items(reponsed_items_id)
            vector_db_operator.delete(reponsed_items_id)
            return sql_response
        elif operation_type == "update":
            sql_response = db_operator.update_items(reponsed_items_id, info)
            vector_db_operator.update(reponsed_items_id, info)
            return sql_response
        elif operation_type == "search":
            # convert match_items to a human-readable string
            match_items_str = "; ".join([", ".join([f"{k}: {v}" for k, v in item.items()]) for item in match_items])
            sql_search_response["message"] = match_items_str
            return sql_search_response

@timing
def generate_response(input_query, operation_type, task_type, sql_response, history=None):
    chat_llm = ChatLLM(input_query, operation_type, task_type, sql_response, history)
    response = chat_llm.generate_response(input_query)
    return response

@timing
def main(input_query, history=None):
    print("Inferencing...")
    
    # initialize db operators
    db_operator = SQLDBOperator()
    vector_db_operator = VectorDBOperator(db_operator, vector_db_type="faiss")
    
    # step 1: get intent
    task_type, operation_type, info = get_intent(input_query)
    if not task_type or not operation_type:
        # skip 2-3, directly return response
        return generate_response(input_query, operation_type, task_type, None, history)
    print(f"Task: {task_type}, Operation: {operation_type}, Info: {info}")
    print("====================================")
    
    # step 2: semantic search
    relevant_record_id = semantic_search(input_query, vector_db_operator)
    print(f"Relevant item: {relevant_record_id}")
    print("====================================")
    
    # step 3: manipulate Database
    sql_response = manipulate_database(operation_type, task_type, info, relevant_record_id, db_operator, vector_db_operator)
    print(f"SQL Response: {sql_response}")
    print("====================================")
    
    # step 4: generate response
    response = generate_response(input_query, operation_type, task_type, sql_response, history)
    print(response)
    print("====================================")
    
    return response

# Gradio chat interface
def gradio_interface(user_input, history):
    # Get the response from the main function
    response = main(user_input, history)
    
    return response

# Create the Gradio interface
iface = gr.ChatInterface(fn=gradio_interface, type="messages")

if __name__ == "__main__":
    # iface.launch()
    input = "show me recent meetings"
    response = main(input)
    print(response)