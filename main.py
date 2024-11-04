from sql_db_operator import SQLDBOperator
from vector_db_operator import VectorDBOperator
from intent_recognize import IntentRecognizer
from text2sql import TextToSQL
from chat_llm import ChatLLM

def get_intent(input_query):
    recognizer = IntentRecognizer(input_query)
    return recognizer.get_intents()

def infer_process(input_query):
    
    # initialize
    db_operator = SQLDBOperator()
    vector_db_operator = VectorDBOperator(db_operator, vector_db_type="faiss")
    
    # step 1: get intent
    task_type, operation_type = get_intent(input_query)
    if not task_type or not operation_type:
        return None, None, None
    else:
        print(f"Task Type: {task_type}")
        print(f"Operation Type: {operation_type}")
    
    search_table_name = None
    if task_type == "schedule":
        search_table_name = "event"
    elif task_type == "memo":
        search_table_name = "memo"
    
    # step 2: semantic search
    relevant_docs = vector_db_operator.search(input_query, search_table_name)
    print(relevant_docs)
    
    # step 3: convert to SQL
    sql_converter = TextToSQL(input_query, operation_type, task_type)
    sql_statement = sql_converter.convert_to_sql()
    print(sql_statement)
    
    # step 4: execute SQL
    sql_response = db_operator.run_sql_statement(sql_statement, operation_type)
    print(sql_response)
    
    # step 5: manipulate vector db
    # if operation_type == "create":
    #     vector_db_operator.insert(input_query, search_table_name)
    # elif operation_type == "update":
    #     vector_db_operator.update(input_query, search_table_name)
    # elif operation_type == "delete":
    #     vector_db_operator.delete(input_query, search_table_name)
    # elif operation_type == "search":
    #     pass
        
    return task_type, operation_type, sql_response

def main(input_query=None):
    task_type, operation_type, sql_response = infer_process(input_query)
    chat_llm = ChatLLM(input_query, operation_type, task_type, sql_response)
    response = chat_llm.generate_response(input_query)
    print(response)
    
    return response

if __name__ == "__main__":
    
    input_query = "Can you show the meetings for next week?"
    # input_query = "Please remove my appointment from the schedule."
    # input_query = "Are you happy today?"
    
    res = main(input_query)
    # print(res)
    print("Done!")