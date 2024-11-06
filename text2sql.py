import os
import logging
import yaml
import re
from langchain_ollama.llms import OllamaLLM
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from sqldb import SQLDBOperator
from vectordb import VectorDBOperator

os.environ["LANGCHAIN_TRACING_V2"] = "false"
logging.basicConfig(level=logging.INFO)

class TextToSQL:
    def __init__(self, input_query, operation_type=None, task_type=None):
        self.config = self.load_config('config.yaml')
        self.db_manager = SQLDBOperator()
        self.llm = OllamaLLM(model=self.config['text2sql_llm_model'], temperature=0.6)
        self.input_query = input_query
        self.operation_type = operation_type
        self.task_type = task_type
        self.setup_prompt()

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    
    def get_db_schema(self):
        if self.task_type == "memo":
            return """
            CREATE TABLE `memo` (
                    memo_id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(100) DEFAULT NULL,                        -- Optional title for the memo
                    content TEXT NOT NULL,                                  -- Content of the memo
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,        -- Timestamp when record is created
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Timestamp when record is updated
                );
            """
        elif self.task_type == "schedule":
            return """
            CREATE TABLE `event` (
                event_id INT AUTO_INCREMENT PRIMARY KEY,               -- Unique identifier for the event
                title VARCHAR(100) NOT NULL,                           -- Title of the event
                content TEXT,                                      -- Detailed description of the event
                recurrence_pattern ENUM('DAILY', 'WEEKLY', 'MONTHLY') DEFAULT NULL,  -- Recurrence pattern
                recurrence_rule VARCHAR(10) DEFAULT NULL,              -- Specific rule for recurrence (e.g., day of week/month)
                location VARCHAR(255) DEFAULT NULL,                   -- Location of the event (optional)
                status ENUM('ACTIVE', 'CANCELLED', 'COMPLETED') DEFAULT 'ACTIVE', -- Status of the event
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,       -- Record creation timestamp
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Record update timestamp
            );
            CREATE TABLE `schedule` (
                schedule_id INT AUTO_INCREMENT PRIMARY KEY,            -- Unique identifier for the schedule
                event_id INT NOT NULL,                                 -- Foreign key reference to Events
                start_time TIMESTAMP NOT NULL,                        -- Scheduled start time for the event
                end_time TIMESTAMP NOT NULL,                          -- Scheduled end time for the event
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,      -- Record creation timestamp
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Record update timestamp
                FOREIGN KEY (event_id) REFERENCES event(event_id) ON DELETE CASCADE -- Ensure referential integrity
            );
            """
    
    def setup_prompt(self):
        
        sql_keywords = {
            "create": "INSERT",
            "delete": "DELETE",
            "update": "UPDATE",
            "search": "SELECT"
        }
        db_schema = self.get_db_schema()
        clause_type = sql_keywords[self.operation_type]
        sys_prompt = f"""You are a SQL expert, convert the user query to {clause_type} SQL clause statement, given the following database schema:{db_schema}"""
        
        if self.task_type == "memo":
            human_prompt = """
                User Query: {user_query}.
                No preamble and explanation. Just the SQL.
                SQL statement:
                """
        elif self.task_type == "schedule":
            human_prompt = """
                ### Instructions:
                You should operate on two tables: `event` and `schedule`. Include columns as much as possible.
                User Query: {user_query}.
                No preamble and explanation. Just the SQL.
                SQL statement:
                """
                
        messages = [("system", sys_prompt), ("human", human_prompt)]
        self.prompt = ChatPromptTemplate.from_messages(messages)
        self.chain = self.prompt | self.llm

    def convert_to_sql(self):
        response = self.chain.invoke({"user_query":self.input_query})
        return self.extract_sql(response)
    
    def extract_sql(self, sql_text):
        # use regex to extract sql statements from the text
        pattern = r'```sql\n(.*?)\n```'
        sql_statements = re.findall(pattern, sql_text, re.DOTALL)
        if len(sql_statements) == 0:
            return sql_text
        else:
            return sql_statements[0] 

if __name__ == "__main__":
    # example 1
    # input_query = "i will have a meeting tomorrow 3 pm with my team, add to schedule"
    # operation_type = "create"
    # task_type = "schedule"
    
    # example 2 (search schedule)
    input_query = "show me my schedule for next week"
    operation_type = "search"
    task_type = "schedule"
    
    converter = TextToSQL(input_query, operation_type, task_type)
    sql_statement = converter.convert_to_sql()
    print(sql_statement)

    
    
# class TextToSQL:
#     def __init__(self, operation_type=None, task_type=None):
#         self.config = self.load_config('config.yaml')
#         self.db_manager = SQLDBOperator()
#         self.llm = OllamaLLM(model=self.config['text2sql_llm_model'],temperature=0.6)
#         self.setup_prompt()

#     def load_config(self, config_file):
#         with open(config_file, 'r') as file:
#             return yaml.safe_load(file)
        
#     def setup_prompt(self):
        
#         sys_prompt = """You are a SQL query generator. Given the following database schema:{schema}"""

#         human_prompt = """
#         ### Instructions:
#         The operation type is: {intent_operation}
#         The related data to consider is: {pk}: {retrieved_ids}
#         The table name is: {intent_table}
        
#         ### Your task:
#         User request: {user_query}.
#         No preamble and explanation. Just the SQL.
#         SQL statement:
#         """

#         messages = [("system", sys_prompt), ("human", human_prompt)]
#         self.prompt = ChatPromptTemplate.from_messages(messages)
#         self.chain = self.prompt | self.llm

#     def convert_to_sql(self, input_query, intent_operation, intent_table, retrieved_ids):
#         schema = self.db_manager.get_table_info(intent_table)
#         tbl_pk = self.db_manager.get_pk(intent_table)

#         if tbl_pk is None:
#             logging.error(f"PK for {intent_table} not found")
#             return None

#         sql_statement = self.chain.invoke({
#             "schema": schema,
#             "user_query": input_query,
#             "intent_operation": intent_operation,
#             "intent_table": intent_table,
#             "pk": tbl_pk,
#             "retrieved_ids": retrieved_ids
#         })

#         return sql_statement

#     def execute_sql(self, sql_statement):
#         return self.db_manager.run_sql_statement(sql_statement)
    

# if __name__ == "__main__":
#     # Initialize
#     text_to_sql = TextToSQL()

#     # Examples
#     input_query = "i will have a meeting tomorrow 3 pm with my team, add to schedule"
#     intent_operation = "insert"
#     intent_table = "Schedules"
#     retrieved_ids = [1, 2, 3]

#     sql_statement = text_to_sql.convert_to_sql(input_query, intent_operation, intent_table, retrieved_ids)

#     if sql_statement:
#         logging.info(f"Generatede SQL Statement: {sql_statement}")
#         response = text_to_sql.execute_sql(sql_statement)
#         if response:
#             logging.info(f"Respones: {response}")
#         else:
#             logging.info("SQL Statement not executed")
#     else:
#         logging.info("SQL Statement not valid")