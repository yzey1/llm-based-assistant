import os
import logging
import yaml
from langchain_ollama.llms import OllamaLLM
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from sql_db_operator import DBOperator

os.environ["LANGCHAIN_TRACING_V2"] = "false"
logging.basicConfig(level=logging.INFO)

class TextToSQL:
    def __init__(self):
        self.config = self.load_config('config.yaml')
        self.db_manager = DBOperator()
        self.llm = OllamaLLM(model=self.config['text2sql_llm_model'],temperature=0.6)
        self.setup_prompt()

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
        
    def setup_prompt(self):
        
        sys_prompt = """You are a SQL query generator. Given the following database schema:{schema}"""

        human_prompt = """
        ### Instructions:
        The operation type is: {intent_operation}
        The related data to consider is: {pk}: {retrieved_ids}
        The table name is: {intent_table}
        
        ### Your task:
        User request: {user_query}.
        No preamble and explanation. Just the SQL.
        SQL statement:
        """

        messages = [("system", sys_prompt), ("human", human_prompt)]
        self.prompt = ChatPromptTemplate.from_messages(messages)
        self.chain = self.prompt | self.llm

    def convert_to_sql(self, input_query, intent_operation, intent_table, retrieved_ids):
        schema = self.db_manager.get_table_info(intent_table)
        tbl_pk = self.db_manager.get_pk(intent_table)

        if tbl_pk is None:
            logging.error(f"PK for {intent_table} not found")
            return None

        sql_statement = self.chain.invoke({
            "schema": schema,
            "user_query": input_query,
            "intent_operation": intent_operation,
            "intent_table": intent_table,
            "pk": tbl_pk,
            "retrieved_ids": retrieved_ids
        })

        return sql_statement

    def execute_sql(self, sql_statement):
        return self.db_manager.run_sql_statement(sql_statement)
    

if __name__ == "__main__":
    # Initialize
    text_to_sql = TextToSQL()

    # Examples
    input_query = "i will have a meeting tomorrow 3 pm with my team, add to schedule"
    intent_operation = "insert"
    intent_table = "Schedules"
    retrieved_ids = [1, 2, 3]

    sql_statement = text_to_sql.convert_to_sql(input_query, intent_operation, intent_table, retrieved_ids)

    if sql_statement:
        logging.info(f"Generatede SQL Statement: {sql_statement}")
        response = text_to_sql.execute_sql(sql_statement)
        if response:
            logging.info(f"Respones: {response}")
        else:
            logging.info("SQL Statement not executed")
    else:
        logging.info("SQL Statement not valid")