import os
import logging
import yaml
from langchain_ollama.llms import OllamaLLM
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from db_utils import DBOperator

# 设置日志
logging.basicConfig(level=logging.INFO)

class TextToSQL:
    def __init__(self):
        self.config = self.load_config('config.yaml')
        self.db_manager = DBOperator()
        self.llm = OllamaLLM(model=self.config['text2sql_llm_model'])
        self.setup_prompt()

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
        
    def setup_prompt(self):
        
        sys_prompt = """Convert the user request to a SQL query. 
        The table schema is as follows:{schema}
        """

        human_prompt = """
        
        Instructions:
        - Check if the user request can be filter by time-format field.
        - If yes, generate a SQL query directly to {intent_operation} data by time.
        - If no, generate a SQL query to {intent_operation} on records whose {pk} in {retrieved_ids}.
        
        User request: {user_query}.
        No preamble and explanation. Just the SQL query.
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
        return self.db_manager.run(sql_statement)

def main():
    # 初始化 TextToSQL
    text_to_sql = TextToSQL()

    # 示例查询
    input_query = "Please remove my appointment from the schedule."
    intent_operation = "delete"
    intent_table = "Schedules"
    retrieved_ids = [1, 2, 3]

    sql_statement = text_to_sql.convert_to_sql(input_query, intent_operation, intent_table, retrieved_ids)

    if sql_statement:
        logging.info(f"生成的 SQL 语句: {sql_statement}")
        response = text_to_sql.execute_sql(sql_statement)
        if response:
            logging.info(f"查询结果: {response}")
        else:
            logging.info("未找到匹配的记录")
    else:
        logging.info("未生成有效的 SQL 语句")

if __name__ == "__main__":
    main()