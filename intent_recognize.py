import re
import yaml
import logging
import pandas as pd
from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate

logging.basicConfig(level=logging.INFO)

class IntentRecognizer:
    def __init__(self, input_query):
        self.config = self.load_config('config.yaml')
        self.llm = OllamaLLM(model=self.config["intent_llm_model"])
        self.tables_set = set(self.config['tables'])
        self.op_types = self.config['operation_types']
        self.input_query = input_query
        self.setup_prompts()

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def setup_prompts(self):
        
        self.operation_prompt = ChatPromptTemplate.from_messages([
            ("system", self.generate_operation_template()),
            ("human", "{input}")
        ])
        
        self.table_prompt = ChatPromptTemplate.from_messages([
            ("system", self.generate_table_template()),
            ("human", "{input}")
        ])
        


    def generate_table_template(self):
        # return f"""
        # ### Examples:
        # Query: 'Please remove my appointment from the schedule.'
        # Table Name: Schedules

        # Query: 'Update the weekly team meeting to 11 AM.'
        # Table Name: RecurringSchedules

        # Query: 'Create a note for my presentation next week.'
        # Table Name: Memos

        # ### Instructions:
        # Determine whether the user query is related to one of the tables: [{', '.join(self.tables_set)}]
        # (Schedules: happens once, RecurringSchedules: happens repeatedly, Memos: notes)
        # Reply with only the table name or "False" if no relevant table is found.
        # Query: {{input}}
        # Table Name: 
        # """
        return f"""
        ### Instructions:
        Determine whether the user query is related to one of the tables: [{', '.join(self.tables_set)}]
        (Schedules: happens once, RecurringSchedules: happens repeatedly, Memos: notes)
        Reply with only the table name or "False" if no relevant table is found.
        Query: {{input}}
        Table Name: 
        """
        
        
    def generate_operation_template(self):
        return f"""
        ### Examples:
        Query: 'Please delete my appointment.'
        Operation Type: delete

        Query: 'Update the team meeting to 11 AM.'
        Operation Type: update

        Query: 'Create a note for my presentation next week.'
        Operation Type: create

        Query: 'Can you show my upcoming events?'
        Operation Type: search

        ### Instructions:
        Determine whether the user query is related to one of the operation types: [{', '.join(self.op_types)}]
        Reply with only the operation type or "False" if no relevant operation is found.
        Query: {input}
        Operation Type:
        """
    
    def check_table_relevance(self):
        chain = self.table_prompt | self.llm
        return self.extract_valid_answer(chain, valid_set=self.tables_set)

    def identify_operation_type(self):
        chain = self.operation_prompt | self.llm
        return self.extract_valid_answer(chain, valid_set=self.op_types)

    def extract_valid_answer(self, chain, valid_set=None, valid_threshold=3, max_attempts=5):
        """Extracts a valid answer, ensuring consistency across multiple attempts."""
        answer_counts = {}
        attempts = 0
        
        while attempts < max_attempts:
            response = chain.invoke({"input": self.input_query}).strip()
            logging.info(f"Response: {response}")
            words = response.split()
            valid_answers = set([word for word in words if not valid_set or word in valid_set])
            
            if len(valid_answers) == 1:
                answer = valid_answers.pop()
                answer_counts[answer] = answer_counts.get(answer, 0) + 1
                
                if answer_counts[answer] >= valid_threshold: # Found a valid answer twice
                    return answer
            
            # Increment attempts
            attempts += 1

        return None  # Return None if no consistent valid answer is found after max attempts

    def identify_intents(self):
        table = self.check_table_relevance()
        if not table:
            return False, None  # No relevance, skip further steps
        operation_type = self.identify_operation_type()
        return table, operation_type

if __name__ == "__main__":
    
    # input_query = "Can you show the meetings for last month?"
    input_query = "Please add a weekly team meeting every monday."
    recognizer = IntentRecognizer(input_query)
    operation_type, table_name = recognizer.identify_intents()

    print(f"Table Name: {operation_type}")
    print(f"Operation Type: {table_name}")
    