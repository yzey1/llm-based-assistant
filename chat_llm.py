import yaml
import os 
from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

class ChatLLM:
    def __init__(self, input_query, operation_type=None, task_type=None, sql_response=None, history=None):
        self.config = self.load_config('config.yaml')
        self.llm = OllamaLLM(model=self.config['chat_llm_model'], top_p=0.6)
        self.input_query = input_query
        self.operation_type = operation_type
        self.task_type = task_type
        self.sql_response = sql_response
        self.history = history if history is not None else []  # Initialize history if None
        self.setup_prompts()
    
    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    
    def setup_prompts(self):
        # chat: normal chat without any specific task
        # success: chat with successful SQL operation
        # fail: chat with failed SQL operation
        
        if not self.operation_type or not self.task_type:
            self.mode = "chat"
            self.prompt = ChatPromptTemplate.from_messages([
                ("system", self.generate_chat_template()),
                MessagesPlaceholder(variable_name="chat_history"),
                ("human", "{input}")
            ])
        elif self.sql_response["status"] == 1:
            self.mode = "success"
            self.prompt = ChatPromptTemplate.from_messages([
                ("system", self.generate_success_template()),
                MessagesPlaceholder(variable_name="chat_history"),
                ("human", f"User query: {self.input_query}")
            ])
        elif self.sql_response["status"] == 0:
            self.mode = "fail"
            self.prompt = ChatPromptTemplate.from_messages([
                ("system", self.generate_fail_template()),
                MessagesPlaceholder(variable_name="chat_history"),
                ("human", f"User query: {self.input_query}")
            ])
        
    def generate_chat_template(self):
        return f"""
        You are now in chat mode. You can chat with user about anything.
        """
    
    def generate_success_template(self):
        return f"""
        You are an assistant to help users manage their notes, events, and schedules.
        Now you are in {self.task_type} task with {self.operation_type} operation.
        You have got the following response from the SQL Database: {self.sql_response["message"]}
        Reply to user in a brief and human-friendly way.
        """
    
    def generate_fail_template(self):
        return f"""
        You are an assistant to help users manage their schedules and memos.
        Now you are in {self.task_type} task with {self.operation_type} operation.
        The SQL operation failed with the following error: {self.sql_response["message"]}
        Ask user to fix the query based on the error, or provide missing information.
        Ask in a brief and human-friendly way.
        """
    
    def generate_response(self, input_query):
        chain = self.prompt | self.llm
        return chain.invoke({"input": input_query, "chat_history": self.history})

if __name__ == "__main__":
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    
    input_query = "Can you update the schedule for the project kickoff?"
    chat_llm = ChatLLM(input_query, "update", "schedule", {"status": 1, "response": "Updated the schedule for the project kickoff."})
    print(chat_llm.generate_response("Can you update the schedule for the project kickoff?"))
    
    input_query = "Please remove my appointment from the schedule."
    chat_llm = ChatLLM(input_query, "delete", "schedule", {"status": 0, "message": "Error deleting the appointment."})
    print(chat_llm.generate_response(input_query))
    
    input_query = "Are you happy today?"
    chat_llm = ChatLLM(input_query, None, None, None)
    print(chat_llm.generate_response(input_query))
    
    print("Done")