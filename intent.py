import re
import os
import yaml
import logging
import json
import pandas as pd
from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate

class IntentRecognizer:
    def __init__(self, input_query):
        self.config = self.load_config('config.yaml')
        self.llm = OllamaLLM(model=self.config["intent_llm_model"], top_p=0.6)
        self.tasks = self.config['task_types']
        self.ops = self.config['operation_types']
        self.time_cols = ["content", "start_date", "start_time", "end_date", "end_time", "recurrence_pattern", "recurrence_rule", "search_time_frame"]
        self.input_query = input_query
        self.setup_prompts()

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def setup_prompts(self):
        
        self.task_prompt = ChatPromptTemplate.from_messages([
            ("system", self.get_task_template()),
            ("human", "{input}")
        ])
        
        self.operation_prompt = ChatPromptTemplate.from_messages([
            ("system", self.get_operation_template()),
            ("human", "{input}")
        ])
        
        self.schedule_prompt = ChatPromptTemplate.from_messages([
            ("system", self.extract_info_for_schedule_template()),
            ("human", "{input}")
        ])
        self.note_prompt = ChatPromptTemplate.from_messages([
            ("system", self.extract_info_for_note_template()),
            ("human", "{input}")
        ])
        
        
    def get_task_template(self):
        return f"""
        ### Examples:
        Query: "Can you update the schedule for the project kickoff?"
        Answer: schedule
        Query: "Draft a memo to inform the team about the upcoming training."
        Answer: note
        Query: "How are you?"
        Answer: None
        
        ### Instructions:
        Determine the relevance of the user query to schedule or note.
        - note: event, memo, or task, and everything that can be noted.
        - schedule: specific datetime for an event is mentioned.
        - None: chat without information
        Reply only one word: "schedule", "note", or "None" without explanations.
        
        ### Your task:
        Query: {{input}}
        Answer: 
        """
        
    def get_operation_template(self):
        return f"""
        ### Examples:
        Query: 'Please delete my appointment with John.'
        Answer: delete
        Query: 'Update tomorrow's team meeting to 11 AM.'
        Answer: update
        Query: 'Create a note for my presentation next week.'
        Answer: create
        Query: 'Can you show my upcoming events?'
        Answer: search

        ### Instructions:
        Determine whether the user query is related to one of the operation types: [{', '.join(self.ops)}].
        Reply with only one word: "create", "delete", "update", "search", or None without explanations.
        
        ### Your task:
        Query: {input}
        Answer:
        """
    
    def extract_info_for_note_template(self):
        return """
        ### Examples:
        Query: "Can you show me the groceries list?"
        Answer: {{content: 'groceries list'}}
        Query: "Please update the status for the group project of my NLP course to completed."
        Answer: {{content: 'status for the group project of my NLP course to completed'}}
        
        ### Instructions:
        Extract the original description of an event from the user query, not to change the content.
        Reply with only the JSON without explanations.
        
        ### Your task:
        Query: {input}
        Answer:
        """
        
    
    def extract_info_for_schedule_template(self):

        return """
        ### Examples:
        Query: "Schedule a 2-hour meeting for tomorrow starting at 3 PM, which is for the next summer's new product."
        Answer: {{"content": "2-hour meeting which is for the next summer's new product", "start_date": "tomorrow", "start_time": "at 3 PM", "end_date": "tomorrow", "end_time": "5 PM", "recurrence_pattern": null, "recurrence_rule": null, "search_time_frame": null}}
        
        Query: "Set a reminder for my NLP course presentation next Thursday at 1 PM for 90 minutes."
        Answer: {{"content": "my NLP course presentation", "start_date": "next Thursday", "start_time": "1 PM", "end_date": "next Thursday", "end_time": "2:30 PM", "recurrence_pattern": null, "recurrence_rule": null, "search_time_frame": null}}
        
        Query: "Can you book a weekly team meeting every Tuesday from 9 AM to 10:30 AM?"
        Answer: {{"content": "weekly team meeting", "start_date": "every Tuesday", "start_time": "9 AM", "end_date": "every Tuesday", "end_time": "10:30 AM", "recurrence_pattern": "WEEKLY", "recurrence_rule": 2, "search_time_frame": null}}
        
        Query: "Set a daily reminder to take my medication at 9 AM."
        Answer: {{"content": "take my medication", "start_date": "today", "start_time": "9 AM", "end_date": "today", "end_time": null, "recurrence_pattern": "DAILY", "recurrence_rule": 1, "search_time_frame": null}}
        
        Query: "Schedule a lunch meeting every month on the 15th at noon for 1 hour."
        Answer: {{"content": "lunch meeting every month", "start_date": "15th of every month", "start_time": "12 PM", "end_date": "15th of every month", "end_time": "1 PM", "recurrence_pattern": "MONTHLY", "recurrence_rule": 15, "search_time_frame": null}}
        
        Query: "What meetings did I have last week?"
        Answer: {{"content": "meetings", "start_date": null, "start_time": null, "end_date": null, "end_time": null, "recurrence_pattern": null, "recurrence_rule": null, "search_time_frame": "last week"}}
        
        ### Instructions:
        Analyze the user's query to identify the time information for scheduling events:
        - "content": the complete original event description in the query, only except the time details.
        - "start_date": the event date (e.g., "tomorrow", "next Thursday", "every Tuesday", "January 1st").
        - "start_time": the event time specifications (e.g., "3 PM", "12:30", ...).
        - "end_date": the event end date if present in the query.
        - "end_time": the event end time if present in the query. 
        - "recurrence_pattern": `DAILY`, `WEEKLY`, `BIWEEKLY`, `MONTHLY`
        - "recurrence_rule": the frequency for daily; weekday for weekly, and day of the month for monthly.
        - "search_time_frame": the time frame for searching events (e.g., "today", "this week", "next month").

        Reply with complete time information or "null" if no time details are present.
        Reply only the JSON without explanations.
        
        ### Your task:
        Query: {input}
        Answer:
        """
    
    
    def check_task_relevance(self):
        chain = self.task_prompt | self.llm
        return self.extract_valid_answer(chain, valid_set=self.tasks)

    def identify_operation_type(self):
        chain = self.operation_prompt | self.llm
        return self.extract_valid_answer(chain, valid_set=self.ops)
    
    def extract_info(self, task_type=None):
        if task_type == "schedule":
            chain = self.schedule_prompt | self.llm
            return self.extract_info_dict(chain.invoke({"input": self.input_query}))
        elif task_type == "note":
            chain = self.note_prompt | self.llm
            response = chain.invoke({"input": self.input_query})
            d = self.extract_info_dict(response)
            if d and 'content' in d.keys():
                return self.extract_info_dict(response)
            else:
                return {"content": self.input_query}
        else:
            return None
    
    def extract_valid_answer(self, chain, valid_set=None, valid_threshold=1, max_attempts=5):
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
    
    def extract_info_dict(self, response):
        json_pattern = r"\{.*\}"
        match = re.search(json_pattern, response)
        if match:
            json_string = match.group()
            try:
                time_info = json.loads(json_string, strict=False)
                return {col: time_info.get(col, None) for col in self.time_cols}
            except json.JSONDecodeError as e:
                return None
        else:
            return None


    def get_intents(self):
        # step 1: check task relevance
        task_type = self.check_task_relevance()
        
        # No relevance, skip further steps
        if not task_type:
            return None, None, None  
        
        # step 2: identify operation type
        operation_type = self.identify_operation_type()
        
        # No operation, skip time info
        if not operation_type:
            return task_type, None, None
        
        # step 3: extract information
        info = self.extract_info(task_type)
        return task_type, operation_type, info
            

if __name__ == "__main__":
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    logging.basicConfig(level=logging.INFO)
    # input_query = "Can you show the meetings for last month?"
    # input_query = "what is my appointment for today?"
    # input_query = "Can you update the schedule for the project kickoff?"
    # input_query = "i will have a project next friday, and take down the new project requirements, 1. review the project scope, 2. discuss the timeline, 3. assign tasks."
    input_query = "add a weekly team meeting every Friday at 10 AM."
    # input_query = "do you love me?"
    # input_query = "i will have a daily standup meeting at 9 AM."
    
    recognizer = IntentRecognizer(input_query)
    task_type, operation_type, info = recognizer.get_intents()

    print(f"Task Type: {task_type}")
    print(f"Operation Type: {operation_type}")
    print(f"Info: {info}")
    