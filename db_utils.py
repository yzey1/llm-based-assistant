import yaml
import re
from langchain_community.utilities import SQLDatabase


class DBOperator:
    def __init__(self):
        db_config = self.load_db_config('config.yaml')
        self.db = SQLDatabase.from_uri(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        )
    
    def load_db_config(self, file_path):
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config['database']

    def run_sql_statement(self, sql_statement):
        # identify the type of sql statement (insert, delete, update, select)
        sql_keywords = {
            "insert": "insert",
            "delete": "delete",
            "update": "update",
            "select": "select"
        }
        op_type = next((op for keyword, op in sql_keywords.items() if keyword in sql_statement.lower()), "unknown")
        
        try:
            res = self.db.run(sql_statement)
            return {"op": op_type, "res": res, "status": 1}
        except Exception as e:
            return {"op": op_type, "res": str(e), "status": 0}
    
    def get_table_info(self, table_name):
        if table_name:
            return self.db.get_table_info([table_name])
        else:
            return self.db.get_table_info()
        
    def get_pk(self, table_name):
        primary_key_pattern = re.compile(r'PRIMARY KEY\s*\(([^)]+)\)')
        
        sql_text = self.db.get_table_info([table_name])
        match = primary_key_pattern.search(sql_text)
        if match:
            return match.group(1)
        else:
            return None


if __name__ == '__main__':
    db_manager = DBManager()
    test = """INSERT INTO Schedules (user_id, description, end_time)
            VALUES (1, 'Weekly team meeting to discuss progress', '2024-11-05 11:00:00');"""
    result = db_manager.run_sql_statement(test)
    print(result)
    print(db_manager.db.dialect)