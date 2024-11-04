import logging
import yaml
import re
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, Enum, TIMESTAMP, ForeignKey
from sqlalchemy.sql import text

# Define the base class for declarative models
Base = declarative_base()

# Define the models based on your schema
class event(Base):
    __tablename__ = 'event'
    event_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(100), nullable=False)
    content = Column(Text)
    recurrence_pattern = Column(Enum('DAILY', 'WEEKLY', 'MONTHLY'), default=None)
    recurrence_rule = Column(String(10), default=None)
    location = Column(String(255), default=None)
    status = Column(Enum('ACTIVE', 'CANCELLED', 'COMPLETED'), default='ACTIVE')
    created_at = Column(TIMESTAMP, default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

class schedule(Base):
    __tablename__ = 'schedule'
    schedule_id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey('Events.event_id', ondelete='CASCADE'), nullable=False)
    start_time = Column(TIMESTAMP, nullable=False)
    end_time = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP, default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

class memo(Base):
    __tablename__ = 'memo'
    memo_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(100), default=None)
    content = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'))

class SQLDBOperator:
    def __init__(self):
        self.config = self.load_config('config.yaml')
        db_config = self.config['database']
        mysql_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        self.engine = create_engine(mysql_url)
        self.Session = sessionmaker(bind=self.engine)

        # Set up logging
        logging.basicConfig(filename=self.config['paths']['logging_file'], level=logging.INFO)

    def load_config(self, file_path):
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def insert(self, table_name, data):
        """Insert data into the specified table."""
        session = self.Session()
        try:
            table = globals()[table_name]
            record = table(**data)
            session.add(record)
            session.commit()
            logging.info(f"Inserted data into {table_name}")
            return {"status": 1, "message": f"Inserted data into {table_name}"}
        except Exception as e:
            session.rollback()
            logging.error(f"Error inserting into {table_name}: {str(e)}")
            return {"status": 0, "message": str(e)}
        finally:
            session.close()

    def delete(self, table_name, condition):
        """Delete records from the specified table based on a condition."""
        session = self.Session()
        try:
            table = globals()[table_name]
            query = session.query(table).filter(text(condition))
            query.delete()
            session.commit()
            return {"status": 1, "message": f"Deleted records from {table_name} where {condition}"}
        except Exception as e:
            session.rollback()
            return {"status": 0, "message": str(e)}
        finally:
            session.close()

    def update(self, table_name, updates, condition):
        """Update records in the specified table based on a condition."""
        session = self.Session()
        try:
            table = globals()[table_name]
            query = session.query(table).filter(text(condition))
            query.update(updates)
            session.commit()
            return {"status": 1, "message": f"Updated records in {table_name} where {condition}"}
        except Exception as e:
            session.rollback()
            return {"status": 0, "message": str(e)}
        finally:
            session.close()

    def select(self, table_name, columns='*', condition=None):
        """Select records from the specified table, optionally with a condition."""
        session = self.Session()
        try:
            table = globals()[table_name]
            query = session.query(table)
            if condition:
                query = query.filter(text(condition))
            result = query.all()
            return {"status": 1, "data": result}
        except Exception as e:
            return {"status": 0, "message": str(e)}
        finally:
            session.close()
    
    def object_as_dict(self, obj):
        """Convert a SQLAlchemy object to a dictionary."""
        return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}
    
    def object_list_as_dict(self, obj_list):
        """Convert a list of SQLAlchemy objects to a list of dictionaries."""
        return [self.object_as_dict(obj) for obj in obj_list]
    
    def object_as_str(self, obj):
        """Convert a SQLAlchemy object to a string."""
        return str(self.object_as_dict(obj))
    
    def object_list_as_str(self, obj_list):
        """Convert a list of SQLAlchemy objects to a list of strings."""
        return [self.object_as_str(obj) for obj in obj_list]
        
    def get_schema(self, table_name):
        """Get the schema of the specified table."""
        inspector = inspect(self.engine)
        return inspector.get_columns(table_name)

    def get_pk(self, table_name=None):
        """Get the primary key of a specified table."""
        inspector = inspect(self.engine)
        if table_name is None:
            # get pk for all tables
            return {table: inspector.get_pk_constraint(table)['constrained_columns'] for table in inspector.get_table_names()}
        else:
            return inspector.get_pk_constraint(table_name)['constrained_columns']

    def get_table_names(self):
        """Retrieve and return the table names in the database."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def export_to_csv(self, query, csv_file):
        """Export data from the MySQL database to a CSV file."""
        df = pd.read_sql(query, self.engine)
        output_file = self.config['paths']['csv_output'] + csv_file
        df.to_csv(output_file, index=False)
        logging.info(f"Data exported to {output_file}")
        print(f"Data exported to {output_file}")
    
    def run_sql_statement(self, sql, operation_type):
        with self.engine.connect() as connection:
            try:
                result = connection.execute(text(sql))
                if operation_type == "search":
                    column = result.keys()
                    row = result.fetchall()
                    records = [dict(zip(column, r)) for r in row]
                    # convert each record to a string (no brackets, quotes, etc.)
                    string_records = []
                    for record in records:
                        string_records.append(", ".join([f"{key}: {value}" for key, value in record.items()]))

                    return {"status": 1, "response": string_records}
                else:
                    return {"status": 1, "response": f"{operation_type} successful"}
            except Exception as e:
                logging.error(f"Error executing SQL statement: {str(e)}")
                return {"status": 0, "message": str(e)}
        
        
        
if __name__ == '__main__':
    sql_operator = SQLDBOperator()
    # test = """INSERT INTO Schedules (user_id, description, end_time)
    #         VALUES (1, 'Weekly team meeting to discuss progress', '2024-11-05 11:00:00');"""
    # result = db_manager.run_sql_statement(test)
    # print(result)
    # print(db_manager.db.dialect)
    
    # for tbl_name in ["memo", "schedule", "event"]:
    #     tbl = sql_operator.select(tbl_name)
    #     tbl = sql_operator.object_list_as_str(tbl['data'])
    #     print(tbl)
    
    # schema = sql_operator.get_schema('event')
    # print(schema)
    
    sql_statement = 'SELECT e.title, s.start_time, s.end_time \nFROM event e \nJOIN schedule s ON e.event_id = s.event_id \nWHERE s.start_time >= NOW() AND s.start_time <= DATE_ADD(NOW(), INTERVAL 7 DAY);'
    result = sql_operator.run_sql_statement(sql_statement, "search")
    print(result)