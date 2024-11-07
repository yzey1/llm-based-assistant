import logging
import yaml
import re
import parsedatetime as pdt
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, inspect
from sqlalchemy import Column, Integer, String, Text, Date, Time, TIMESTAMP, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.sql import func, text

Base = declarative_base()

class recurrence(Base):
    __tablename__ = 'recurrence'
    
    recurrence_id = Column(Integer, primary_key=True, autoincrement=True)
    recurrence_pattern = Column(String, nullable=False)  # Changed from ENUM to String
    recurrence_rule = Column(Integer, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('recurrence_pattern', 'recurrence_rule', name='uq_recurrence_pattern_rule'),
    )

class item(Base):
    __tablename__ = 'item'
    
    item_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(100), default=None)
    content = Column(Text, nullable=False)
    item_type = Column(String, nullable=False, default='NOTE')  # Changed from ENUM to String
    item_status = Column(String, default='ACTIVE')  # Changed from ENUM to String
    recurrence_id = Column(Integer, ForeignKey('recurrence.recurrence_id'), default=None)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    recurrence = relationship('recurrence', backref='items')

class schedule(Base):
    __tablename__ = 'schedule'
    
    schedule_id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Integer, ForeignKey('item.item_id'), nullable=False)
    start_date = Column(Date, nullable=False)
    start_time = Column(Time, nullable=False)
    end_date = Column(Date, nullable=False)
    end_time = Column(Time, nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    item = relationship('item', backref='schedules')

# Define the SQLDBOperator class
class SQLDBOperator:
    def __init__(self):
        self.config = self.load_config('config.yaml')
        db_config = self.config['database']
        sqlite_url = f"sqlite:///{db_config['name']}.db"  # SQLite connection string
        self.engine = create_engine(sqlite_url)
        self.Session = sessionmaker(bind=self.engine)

        logging.basicConfig(filename=self.config['paths']['logging_file'], level=logging.INFO)

    def load_config(self, file_path):
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    
    def get_schema(self, table_name):
        """Get the schema of the specified table."""
        inspector = inspect(self.engine)
        return inspector.get_columns(table_name)

    def get_pk(self, table_name=None):
        """Get the primary key of a specified table."""
        inspector = inspect(self.engine)
        if table_name is None:
            return {table: inspector.get_pk_constraint(table)['constrained_columns'] for table in inspector.get_table_names()}
        else:
            return inspector.get_pk_constraint(table_name)['constrained_columns']

    def get_table_names(self):
        """Retrieve and return the table names in the database."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def create_recurrence(self, data):
        new_recurrence = recurrence(
            recurrence_pattern=data['recurrence_pattern'],
            recurrence_rule=data['recurrence_rule']
        )
        session = self.Session()
        session.add(new_recurrence)
        session.flush()  # get recurrence_id
        session.commit()
        return new_recurrence
    
    def parse_date_time(self, date_time_string):
        cal = pdt.Calendar()
        now = datetime.now()
        try:
            return cal.parseDT(date_time_string, now)[0]
        except Exception as e:
            logging.error(f"Error parsing date/time: {str(e)}")
            return None
    
    def get_time_frame(self, dt):
        dt = dt.date()
        now = datetime.now().date()
        if dt > now:
            return [now, dt]
        else:
            return [dt, now]        

    def create_item(self, data):
        # get the recurrence pattern and rule if they exist
        recurrence_obj = None
        if data['recurrence_pattern']:
            recurrence_obj = self.Session.query(recurrence).filter_by(
                recurrence_pattern=data['recurrence_pattern'],
                recurrence_rule=data['recurrence_rule']
            ).first()
            if not recurrence_obj:
                recurrence_obj = self.create_recurrence(data)
        
        recurrence_id = recurrence_obj.recurrence_id if recurrence_obj else None
        item_type = 'EVENT' if data['start_date'] else 'NOTE'
        
        new_item = item(
            title=data['content'],
            content=data['content'],
            item_type=item_type,
            recurrence_id=recurrence_id
        )
        session = self.Session()
        session.add(new_item)
        session.flush()  # get item_id
        
        if item_type == 'EVENT':
            self.create_schedule(new_item.item_id, data)
        
        session.commit()
        return new_item

    def create_schedule(self, item_id, data):
        start_date = self.parse_date_time(data['start_date'])
        start_time = self.parse_date_time(data['start_time']) if data['start_time'] else None
        end_date = self.parse_date_time(data['end_date']) if data['end_date'] else None
        end_time = self.parse_date_time(data['end_time']) if data['end_time'] else None
        
        new_schedule = schedule(
            item_id=item_id,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time
        )
        session = self.Session()
        session.add(new_schedule)
        session.commit()
        
        return new_schedule
    
    def get_items(self, item_id=None, content=None, start_date=None, start_time=None, end_date=None, end_time=None,
                  recurrence_pattern=None, recurrence_rule=None, search_time_frame=None):
        """Retrieve items based on the specified criteria."""
        session = self.Session()
        query = session.query(item).join(schedule).join(recurrence)
        
        try:
            if item_id:
                if isinstance(item_id, list):
                    query = query.filter(item.item_id.in_(item_id))
                else:
                    query = query.filter(item.item_id == item_id)
            
            if content:
                query = query.filter(item.content.like(f"%{content}%"))
            
            if start_date:
                start_date = self.parse_date_time(start_date).date()
                query = query.filter(schedule.start_date == start_date)
            
            if start_time:
                start_time = self.parse_date_time(start_time).time()
                query = query.filter(schedule.start_time == start_time)
            
            if end_date:
                end_date = self.parse_date_time(end_date).date()
                query = query.filter(schedule.end_date == end_date)
            
            if end_time:
                end_time = self.parse_date_time(end_time).time()
                query = query.filter(schedule.end_time == end_time)
            
            if recurrence_pattern:
                query = query.filter(recurrence.recurrence_pattern == recurrence_pattern)
            
            if recurrence_rule:
                query = query.filter(recurrence.recurrence_rule == recurrence_rule)
            
            if search_time_frame:
                start_date, end_date = self.get_time_frame(self.parse_date_time(search_time_frame))
                query = query.filter(schedule.start_date >= start_date, schedule.start_date <= end_date)
            
            # Exclude created_at and updated_at columns
            query = query.with_entities(
                item.item_id, item.title, item.content, item.item_status,
                schedule.start_date, schedule.start_time, schedule.end_date, schedule.end_time,
                recurrence.recurrence_pattern, recurrence.recurrence_rule
            )
            
            result = query.all()
            columns = [
                'item_id', 'title', 'content', 'item_status',
                'start_date', 'start_time', 'end_date', 'end_time',
                'recurrence_pattern', 'recurrence_rule'
            ]
            
            return {"status": 1, "data": [dict(zip(columns, record)) for record in result]}
        
        except Exception as e:
            logging.error(f"Error retrieving items: {str(e)}")
            return {"status": 0, "message": str(e)}
    
    def delete_items(self, item_ids):
        """Delete items from the database."""
        session = self.Session()
        try:
            session.query(item).filter(item.item_id.in_(item_ids)).delete(synchronize_session=False)
            session.commit()
            return {"status": 1, "message": f"Deleted items with IDs: {item_ids}"}
        except Exception as e:
            session.rollback()
            return {"status": 0, "message": str(e)}
        finally:
            session.close()
            
    def update_items(self, item_ids, updates):
        """Update items in the database."""
        session = self.Session()
        try:
            session.query(item).filter(item.item_id.in_(item_ids)).update(updates, synchronize_session=False)
            session.commit()
            return {"status": 1, "message": f"Updated items with IDs: {item_ids}"}
        except Exception as e:
            session.rollback()
            return {"status": 0, "message": str(e)}
        finally:
            session.close()
            
    def object_as_dict(self, obj):
        """Convert a SQLAlchemy object to a dictionary."""
        return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}
    
    def object_list_as_dict(self, obj_list):
        """Convert a list of SQLAlchemy objects to a list of dictionaries."""
        return [self.object_as_dict(obj) for obj in obj_list]
    
    def export_to_csv(self, query, csv_file):
        """Export data from the SQLite database to a CSV file."""
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
                    return {"status": 1, "response": records}
                elif operation_type in ["create", "update"]:
                    pk = result.lastrowid
                    return {"status": 1, "response": f"{operation_type} successful. Primary key: {pk}"}
                elif operation_type == "delete":
                    num_rows_deleted = result.rowcount
                    return {"status": 1, "response": f"{operation_type} successful. Rows deleted: {num_rows_deleted}"}
            except Exception as e:
                logging.error(f"Error executing SQL statement: {str(e)}")
                return {"status": 0, "message": str(e)}

if __name__ == '__main__':
    sql_operator = SQLDBOperator()
    
    # test get_items
    items = sql_operator.get_items(content='meeting', search_time_frame='next week')
    print(items)
    
    # test delete_items
    item_ids = [1, 2, 3]
    result = sql_operator.delete_items(item_ids)
    print(result)
    
    # test update_items
    item_ids = [4, 5]
    updates = {'content': 'updated content'}
    result = sql_operator.update_items(item_ids, updates)
    print(result)