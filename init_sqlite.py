import sqlite3

# Create or connect to SQLite database
conn = sqlite3.connect('llm_asst.db')  # If the database does not exist, it will be created automatically

# Create a cursor object
cursor = conn.cursor()

# drop tables if they exist
cursor.execute('''
DROP TABLE IF EXISTS schedule;
''')
cursor.execute('''
DROP TABLE IF EXISTS item;
''')
cursor.execute('''
DROP TABLE IF EXISTS recurrence;
''')


# Create Recurrence table
cursor.execute('''
CREATE TABLE IF NOT EXISTS recurrence (
    recurrence_id INTEGER PRIMARY KEY AUTOINCREMENT,
    recurrence_pattern TEXT CHECK(recurrence_pattern IN ('DAILY', 'WEEKLY', 'BIWEEKLY', 'MONTHLY')) NOT NULL,
    recurrence_rule INTEGER NOT NULL,
    UNIQUE (recurrence_pattern, recurrence_rule)
);
''')

# Create Item table
cursor.execute('''
CREATE TABLE IF NOT EXISTS item (
    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT DEFAULT NULL,
    content TEXT NOT NULL,
    item_type TEXT CHECK(item_type IN ('NOTE', 'EVENT')) NOT NULL DEFAULT 'NOTE',
    item_status TEXT CHECK(item_status IN ('ACTIVE', 'CANCELLED', 'COMPLETED')) DEFAULT 'ACTIVE',
    recurrence_id INTEGER DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (recurrence_id) REFERENCES recurrence(recurrence_id) ON DELETE SET NULL
);
''')

# Create Schedule table
cursor.execute('''
CREATE TABLE IF NOT EXISTS schedule (
    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER NOT NULL,
    start_date DATE NOT NULL,
    start_time TIME NOT NULL,
    end_date DATE NOT NULL,
    end_time TIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (item_id) REFERENCES item(item_id) ON DELETE CASCADE
);
''')

# Insert data into Recurrence table
cursor.execute('''
INSERT INTO recurrence (recurrence_pattern, recurrence_rule) VALUES 
    ('DAILY', 1),       -- Every day
    ('DAILY', 2),       -- Every 2 days
    ('DAILY', 5),       -- Every 5 days
    ('DAILY', 7),       -- Every 7 days (weekly)
    ('WEEKLY', 1),      -- Every Monday
    ('WEEKLY', 3),      -- Every Wednesday
    ('WEEKLY', 5),      -- Every Friday
    ('WEEKLY', 6),      -- Every Saturday
    ('BIWEEKLY', 1),    -- Every first Monday
    ('BIWEEKLY', 2),    -- Every second Tuesday
    ('BIWEEKLY', 3),    -- Every third Sunday
    ('BIWEEKLY', 4),    -- Every fourth Thursday
    ('MONTHLY', 1),     -- On the 1st of every month
    ('MONTHLY', 10),    -- On the 10th of every month
    ('MONTHLY', 15),    -- On the 15th of every month
    ('MONTHLY', 30);    -- On the 30th of every month
''')

# Insert data into Item table
cursor.execute("""
INSERT INTO item (title, content, item_type, item_status, recurrence_id) VALUES 
    ("Daily Standup", "Short daily meeting to discuss what was done yesterday, what will be done today, and any blockers.", "EVENT", "ACTIVE", 1),
    ("Every Other Day Report", "Submit a report every other day to track progress.", "EVENT", "ACTIVE", 2),
    ("Weekly Team Sync", "A sync meeting to align on project updates and address any blockers.", "EVENT", "ACTIVE", 5),
    ("Biweekly Project Check-in", "Check in on the project status every other week on Monday.", "EVENT", "ACTIVE", 9),
    ("Weekly Review Meeting", "Review ongoing projects, discuss progress and next steps.", "EVENT", "ACTIVE", 6),
    ("Quarterly Planning Session", "Plan for the upcoming quarter with the team.", "EVENT", "ACTIVE", NULL), 
    ("Monthly Financial Review", "Review the financial performance for the month and adjust forecasts.", "NOTE", "COMPLETED", NULL), 
    ("Monthly Newsletter Preparation", "Prepare and distribute the monthly company newsletter.", "EVENT", "ACTIVE", 14),
    ("Anniversary Reminder", "Reminder for Mark's work anniversary next week.", "NOTE", "ACTIVE", NULL), 
    ("Health Check Reminder", "Reminder to schedule annual health check-ups for the family.", "NOTE", "ACTIVE", NULL), 
    ("Software Training", "Conduct a training session on the new software features for the team.", "EVENT", "ACTIVE", NULL), 
    ("Team Building Activity", "Organize an outdoor team-building activity for all staff.", "EVENT", "CANCELLED", NULL), 
    ("Project Submission Deadline", "Final deadline for submitting the project deliverables.", "NOTE", "COMPLETED", NULL), 
    ("Annual Company Retreat Planning", "Plan logistics and activities for the upcoming company retreat.", "EVENT", "ACTIVE", 10),
    ("Weekly Yoga Class", "Attend yoga class every Friday evening.", "EVENT", "ACTIVE", 7),
    ("Monthly Budget Review", "Discuss and adjust the monthly budget based on the latest expenses.", "EVENT", "ACTIVE", 15),
    ("Daily Motivational Quote", "Send out a motivational quote to the team every day.", "EVENT", "ACTIVE", 1);
""")

# Insert data into Schedule table
cursor.execute("""
INSERT INTO schedule (item_id, start_date, start_time, end_date, end_time) VALUES 
    (1, "2024-11-06", "09:00:00", "2024-11-06", "09:15:00"),  -- Daily Standup
    (2, "2024-11-07", "10:00:00", "2024-11-07", "10:30:00"),  -- Every Other Day Report
    (3, "2024-11-04", "10:00:00", "2024-11-04", "11:00:00"),  -- Weekly Team Sync
    (4, "2024-11-11", "10:00:00", "2024-11-11", "10:30:00"),  -- Biweekly Project Check-in (next occurrence)
    (5, "2024-11-06", "14:00:00", "2024-11-06", "15:00:00"),  -- Weekly Review Meeting
    (6, "2024-11-01", "09:00:00", "2024-11-01", "10:00:00"),  -- Quarterly Planning Session
    (8, "2024-11-10", "08:00:00", "2024-11-10", "09:00:00"),  -- Monthly Newsletter Preparation
    (14, "2024-11-10", "10:00:00", "2024-11-10", "11:00:00"), -- Annual Company Retreat Planning
    (15, "2024-11-08", "17:00:00", "2024-11-08", "18:00:00"), -- Weekly Yoga Class
    (16, "2024-11-15", "09:00:00", "2024-11-15", "10:00:00"); -- Monthly Budget Review
""")

# Commit changes and close connection
conn.commit()
conn.close()

print("SQLite database and tables created successfully!")