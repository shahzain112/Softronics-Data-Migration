import os
import psycopg2
from datetime import datetime

def execute_sql_files(folder_path, db_config):
    def preprocess_sql(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            sql = file.read()
            sql = sql.replace('GO', ';')  # Replace SQL Server's 'GO'
            return sql

    log_file = "uss_log.txt"
    with open(log_file, "w", encoding="utf-8") as log:
        def log_error(file_name, error):
            log.write(f"{file_name}: {error}\n")

        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            print("Connected to the database.")
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            return

        for file_name in os.listdir(folder_path):
            if file_name.endswith('.sql'):
                file_path = os.path.join(folder_path, file_name)
                try:
                    sql_query = preprocess_sql(file_path)
                    cursor.execute("BEGIN;")  # Start a transaction
                    cursor.execute(sql_query)
                    cursor.execute("COMMIT;")
                    print(f"Successfully executed {file_name}")
                except Exception as e:
                    cursor.execute("ROLLBACK;")  # Rollback on error
                    log_error(file_name, e)

        cursor.close()
        conn.close()
        print(f"Errors logged in {log_file}.")

# Configuration for PostgreSQL database connection
db_config = {
    "dbname": "abc",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Path to the folder containing .sql files
folder_path = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\umer+shariq+shahzain"

# Execute the SQL files
execute_sql_files(folder_path, db_config)
