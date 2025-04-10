import os
import psycopg2
from datetime import datetime

def execute_sql_files(folder_path, db_config):
    def preprocess_sql(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            sql = file.read()
            sql = sql.replace('GO', ';')  # Replace SQL Server's 'GO'
            return sql

    # Directories for success and failed files
    success_dir = os.path.join(folder_path, "SuccessSps")
    failed_dir = os.path.join(folder_path, "FailedSps")

    os.makedirs(success_dir, exist_ok=True)
    os.makedirs(failed_dir, exist_ok=True)

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

                # Save to success directory
                success_file_path = os.path.join(success_dir, file_name)
                with open(success_file_path, 'w', encoding='utf-8') as success_file:
                    success_file.write(sql_query)
                print(f"Successfully executed {file_name}. Saved to SuccessSps.")

            except Exception as e:
                cursor.execute("ROLLBACK;")  # Rollback on error

                # Save to failed directory
                failed_file_path = os.path.join(failed_dir, file_name)
                with open(failed_file_path, 'w', encoding='utf-8') as failed_file:
                    failed_file.write(sql_query)
                print(f"Failed to execute {file_name}. Error: {e}. Saved to FailedSps.")

    cursor.close()
    conn.close()
    print(f"Execution complete. Check 'SuccessSps' and 'FailedSps' directories.")

# Configuration for PostgreSQL database connection
db_config = {
    "dbname": "empty_database",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Path to the folder containing .sql files
folder_path = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\Full_And_Final"

# Execute the SQL files
execute_sql_files(folder_path, db_config)
