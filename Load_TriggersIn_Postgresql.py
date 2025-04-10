import os
import psycopg2
from datetime import datetime

def execute_sql_files_for_views(folder_path, db_config):
    def preprocess_sql(file_path):
        """Read and preprocess the SQL file content."""
        with open(file_path, 'r', encoding='utf-8') as file:
            sql = file.read()
            # Replace SQL Server's 'GO' with PostgreSQL-compatible syntax
            sql = sql.replace('GO', ';')
            return sql

    log_file = "execution_log_views.txt"
    with open(log_file, "w", encoding="utf-8") as log:
        def log_error(file_name, error):
            """Log errors during SQL execution."""
            log.write(f"{file_name}: {error}\n")

        # Establish connection to the PostgreSQL database
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            print("Connected to the database.")
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            return

        # Iterate through .sql files in the specified folder
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.sql'):
                file_path = os.path.join(folder_path, file_name)
                try:
                    # Preprocess and execute the SQL query
                    sql_query = preprocess_sql(file_path)
                    cursor.execute("BEGIN;")  # Start a transaction
                    cursor.execute(sql_query)  # Execute the SQL query
                    cursor.execute("COMMIT;")  # Commit the transaction
                    print(f"Successfully executed {file_name}")
                except Exception as e:
                    cursor.execute("ROLLBACK;")  # Rollback the transaction on error
                    log_error(file_name, str(e))
                    print(f"Error executing {file_name}: {e}")

        # Close the cursor and connection
        cursor.close()
        conn.close()
        print(f"Execution completed. Errors (if any) are logged in {log_file}.")

# Configuration for PostgreSQL database connection
db_config = {
    "dbname": "PharmaCRM",
    "user": "postgres",
    "password": "postgres",
    "host": "10.10.0.99",
    "port": "5432"
}

# Path to the folder containing .sql files for views
folder_path = r"C:\Users\user1\Pictures\sqlines-3.3.171\newdestintation"

# Execute the SQL files for views
execute_sql_files_for_views(folder_path, db_config)
