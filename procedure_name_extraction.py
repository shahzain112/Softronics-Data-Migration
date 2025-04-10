import pyodbc
import psycopg2

# SQL Server connection details
sql_server_conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.10.0.99;DATABASE=PharmaCRM;UID=Shahzain;PWD=abc*123ABC"

# PostgreSQL connection details
pg_conn_params = {
    "dbname": "PharmaCRM",
    "user": "postgres",
    "password": "postgres",
    "host": "10.10.0.99",
    "port": "5432",
}

# Query to get stored procedures
sql_server_query = "SELECT name FROM sys.procedures;"
postgres_query = """
    SELECT proname 
    FROM pg_proc 
    JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
    WHERE pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema');
"""

def get_sql_server_procedures():
    procedures = set()
    try:
        with pyodbc.connect(sql_server_conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_server_query)
                procedures = {row[0] for row in cursor.fetchall()}
    except Exception as e:
        print(f"SQL Server Error: {e}")
    return procedures

def get_postgres_procedures():
    procedures = set()
    try:
        with psycopg2.connect(**pg_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(postgres_query)
                procedures = {row[0] for row in cursor.fetchall()}
    except Exception as e:
        print(f"PostgreSQL Error: {e}")
    return procedures

# Get procedure names
sql_server_procs = get_sql_server_procedures()
postgres_procs = get_postgres_procedures()

# Find unique procedures
unique_in_sql_server = sql_server_procs - postgres_procs
unique_in_postgres = postgres_procs - sql_server_procs

# Print results
print("Procedures unique to SQL Server:")
print(unique_in_sql_server)

print("\nProcedures unique to PostgreSQL:")
print(unique_in_postgres)