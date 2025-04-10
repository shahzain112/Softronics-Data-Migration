import re
import psycopg2

# Function to extract table and column names from a stored procedure
def extract_table_and_column_names(procedure_text):
    # Regex patterns for extracting table names and column names
    table_pattern = re.compile(r"\bFROM\s+([\w\"\.]+)|\bJOIN\s+([\w\"\.]+)", re.IGNORECASE)
    column_pattern = re.compile(r"\bSELECT\s+(.*?)\s+FROM", re.IGNORECASE | re.DOTALL)

    # Find all matches for table names
    table_matches = table_pattern.findall(procedure_text)
    tables = set([match[0] or match[1] for match in table_matches])

    # Find all matches for column names
    column_matches = column_pattern.findall(procedure_text)
    columns = set()
    for match in column_matches:
        # Split columns by commas and remove extra spaces or aliases
        for col in match.split(','):
            clean_col = col.strip().split(' ')[0]
            if clean_col:
                columns.add(clean_col)

    return tables, columns

# Function to create a table for storing table and column usage
def create_usage_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS procedure_table_usage (
                id SERIAL PRIMARY KEY,
                table_name TEXT,
                column_name TEXT
            )
        """)
        conn.commit()

# Function to insert extracted table and column names into the usage table
def insert_usage_data(conn, tables, columns):
    with conn.cursor() as cursor:
        for table in tables:
            for column in columns:
                cursor.execute(
                    "INSERT INTO procedure_table_usage (table_name, column_name) VALUES (%s, %s)",
                    (table, column)
                )
        conn.commit()

# Full stored procedure text (replace with your stored procedure)
stored_procedure = """"""

# Extract table and column names
tables, columns = extract_table_and_column_names(stored_procedure)

# Connect to the database (replace with your connection details)
conn = psycopg2.connect(
     dbname="your_dbname",
     user="your_username",
     password="your_password",
     host="your_host",
     port="your_port"
)

# Hardcoding connection placeholder (you should uncomment and replace the above)
conn = None

try:
    # Create the usage table
    create_usage_table(conn)

    # Insert the extracted data into the usage table
    insert_usage_data(conn, tables, columns)

    print("Table and column usage data has been stored successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()
