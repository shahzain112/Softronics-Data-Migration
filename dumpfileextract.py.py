import pyodbc
import os

# Define connection parameters
server = '10.10.0.99'
database = 'PharmaCRM'
username = 'Shahzain'
password = 'abc*123ABC'

# Directory to save the dump files
output_dir = r"D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\SourceFolderForDump"
os.makedirs(output_dir, exist_ok=True)

def export_stored_procedures():
    try:
        # Connect to the database
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = connection.cursor()
        
        # Fetch all stored procedures
        cursor.execute("""
            SELECT 
                SCHEMA_NAME(schema_id) AS [Schema], 
                name AS [ProcedureName], 
                OBJECT_DEFINITION(object_id) AS [Definition]
            FROM sys.procedures
        """)
        procedures = cursor.fetchall()

        for proc in procedures:
            proc_name = proc.ProcedureName
            proc_definition = proc.Definition

            if proc_definition:
                # Generate a file for each stored procedure
                file_name = f"{proc_name}.sql"
                file_path = os.path.join(output_dir, file_name)

                # Write the procedure definition only
                with open(file_path, 'w', encoding='utf-8', newline='') as file:
                    file.write(proc_definition.strip())  # Write only the stored procedure code

                print(f"Exported: {file_path}")
            else:
                print(f"Skipped empty definition for procedure: {proc_name}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    export_stored_procedures()
