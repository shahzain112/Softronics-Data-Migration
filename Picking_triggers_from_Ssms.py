import pyodbc
import os

server = '10.10.0.99'
database = 'PharmaCRM'
username = 'Shahzain'
password = 'abc*123ABC'

folder_path = r'C:\Users\user1\Pictures\sqlines-3.3.171\triggerssourcefolder'  # Folder to save triggers
os.makedirs(folder_path, exist_ok=True)

connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes"
)

with pyodbc.connect(connection_string) as conn:
    cursor = conn.cursor()
    # Fetch trigger names from the sysobjects table or INFORMATION_SCHEMA
    cursor.execute("""
        SELECT name
        FROM sys.triggers
    """)
    triggers = cursor.fetchall()

    for trigger in triggers:
        trigger_name = trigger[0]
        
        # Fetch the DDL for the trigger using OBJECT_DEFINITION
        cursor.execute("""
            SELECT OBJECT_DEFINITION(OBJECT_ID(?))
        """, (trigger_name,))
        ddl_content = cursor.fetchone()[0]

        if ddl_content:
            file_path = os.path.join(folder_path, f"{trigger_name}.txt")
            # Save the DDL to a file
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(ddl_content)
            print(f"Trigger DDL for {trigger_name} saved to {file_path}")
        else:
            print(f"No DDL found for trigger {trigger_name}")
