import pyodbc
import csv

sqlserver_config = {
    "server": "10.10.0.99",
    "database": "AuditTrailPharmaCRM",
    "username": "Shahzain",
    "password": "abc*123ABC",
    "port": 1433,
    "driver": "ODBC Driver 17 for SQL Server"
}

def get_sqlserver_connection(config):
    try:
        conn = pyodbc.connect(
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']},{config['port']};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']}"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        return None

def get_table_counts_from_sqlserver(schema_name):
    try:
        conn = get_sqlserver_connection(sqlserver_config)
        if not conn:
            return None
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' 
            AND TABLE_NAME NOT IN ('sysdiagrams');
        """, schema_name)
        
        tables = cursor.fetchall()
        table_counts = {}
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
            count = cursor.fetchone()[0]
            table_counts[table_name] = count
        
        return table_counts
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

def write_counts_to_csv(file_path, table_counts):
    """
    Writes table counts to a CSV file.
    """
    try:
        with open(file_path, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["Table Name", "Row Count"])
            for table_name, row_count in table_counts.items():
                writer.writerow([table_name, row_count])
        print(f"Table counts written to {file_path} successfully.")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

if __name__ == "__main__":
    schema_to_check = "dbo"
    table_counts = get_table_counts_from_sqlserver(schema_to_check)
    
    if table_counts:
        csv_file_path = "AuditTrailPharmaCRM_sqlserver_data_count_public.csv"
        write_counts_to_csv(csv_file_path, table_counts)
