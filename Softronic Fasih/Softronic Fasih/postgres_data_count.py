import psycopg2
import csv

postgres_config = {
    "host": "10.10.0.99",
    "database": "AuditTrailPharmaCRM",
    "user": "postgres",
    "password": "postgres",
    "port": 5432,  
}

def get_table_counts(schema_name):
    try:
        conn = psycopg2.connect(**postgres_config)
        cursor = conn.cursor()
        
        cursor.execute(f"""
           SELECT TABLE_NAME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = 'public' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT IN ('sysdiagrams', 'tbl##ss_specificmonthsalesdsmteamwisesummary');
        """)
        
        tables = cursor.fetchall()
        print(f"Number of tables retrieved: {len(tables)}")
        table_counts = {}
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f'SELECT COUNT(*) FROM {table_name};')
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
    schema_to_check = "public"
    table_counts = get_table_counts(schema_to_check)
    
    if table_counts:
        csv_file_path = "AuditTrailPharmaCRM_postgres_data_count_public.csv"
        write_counts_to_csv(csv_file_path, table_counts)

