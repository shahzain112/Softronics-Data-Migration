import pyodbc
import psycopg2
from psycopg2 import sql
from collections import defaultdict, deque

# Define connection parameters for source and target databases
source_server = '10.10.0.99'
source_database = 'AuditTrailPharmaCRM'
source_username = 'Shahzain'
source_password = 'abc*123ABC'
source_driver = '{ODBC Driver 18 for SQL Server}'

target_host = '10.10.0.99'
target_database = 'AuditTrailPharmaCRM'
target_username = 'postgres'
target_password = 'postgres'

# Comprehensive mapping of SQL Server data types to PostgreSQL data types
data_type_mapping = {
    'bigint': 'bigint',
    'binary': 'bytea',
    'bit': 'boolean',
    'char': 'char',
    'date': 'date',
    'datetime': 'timestamp',
    'datetime2': 'timestamp',
    'datetimeoffset': 'timestamptz',
    'decimal': 'numeric',
    'float': 'double precision',
    'image': 'bytea',
    'int': 'integer',
    'money': 'numeric',
    'nchar': 'char',
    'ntext': 'text',
    'numeric': 'numeric',
    'nvarchar': 'varchar',
    'real': 'real',
    'smalldatetime': 'timestamp',
    'smallint': 'smallint',
    'smallmoney': 'numeric',
    'sql_variant': 'text',
    'text': 'text',
    'time': 'time',
    'timestamp': 'bytea',
    'tinyint': 'smallint',
    'uniqueidentifier': 'uuid',
    'varbinary': 'bytea',
    'varchar': 'varchar',
    'xml': 'xml',
}

def sanitize_name(name):
    # Convert to lowercase and add quotes
    return f'"{name.lower()}"'

try:
    # Connect to the source database
    source_conn_str = (
        f"DRIVER={source_driver};"
        f"SERVER={source_server};"
        f"DATABASE={source_database};"
        f"UID={source_username};"
        f"PWD={source_password};"
        f"TrustServerCertificate=yes;"
    )
    source_conn = pyodbc.connect(source_conn_str)
    source_cursor = source_conn.cursor()

    # Connect to the target PostgreSQL database
    target_conn = psycopg2.connect(
        dbname=target_database, user=target_username, password=target_password, host=target_host, port='5432'
    )
    target_cursor = target_conn.cursor()

    # Get a list of table names
    table_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'"
    source_cursor.execute(table_query)
    table_names = [row[0].lower() for row in source_cursor.fetchall()]  # Convert to lowercase here

    # Build a dependency graph
    dependency_graph = defaultdict(list)
    foreign_key_constraints = defaultdict(list)
    failed_statements = []  # List to store failing statements (DDL or FK)

    for table_name in table_names:
        sanitized_table_name = sanitize_name(table_name)

        # Get foreign key constraints (convert table name to original case for SQL Server query)
        foreign_key_query = f"""
            SELECT 
                tp.name AS parent_table, 
                ref.name AS referenced_table, 
                cp.name AS parent_column, 
                cref.name AS referenced_column
            FROM 
                sys.foreign_keys fk
            INNER JOIN 
                sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN 
                sys.tables tp ON fkc.parent_object_id = tp.object_id
            INNER JOIN 
                sys.tables ref ON fkc.referenced_object_id = ref.object_id
            INNER JOIN 
                sys.columns cp ON fkc.parent_column_id = cp.column_id AND cp.object_id = tp.object_id
            INNER JOIN 
                sys.columns cref ON fkc.referenced_column_id = cref.column_id AND cref.object_id = ref.object_id
            WHERE 
                tp.name = '{table_name.upper()}' OR tp.name = '{table_name.lower()}' OR tp.name = '{table_name.capitalize()}'
        """
        source_cursor.execute(foreign_key_query)
        foreign_keys = source_cursor.fetchall()

        for fk in foreign_keys:
            parent_table, referenced_table, parent_column, referenced_column = fk
            # Convert all table names to lowercase for the dependency graph
            parent_table = parent_table.lower()
            referenced_table = referenced_table.lower()
            dependency_graph[parent_table].append(referenced_table)
            foreign_key_constraints[parent_table].append((
                parent_table, 
                referenced_table, 
                parent_column.lower(), 
                referenced_column.lower()
            ))

    # Create tables and process foreign keys
    for table_name in table_names:
        sanitized_table_name = sanitize_name(table_name)

        # Get column information (convert table name to original case for SQL Server query)
        column_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,
                   COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IsIdentity
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name.upper()}' OR TABLE_NAME = '{table_name.lower()}' OR TABLE_NAME = '{table_name.capitalize()}'
        """
        source_cursor.execute(column_query)
        columns = source_cursor.fetchall()

        # Start building the CREATE TABLE DDL
        ddl = f"CREATE TABLE IF NOT EXISTS {sanitized_table_name} (\n"
        for column_name, data_type, max_length, is_nullable, is_identity in columns:
            sanitized_column_name = sanitize_name(column_name.lower())  # Convert column names to lowercase
            pg_data_type = data_type_mapping.get(data_type.lower(), 'text')  # Ensure case-insensitive data type matching
            if max_length == -1:
                pg_data_type = 'TEXT'
            if is_identity == 1:
                ddl += f"    {sanitized_column_name} SERIAL,\n"
            elif pg_data_type in ['varchar', 'char'] and max_length not in (None, -1):
                ddl += f"    {sanitized_column_name} {pg_data_type}({max_length}) {'NOT NULL' if is_nullable == 'NO' else ''},\n"
            else:
                ddl += f"    {sanitized_column_name} {pg_data_type} {'NOT NULL' if is_nullable == 'NO' else ''},\n"

        ddl = ddl.rstrip(",\n") + "\n);"

        try:
            target_cursor.execute(ddl)
            target_conn.commit()
        except Exception as e:
            print(f"Failed to execute DDL for table {table_name}: {e}")
            failed_statements.append(ddl)

    # Add foreign key constraints
    for table_name, fks in foreign_key_constraints.items():
        for fk in fks:
            parent_table, referenced_table, parent_column, referenced_column = fk
            fk_query = f"""
                ALTER TABLE {sanitize_name(parent_table)} 
                ADD FOREIGN KEY ({sanitize_name(parent_column)}) 
                REFERENCES {sanitize_name(referenced_table)}({sanitize_name(referenced_column)});
            """
            try:
                target_cursor.execute(fk_query)
                target_conn.commit()
            except Exception as e:
                print(f"Failed to add foreign key for {parent_table} referencing {referenced_table}: {e}")
                failed_statements.append(fk_query)

    # Retry failed statements
    print("Retrying failed statements...")
    retry_attempts = 0
    while failed_statements and retry_attempts < 5:  # Retry up to 5 times
        current_failed = failed_statements[:]
        failed_statements = []
        for statement in current_failed:
            try:
                target_cursor.execute(statement)
                target_conn.commit()
            except Exception as e:
                print(f"Retry failed for statement: {statement}\nError: {e}")
                failed_statements.append(statement)
        retry_attempts += 1

    # Log statements that still fail after retries
    if failed_statements:
        print("The following statements could not be executed after retries:")
        for statement in failed_statements:
            print(statement)

    print("Processing completed.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    try:
        if source_cursor:
            source_cursor.close()
        if source_conn:
            source_conn.close()
        if target_cursor:
            target_cursor.close()
        if target_conn:
            target_conn.close()
    except Exception as cleanup_error:
        print(f"Cleanup error: {cleanup_error}")