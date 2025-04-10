from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("SQLServerToPostgreSQLMigration") \
    .config("spark.jars", 
            "E:/VS Repo/spark_jars/sqljdbc_12.8.1.0_enu/sqljdbc_12.8/enu/jars/mssql-jdbc-12.8.1.jre11.jar," 
            "E:/VS Repo/spark_jars/postgresql-42.7.4.jar") \
    .getOrCreate()

sqlserver_url = "jdbc:sqlserver://10.10.0.99:1433;databaseName=PharmaCRM;encrypt=true;trustServerCertificate=true"
sqlserver_properties = {
    "user": "Shahzain",
    "password": "abc*123ABC",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

postgresql_url = "jdbc:postgresql://10.10.0.99:5432/PharmaCRM"
postgresql_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

schema_to_migrate = "dbo"
log_table = "PharmaCRM.migration_logs.log_datetime_table"

tables_query = f"""
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = '{schema_to_migrate}' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'ActivitySMSRecepient'
"""

def write_log(table_name, start_read, end_read, start_write, end_write, rows_read, rows_written, error=None):
    start_read_str = start_read.strftime('%Y-%m-%d %H:%M:%S') if start_read else None
    end_read_str = end_read.strftime('%Y-%m-%d %H:%M:%S') if end_read else None
    start_write_str = start_write.strftime('%Y-%m-%d %H:%M:%S') if start_write else None
    end_write_str = end_write.strftime('%Y-%m-%d %H:%M:%S') if end_write else None
    
    log_data = [
        (table_name, start_read_str, end_read_str, start_write_str, 
         end_write_str, rows_read, rows_written, error)
    ]
    log_columns = [
        "table_name", "start_read", "end_read", "start_write", 
        "end_write", "rows_read", "rows_written", "error"
    ]
    log_df = spark.createDataFrame(log_data, schema=log_columns)
    
    try:
        log_df.write.jdbc(
            url=postgresql_url,
            table=log_table,
            mode="append",
            properties=postgresql_properties
        )
        print(f"Log written successfully for table: {table_name}")
    except Exception as log_error:
        print(f"Error writing log for table {table_name}: {log_error}")

def migrate_data(table_name):
    start_read_time = datetime.now()
    error_message = None
    rows_read = 0
    rows_written = 0
    start_write_time = None
    end_read_time = None
    end_write_time = None

    try:
        df = spark.read.jdbc(
            url=sqlserver_url,
            table=f"{schema_to_migrate}.{table_name}",
            properties=sqlserver_properties
        )
        rows_read = df.count() 
        end_read_time = datetime.now()

        start_write_time = datetime.now()
        df.write.jdbc(
            url=postgresql_url,
            table=f"{schema_to_migrate}.{table_name}",
            mode="append",
            properties=postgresql_properties
        )
        rows_written = rows_read 
        end_write_time = datetime.now()
        print(f"Successfully migrated table: {schema_to_migrate}.{table_name}")
    except Exception as e:
        error_message = str(e)
        print(f"Error migrating table {schema_to_migrate}.{table_name}: {e}")
    finally:
        write_log(
            table_name=table_name,
            start_read=start_read_time,
            end_read=end_read_time,
            start_write=start_write_time,
            end_write=end_write_time,
            rows_read=rows_read,
            rows_written=rows_written,
            error=error_message
        )

def main():
    try:
        table_names_df = spark.read.jdbc(
            url=sqlserver_url,
            table=f"({tables_query}) AS tables_query",
            properties=sqlserver_properties
        )
        table_names = [row.TABLE_NAME for row in table_names_df.collect()]
        print(f"Found {len(table_names)} tables to migrate.")

        for table_name in table_names:
            print(f"Starting migration for table: {schema_to_migrate}.{table_name}")
            migrate_data(table_name)
    except Exception as e:
        print(f"Error fetching table names: {e}")
    finally:
        spark.stop()
        print("Migration completed. SparkSession stopped.")

if __name__ == "__main__":
    main()
