from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQLServerToPostgreSQLMigration") \
    .config("spark.jars", 
            "E:/VS Repo/spark_jars/sqljdbc_12.8.1.0_enu/sqljdbc_12.8/enu/jars/mssql-jdbc-12.8.1.jre11.jar,"
            "E:/VS Repo/spark_jars/postgresql-42.7.4.jar") \
    .getOrCreate()

sqlserver_url = "jdbc:sqlserver://10.10.0.99:1433;databaseName=AuditTrailPharmaCRM;encrypt=true;trustServerCertificate=true"
sqlserver_properties = {
    "user": "Shahzain",
    "password": "abc*123ABC",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

postgresql_url = "jdbc:postgresql://10.10.0.99:5432/AuditTrailPharmaCRM"
postgresql_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

schema_to_migrate = "dbo"

tables_query = f"""
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = '{schema_to_migrate}' AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT IN ('sysdiagrams', 'log_datetime_table')
"""

try:
    table_names_df = spark.read.jdbc(
        url=sqlserver_url,
        table=f"({tables_query}) AS tables_query",
        properties=sqlserver_properties
    )

    table_names = [row.TABLE_NAME for row in table_names_df.collect()]

    print(f"Found {len(table_names)} tables to migrate.")

    for table_name in table_names:
        print(f"Starting migration for table: {table_name}")
        try:
            df = spark.read.jdbc(
                url=sqlserver_url,
                table=f"{schema_to_migrate}.{table_name}",
                properties=sqlserver_properties
            )
            
            df.write.jdbc(
                url=postgresql_url,
                table=(f'{table_name}'),
                mode="append",
                properties=postgresql_properties
            )
            print(f"Successfully migrated table: {table_name}")
        except Exception as e:
            print(f"Error migrating table {table_name}: {e}")

except Exception as e:
    print(f"Error fetching table names: {e}")

finally:
    spark.stop()
    print("Migration completed. SparkSession stopped.")