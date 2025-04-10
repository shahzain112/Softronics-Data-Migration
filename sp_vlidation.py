import psycopg2

def call_procedure_and_fetch_data():
    try:
        conn = psycopg2.connect(
            dbname="PharmaCRM",
            user="postgres",
            password="postgres",
            host="10.10.0.99",
            port="5432"
        )

        conn.autocommit = False
        cur = conn.cursor()

        # Calling the stored procedure that returns results
        cur.execute("CALL Sp_GetBrandGroup('001')")

        # Fetching the result from the procedure
        rows = cur.fetchall()

        for row in rows:
            print(row)

    except Exception as e:
        print("Error:", e)

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

call_procedure_and_fetch_data()
