import psycopg2
try:
    conn = psycopg2.connect(dbname='base_prueba', user='postgres', password='14789632', host='localhost')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM series LIMIT 0;")
    colnames = [desc[0] for desc in cursor.description]
    print("Columnas en Postgres para la tabla 'series':", colnames)
    cursor.close()
    conn.close()
except Exception as e:
    print("Error:", e)
