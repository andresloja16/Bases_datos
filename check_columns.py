import psycopg2
try:
    conn = psycopg2.connect(dbname='datawarehouse_ja', user='publica_contenido', password='publica_contenido', host='172.18.46.16', port='6432')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jdw_dashboards.transaccion_perfil_riesgo LIMIT 0;")
    colnames = [desc[0] for desc in cursor.description]
    print("Columnas en Postgres para la tabla 'jdw_dashboards.transaccion_perfil_riesgo':", colnames)
    cursor.close()
    conn.close()
except Exception as e:
    print("Error:", e)
