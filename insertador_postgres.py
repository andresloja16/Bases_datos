import psycopg2
from psycopg2.extras import execute_batch
import time
import ast

# 1. CONFIGURACIÓN DE POSTGRESQL
SERVER = '172.18.46.16'      
DATABASE = 'datawarehouse_ja'       # Cambia esto por tu base de datos postgres real
USERNAME = 'publica_contenido'       # Usuario por defecto de Postgres
PASSWORD = 'publica_contenido'     # Tu contraseña de Postgres
PUERTO = '6432'             # Puerto por defecto de Postgres

# 2. EL MEGA-CEREBRO: LISTA DE TAREAS (Para múltiples archivos Postgres)
TAREAS = [
    {
        "archivo": r"C:\Users\Asus\OneDrive\Escritorio\INSERT INTO Series (titulo, descrip.txt",
        "query": "INSERT INTO jdw_dashboards.transaccion_perfil_riesgo (codigo_fecha_corte, codigo_socio, tipo_relacion, es_empleado, es_directivo, lista_consep, tipo_identificacion, numero_identificacion, nombre, nacionalidad, direccion, actividad_economica, total_ingreso_socio, total_ingreso_conyuge, total_gasto_socio, total_gasto_conyuge, total_patrimonio_socio, total_patrimonio_conyuge, genero, edad_cumplida, estado_civil, codigo_socio_con, tipo_identificacion_con, numero_identificacion_con, nombre_cony, codigo_socio_rep, tipo_identificacion_rep, numero_identificacion_rep, nombre_rep, pep_vinculado, descripcion_pep_vinculado, tipo_producto, canal_transaccion, descripcion_movimiento, numero_cuenta, nombre_oficina_soc, codigo_fecha_apertura_cue, codigo_fecha_transaccion, numero_transaccion, valor_total_transaccion, tipo_transaccion, nombre_beneficiario, nombre_institucion_ben, cuenta_ben, pais_destino, nombre_oficina_tra, descripcion_licitud_fon, perfil_economico, descripcion_perfil_eco, perfil_transaccional, descripcion_perfil_tra, perfil, descripcion_perfil) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
        # NOTA: Para evadir duplicados debes crear clave UNIQUE en tu Postgres y luego cambiar tu query a:
        # "INSERT INTO series (...) VALUES (...) ON CONFLICT DO NOTHING"
    }
]

TAMANO_LOTE = 5000         

# ==============================================================================
def ejecutar_mega_insercion_postgres():
    print("Iniciando el MEGA-SCRIPT masivo para PostgreSQL... 🐘")
    tiempo_inicio = time.time()
    
    try:
        # 1. Abrimos conexión a Postgres
        print(f"Conectando al servidor {DATABASE}...")
        conexion = psycopg2.connect(
            host=SERVER,
            database=DATABASE,
            user=USERNAME,
            password=PASSWORD,
            port=PUERTO
        )
        cursor = conexion.cursor()
        
        # EL BUCLE MAGICO: Pasa tarea por tarea de tu lista de arriba
        for i, tarea in enumerate(TAREAS, 1):
            archivo_actual = tarea["archivo"]
            consulta_sql = tarea["query"]
            
            print(f"\n==============================================")
            print(f"▶ TAREA {i}: Procesando el archivo {archivo_actual}")
            print(f"==============================================")
            
            lote_temporal = []
            registros_totales = 0
            
            try:
                with open(archivo_actual, 'r', encoding='utf-8') as archivo:
                    for linea in archivo:
                        linea_limpia = linea.strip()
                        
                        # Extraer datos si la línea viene con el comando "INSERT INTO ..." completo
                        ignorar_mayus = linea_limpia.upper()
                        if " VALUES" in ignorar_mayus:
                            linea_limpia = linea_limpia[ignorar_mayus.find(" VALUES") + 7:].strip()
                        elif ignorar_mayus.startswith("VALUES"):
                            linea_limpia = linea_limpia[6:].strip()
                            
                        # Si no es una tupla, la ignoramos
                        if not linea_limpia.startswith('('): continue
                        
                        # Quitar comas o punto y coma sobrantes del final
                        if linea_limpia.endswith(',') or linea_limpia.endswith(';'): linea_limpia = linea_limpia[:-1]
                        
                        # Cambiar la sintaxis SQL 'NULL' al equivalente en Python 'None'
                        linea_limpia = linea_limpia.replace('NULL', 'None')
                        
                        try:
                            datos_extraidos = ast.literal_eval(linea_limpia)
                            
                            # Contar parámetros en el query (%s)
                            num_placeholders = consulta_sql.count("%s")
                            
                            if len(lote_temporal) == 0:
                                print(f"--> ALERTA: El primer registro tiene {len(datos_extraidos)} columnas de datos. La query espera {num_placeholders}.")
                            
                            # Forzar a que la tupla sea exactamente del tamaño que la base de datos espera
                            if len(datos_extraidos) != num_placeholders:
                                # ¡MÉTODO A PRUEBA DE BALAS PARA VER SI SE DESCUADRAN LAS COLUMNAS!
                                # Extraemos los nombres de las columnas del query para compararlos
                                lista_columnas = consulta_sql[consulta_sql.find("(")+1 : consulta_sql.find(")")].replace(" ", "").split(",")
                                print("\n=========================================================")
                                print(f"🛑 ALERTA: Esta fila del TXT tiene {len(datos_extraidos)} datos, pero la base de datos espera {num_placeholders}.")
                                print("Vamos a comprobar qué dato caería en cada columna para evitar desastres:")
                                
                                # Mostramos las primeras 10 y las últimas 5 columnas para que el usuario compruebe el cuadre
                                for indice in range(min(10, num_placeholders)):
                                    print(f"Columna: {lista_columnas[indice].ljust(30)} => Recibirá el Dato: {datos_extraidos[indice]}")
                                    
                                print("... (mostrando el final)")
                                for indice in range(num_placeholders - 5, num_placeholders):
                                    print(f"Columna: {lista_columnas[indice].ljust(30)} => Recibirá el Dato: {datos_extraidos[indice]}")
                                
                                # Si hay datos extra que sobran al final, los mostramos
                                if len(datos_extraidos) > num_placeholders:
                                    print(f"⚠️ DATOS SOBRANTES QUE QUEDARÍAN FUERA:")
                                    for extra in range(num_placeholders, len(datos_extraidos)):
                                        print(f"-> Dato fuera de lugar: {datos_extraidos[extra]}")
                                
                                print("=========================================================")
                                print("Si los datos SÍ caen en su respectiva columna, entonces todo está en orden y el TXT simplemente trajo columnas extra al final.")
                                print("Si notas que la 'edad' aparece en el campo 'género', ¡entonces los datos se recorrieron!")
                                import sys
                                sys.exit("Script pausado por seguridad. Revisa la tabla de arriba. Quita estas lineas de tu código cuando estés seguro.")
                                
                            lote_temporal.append(datos_extraidos)
                        except Exception:
                            continue
                        
                        # Inserción de lote MASIVA de postgres (100x más rápida)
                        if len(lote_temporal) == TAMANO_LOTE:
                            execute_batch(cursor, consulta_sql, lote_temporal)
                            conexion.commit() 
                            registros_totales += len(lote_temporal)
                            print(f"  --> {registros_totales} registros inyectados...")
                            lote_temporal.clear()

                    if len(lote_temporal) > 0:
                        execute_batch(cursor, consulta_sql, lote_temporal)
                        conexion.commit()
                        registros_totales += len(lote_temporal)
                        print(f"  --> {registros_totales} registros inyectados (Porción final)...")
                        
                print(f" ¡Éxito rotundo en Tarea {i}! Postgres rellenado con {registros_totales} filas.")
                
            except FileNotFoundError:
                print(f" [PELIGRO]: No logré localizar el archivo de postgres de Tarea {i}.")
            except Exception as error_tupla:
                print(f" [PELIGRO]: Tarea {i} colapsó por datos corruptos: {error_tupla}")
                conexion.rollback()
                
        print("\n==============================================")
        print(f" ¡MEGA-SCRIPT POSTGRESQL COMPLETADO!")
        print(f"Tiempo Total: {round(time.time() - tiempo_inicio, 2)} segundos.")
        print("==============================================")

    except Exception as error_grave:
        print(f"\n[ERROR DE POSTGRES]: Ocurrió un problema: {error_grave}")
    
    finally:
        if 'conexion' in locals() and not conexion.closed:
            cursor.close()
            conexion.close()

if __name__ == "__main__":
    ejecutar_mega_insercion_postgres()
