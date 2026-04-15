import psycopg2
from psycopg2.extras import execute_batch
import time
import ast

# 1. CONFIGURACIÓN DE POSTGRESQL
SERVER = 'localhost'      
DATABASE = 'base_prueba'       # Cambia esto por tu base de datos postgres real
USERNAME = 'postgres'       # Usuario por defecto de Postgres
PASSWORD = '14789632'     # Tu contraseña de Postgres
PUERTO = '5432'             # Puerto por defecto de Postgres

# 2. EL MEGA-CEREBRO: LISTA DE TAREAS (Para múltiples archivos Postgres)
TAREAS = [
    {
        "archivo": r"C:\Users\Asus\OneDrive\Escritorio\INSERT INTO Series (titulo, descrip.txt",
        "query": "INSERT INTO series (titulo, descripcion, año_lanzamiento, genero) VALUES (%s, %s, %s, %s)" 
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
                        
                        if not linea_limpia.startswith('('): continue
                        if linea_limpia.endswith(',') or linea_limpia.endswith(';'): linea_limpia = linea_limpia[:-1]
                        linea_limpia = linea_limpia.replace('NULL', 'None')
                        
                        try:
                            datos_extraidos = ast.literal_eval(linea_limpia)
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
                        
                print(f"✅ ¡Éxito rotundo en Tarea {i}! Postgres rellenado con {registros_totales} filas.")
                
            except FileNotFoundError:
                print(f"❌ [PELIGRO]: No logré localizar el archivo de postgres de Tarea {i}.")
            except Exception as error_tupla:
                print(f"❌ [PELIGRO]: Tarea {i} colapsó por datos corruptos: {error_tupla}")
                conexion.rollback()
                
        print("\n==============================================")
        print(f"🏆 ¡MEGA-SCRIPT POSTGRESQL COMPLETADO!")
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
