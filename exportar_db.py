import sqlite3
import pandas as pd

# 1. Configuración (Cambia los nombres si es necesario)
db_name = 'civitatis_history.db'
table_name = 'destinos'
output_file = 'civitatis_historico_json_exportado.csv'

try:
    # 2. Conectar a la base de datos
    conn = sqlite3.connect(db_name)
    
    # 3. Leer la tabla y guardarla en un CSV
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"✅ ¡Éxito! El archivo '{output_file}' ha sido creado.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    conn.close()