import pandas as pd
import sqlite3
import json

# --- CONFIGURACIÓN ---
ARCHIVO_JSON_ANTIGUO = "./Inspector civitatis 2.0/destinos_civitatis.json"  # Pon el nombre real de tu archivo
DB_NAME = "civitatis_history.db"
FECHA_MANUAL = "2025-12-31"
# ---------------------

def cargar_datos_antiguos():
    try:
        # 1. Leer el archivo local
        with open(ARCHIVO_JSON_ANTIGUO, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)

        # 2. Forzar la fecha antigua
        df['snapshot_date'] = FECHA_MANUAL
        
        # 3. Limpiar columnas numéricas (igual que en el script principal)
        cols_numericas = ['numPeople', 'totalActivities', 'rating', 'numReviews']
        for col in cols_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 4. Insertar en la base de datos
        conn = sqlite3.connect(DB_NAME)
        df.to_sql('destinos', conn, if_exists='append', index=False)
        conn.close()

        print(f"✅ ¡Éxito! Se han cargado {len(df)} registros con fecha {FECHA_MANUAL}")

    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo '{ARCHIVO_JSON_ANTIGUO}'")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    cargar_datos_antiguos()