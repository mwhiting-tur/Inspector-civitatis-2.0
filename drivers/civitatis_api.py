import requests
import pandas as pd
import sqlite3
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. Configuración
API_URL = "https://civitatis.com/api/destinations/map"
DB_NAME = "civitatis_history.db"

def run_update():
    try:
        # 2. Descargar datos de la API
        print(f"Conectando a la API...")
        response = requests.get(API_URL, verify=False)
        response.raise_for_status() # Lanza error si la descarga falla
        data = response.json()

        # 3. Convertir a Tabla (DataFrame)
        df = pd.DataFrame(data)

        # 4. Limpieza y preparación
        # Añadimos la columna de fecha para poder comparar semanas después
        df['snapshot_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Convertimos columnas clave a números (por si vienen como texto)
        cols_numericas = ['numPeople', 'totalActivities', 'rating', 'numReviews']
        for col in cols_numericas:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 5. Guardar en la Base de Datos (SQLite)
        # 'append' significa que añade los datos al final de la tabla
        conn = sqlite3.connect(DB_NAME)
        df.to_sql('destinos', conn, if_exists='append', index=False)
        conn.close()

        print(f"¡Éxito! Se han guardado {len(df)} destinos para la fecha {df['snapshot_date'].iloc[0]}")

    except Exception as e:
        print(f"Error en el proceso: {e}")

if __name__ == "__main__":
    run_update()