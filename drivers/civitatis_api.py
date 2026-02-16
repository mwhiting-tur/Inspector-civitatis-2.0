import requests
import pandas as pd
from datetime import datetime
import urllib3

# Desactivar avisos de seguridad
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. Configuración
API_URL = "https://www.civitatis.com/api/destinations/map"
JSON_FILENAME = "civitatis_api_json.json"

# Añadimos Headers para simular un navegador real y evitar el error 406
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.civitatis.com/"
}

def run_update():
    try:
        print(f"Conectando a la API de Civitatis...")
        
        # 2. Descargar datos con headers
        response = requests.get(API_URL, headers=HEADERS, verify=False)
        
        # Si el error persiste, esto nos dará más detalle
        if response.status_code != 200:
            print(f"Error detectado: {response.status_code}")
            print(f"Contenido del error: {response.text[:200]}")
            response.raise_for_status()

        data = response.json()

        # 3. Convertir a DataFrame
        df = pd.DataFrame(data)

        # 4. Limpieza y preparación
        df['snapshot_date'] = datetime.now().strftime('%Y-%m-%d')
        
        cols_numericas = ['numPeople', 'totalActivities', 'rating', 'numReviews']
        for col in cols_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 5. Guardar como JSONL para BigQuery
        # Usamos lines=True para el formato Newline Delimited JSON
        df.to_json(JSON_FILENAME, orient='records', lines=True, force_ascii=False)

        print(f"---")
        print(f"¡Éxito! Archivo generado correctamente: {JSON_FILENAME}")
        print(f"Registros obtenidos: {len(df)}")
        print(f"---")

    except Exception as e:
        print(f"Error en el proceso: {e}")

if __name__ == "__main__":
    run_update()
    
"""
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

"""