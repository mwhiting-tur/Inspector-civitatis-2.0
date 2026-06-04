import asyncio
import os
import json
import sys
import pandas as pd
import io
import time
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import TooManyRequests
from drivers.civitatis_semanal import CivitatisScraperSemanal

# --- Funciones Auxiliares ---
def cargar_destinos_civitatis(paises):
    ruta_json = 'destinos_civitatis.json'
    if not os.path.exists(ruta_json):
        print(f"❌ Error: No se encontró {ruta_json}")
        return []
        
    with open(ruta_json, 'r', encoding='utf-8') as f:
        todos = json.load(f)
    
    paises_lower = [p.lower() for p in paises]
    return [d for d in todos if d.get('nameCountry', '').lower() in paises_lower]

async def ejecutar_civitatis_semanal(pais_objetivo, moneda_objetivo):
    # 1. Cargar destinos
    destinos = cargar_destinos_civitatis([pais_objetivo])
    if not destinos:
        print(f"⚠️ No se encontraron destinos para {pais_objetivo}.")
        return

    # 2. Generar nombre de archivo
    timestamp = datetime.now().strftime("%Y%m%d")
    nombre_archivo = f"data/precios_{pais_objetivo.lower()}_{moneda_objetivo.lower()}_{timestamp}.csv"
    
    print(f"🚀 Iniciando scraping para {pais_objetivo} usando {moneda_objetivo}")
    
    # 3. Ejecutar Scraper
    scraper = CivitatisScraperSemanal()
    await scraper.extract_list(destinos, nombre_archivo, currency_code=moneda_objetivo)

    # 4. Subir a BigQuery (APPEND)
    if os.path.exists(nombre_archivo):
        print(f"\nPreparando datos de {pais_objetivo} para BigQuery...")
        df = pd.read_csv(nombre_archivo)
        
        if df.empty:
            print("El CSV está vacío, no hay datos para subir.")
            return

        table_id = "datatur.supply.activities_civitatis"
        bq_client = bigquery.Client()
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.PARQUET,
        )

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        
        max_reintentos = 5
        for intento in range(max_reintentos):
            try:
                parquet_buffer.seek(0)
                print(f"Subiendo lote de {pais_objetivo} a BQ (Intento {intento + 1})...")
                load_job = bq_client.load_table_from_file(parquet_buffer, table_id, job_config=job_config)
                load_job.result() 
                print(f"✅ ¡Datos de {pais_objetivo} subidos con éxito a BigQuery!")
                break
            except TooManyRequests as e:
                if intento < max_reintentos - 1:
                    espera = 5 * (2 ** intento) 
                    print(f"⚠️ Tráfico alto (Error 429). Esperando {espera} segundos...")
                    time.sleep(espera)
                else:
                    print(f"❌ Fallo definitivo para {pais_objetivo} tras {max_reintentos} intentos.")
                    raise e
    else:
        print(f"No se generó el archivo {nombre_archivo}. Nada que subir a BQ.")

if __name__ == "__main__":
    if not os.path.exists('data'):
        os.makedirs('data')
    
    if len(sys.argv) >= 3:
        pais_arg = sys.argv[1]
        moneda_arg = sys.argv[2]
        asyncio.run(ejecutar_civitatis_semanal(pais_arg, moneda_arg))
    else:
        print("❌ Error: Faltan argumentos.")
        sys.exit(1)