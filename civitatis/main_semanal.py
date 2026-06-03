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
import hashlib

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
    destinos = cargar_destinos_civitatis([pais_objetivo])
    if not destinos:
        print(f"⚠️ No se encontraron destinos para {pais_objetivo}.")
        return

    timestamp = datetime.now().strftime("%Y%m%d")
    nombre_archivo = f"data/precios_{pais_objetivo.lower()}_{moneda_objetivo.lower()}_{timestamp}.csv"
    
    print(f"🚀 Iniciando scraping para {pais_objetivo} usando {moneda_objetivo}")
    
    # 1. Ejecutar Scraper Playwright
    scraper = CivitatisScraperSemanal()
    await scraper.extract_list(destinos, nombre_archivo, currency_code=moneda_objetivo)
    
    # 2. Transformar y subir a BigQuery
    if os.path.exists(nombre_archivo):
        print(f"\nTransformando datos de {pais_objetivo} para BigQuery...")
        df = pd.read_csv(nombre_archivo)
        
        if df.empty:
            print("El CSV está vacío, no hay datos para subir.")
            return

        # A) Construir la columna "content" con los metadatos que extrajo Civitatis
        def build_content(row):
            parts = []
            if pd.notna(row.get('opiniones')) and row['opiniones'] > 0:
                parts.append(f"Opiniones: {int(row['opiniones'])}")
            if pd.notna(row.get('viajeros')) and row['viajeros'] > 0:
                parts.append(f"Viajeros: {int(row['viajeros'])}")
            if pd.notna(row.get('rating')) and row['rating'] > 0:
                parts.append(f"Rating: {row['rating']} / 10")
            if pd.notna(row.get('cancelacion')) and row['cancelacion'] > 0:
                parts.append(f"Cancelación: {int(row['cancelacion'])} horas")
            return " | ".join(parts) if parts else "Sin información adicional"
            
        df['content'] = df.apply(build_content, axis=1)

        def generar_id_unico(url):
            if pd.isna(url):
                return None
            # Limpiamos la URL (quitamos parámetros '?' y slashes finales)
            url_limpia = str(url).strip().split('?')[0].rstrip('/')
            
            # Generamos un hash MD5
            hash_hex = hashlib.md5(url_limpia.encode('utf-8')).hexdigest()
            
            # Convertimos los primeros 15 caracteres hexadecimales a un número entero.
            # Esto garantiza que sea un número único y que no exceda el límite de Int64 de BigQuery.
            return int(hash_hex[:15], 16)

        # B) Crear el DataFrame final con las columnas exactas
        df_final = pd.DataFrame()
        df_final['id'] = df['url_fuente'].apply(generar_id_unico).astype('Int64')
        df_final['pais'] = df['pais'].astype('string')
        df_final['destino'] = df['destino'].astype('string')
        df_final['actividad'] = df['actividad'].astype('string')
        df_final['precio_usd'] = pd.to_numeric(df['precio_real'], errors='coerce')
        df_final['content'] = df['content'].astype('string')
        df_final['url'] = df['url_fuente'].astype('string')

        # C) Subir a BigQuery con Exponential Backoff
        table_id = "datatur.supply.civitatis_tours_precios_actual" # ⚠️ Verifica este nombre
        bq_client = bigquery.Client()
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", # Agrega a la tabla limpia sin borrar
            source_format=bigquery.SourceFormat.PARQUET,
        )

        parquet_buffer = io.BytesIO()
        df_final.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        
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