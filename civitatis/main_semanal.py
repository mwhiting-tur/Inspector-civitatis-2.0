import asyncio
import os
import json
import sys
import pandas as pd
import io
import time
import hashlib
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import TooManyRequests
from drivers.civitatis_semanal import CivitatisScraperSemanal

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
    
    print(f"🚀 Iniciando scraping de lista para {pais_objetivo} usando {moneda_objetivo}")
    
    # 1. Ejecutar Scraper Playwright (Rápido, solo listas)
    scraper = CivitatisScraperSemanal()
    await scraper.extract_list(destinos, nombre_archivo, currency_code=moneda_objetivo)
    
    # 2. Transformar, extraer descripciones y subir a BigQuery
    if os.path.exists(nombre_archivo):
        print(f"\nTransformando datos de {pais_objetivo} para BigQuery...")
        df = pd.read_csv(nombre_archivo)
        
        if df.empty:
            print("El CSV está vacío, no hay datos para subir.")
            return

        # =================================================================
        # NUEVO BLOQUE: Extracción de Descripciones y Detalles en 2do Plano
        # =================================================================
        print(f"Obteniendo descripciones para {len(df)} actividades (esto tomará unos minutos)...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "es-ES,es;q=0.9"
        }
        
        descripciones_extraidas = []
        total = len(df)
        
        for i, url in enumerate(df['url_fuente']):
            if i % 50 == 0:
                print(f"  Procesando textos [{i+1}/{total}]...")
            
            texto_final = ""
            try:
                # Hacemos una petición rápida a la página de la actividad
                r = requests.get(url, headers=headers, timeout=15)
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, "html.parser")
                    partes_texto = []
                    
                    # A) Buscar sección de Descripción e Itinerario
                    sec_desc = soup.find(id="descripcion")
                    if sec_desc:
                        partes_texto.append("Descripción: " + sec_desc.get_text(separator=" ", strip=True))
                        
                    # B) Buscar sección de Detalles (Duración, Inclusiones, etc.)
                    sec_det = soup.find(id="detalles")
                    if sec_det:
                        # LIMPIEZA: Eliminamos el bot de chat y elementos basura antes de extraer texto
                        for tag_basura in sec_det.find_all(['civitatis-bot-ui', 'script', 'style', 'div', 'svg']):
                            # Excluimos div genéricos de la destrucción para no borrar el contenido real, 
                            # solo matamos explícitamente el bot y scripts.
                            if tag_basura.name in ['civitatis-bot-ui', 'script', 'style', 'svg']:
                                tag_basura.decompose()
                                
                        # Usamos " | " como separador para que características como "Duración | 3 horas" se lean claro
                        partes_texto.append("Detalles: " + sec_det.get_text(separator=" | ", strip=True))
                        
                    # Unir y limpiar excesos de espacios
                    texto_unido = " || ".join(partes_texto)
                    texto_final = re.sub(r'\s+', ' ', texto_unido)[:5000] 
            except Exception as e:
                pass
            
            descripciones_extraidas.append(texto_final)
            time.sleep(1) # Pausa amigable para no saturar los servidores de Civitatis
            
        df['texto_html'] = descripciones_extraidas
        # =================================================================


        # A) Construir la columna "content" final (Metadata + Descripción)
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
                
            meta = " | ".join(parts) if parts else "Sin metadata"
            
            # Anexamos la descripción que acabamos de extraer
            desc = str(row.get('texto_html', ''))
            if desc and desc != 'nan' and desc.strip():
                return f"{meta} || {desc}"
            return meta
            
        df['content'] = df.apply(build_content, axis=1)

        # B) Generador de ID Único Permanente (Hash MD5)
        def generar_id_unico(url):
            if pd.isna(url): return None
            # Limpiamos parámetros de rastreo
            url_limpia = str(url).strip().split('?')[0].rstrip('/')
            hash_hex = hashlib.md5(url_limpia.encode('utf-8')).hexdigest()
            return int(hash_hex[:15], 16) # Convertimos a entero seguro para Int64

        # C) Crear el DataFrame final con las columnas exactas
        df_final = pd.DataFrame()
        df_final['id'] = df['url_fuente'].apply(generar_id_unico).astype('Int64')
        df_final['pais'] = df['pais'].astype('string')
        df_final['destino'] = df['destino'].astype('string')
        df_final['actividad'] = df['actividad'].astype('string')
        df_final['precio_usd'] = pd.to_numeric(df['precio_real'], errors='coerce')
        df_final['content'] = df['content'].astype('string')
        df_final['url'] = df['url_fuente'].astype('string')

        # D) Subir a BigQuery con Exponential Backoff
        table_id = "datatur.supply.civitatis_tours_precios_actual" 
        bq_client = bigquery.Client()
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", 
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