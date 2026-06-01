import requests
import pandas as pd
import re
import time
from bs4 import BeautifulSoup
import io
from google.cloud import bigquery

# 1. Configuración y Tasa de Cambio
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

print("Obteniendo tasa de cambio CLP/USD...")
try:
    fx = requests.get("https://api.frankfurter.app/latest?from=CLP&to=USD", timeout=10).json()
    tasa_clp_usd = fx["rates"]["USD"]
    print(f"  Tasa actual: 1 CLP = {tasa_clp_usd} USD\n")
except:
    tasa_clp_usd = 1 / 950
    print(f"  No se pudo obtener tasa, usando 1/950\n")

# 2. Leer las URLs generadas por el Job 1
df = pd.read_csv("tours_latam_urls.csv", sep=";")
total = len(df)
print(f"Total tours a procesar: {total}\n")

# 3. Scraping de Precios
results = []
for i, row in df.iterrows():
    tour_id = row["tour_id"]
    url = row["url"]
    
    # Imprime el progreso cada 50 items para no saturar los logs de GitHub
    if i % 50 == 0:
        print(f"Procesando [{i+1}/{total}]...")

    try:
        r = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            results.append({"tour_id": tour_id, "precio_original": f"ERROR_{r.status_code}", "precio_usd": None})
            time.sleep(1)
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        precio_original = None

        for el in soup.find_all(class_=re.compile(r'price', re.I)):
            text = el.get_text(strip=True)
            if any(c.isdigit() for c in text) and len(text) < 60:
                precio_original = text
                break

        precio_usd = None
        if precio_original:
            numeros = re.sub(r'[^\d]', '', precio_original.replace(".", "").replace(",", ""))
            if numeros:
                clp = int(numeros)
                precio_usd = round(clp * tasa_clp_usd * 1.02, 2)

        results.append({
            "tour_id": tour_id,
            "precio_original": precio_original or "NO ENCONTRADO",
            "precio_usd": precio_usd
        })

    except Exception as e:
        results.append({"tour_id": tour_id, "precio_original": "ERROR_TIMEOUT", "precio_usd": None})

    time.sleep(1.5)

# 4. Cruce de Datos
df_precios = pd.DataFrame(results)
df_final = df.merge(df_precios, on="tour_id", how="left")

# Aseguramos que la columna precio_usd sea numérica (FLOAT) para BigQuery
df_final['precio_usd'] = pd.to_numeric(df_final['precio_usd'], errors='coerce')

print(f"\n✅ Scraping completado. Precios encontrados: {df_precios['precio_usd'].notna().sum()}/{total}")

# 5. Carga a BigQuery vía Parquet
# IMPORTANTE: Reemplaza con la ruta de tu tabla en BigQuery
table_id = "datatur.supply.gyg_tours_precios_actual"

bq_client = bigquery.Client()
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    source_format=bigquery.SourceFormat.PARQUET,
)

print("Empaquetando datos a Parquet...")
parquet_buffer = io.BytesIO()
df_final.to_parquet(parquet_buffer, engine="pyarrow", index=False)
parquet_buffer.seek(0)

print(f"Subiendo tabla a BigQuery: {table_id}...")
load_job = bq_client.load_table_from_file(parquet_buffer, table_id, job_config=job_config)
load_job.result() 

print("¡Proceso finalizado con éxito! Tabla actualizada en BQ.")