import sys
import requests
import pandas as pd
import re
import time
from bs4 import BeautifulSoup
import io
from google.cloud import bigquery

# Recibir el nombre del país desde el argumento del sistema (GitHub Actions Matrix)
if len(sys.argv) < 2:
    print("Error: No se proporcionó el nombre del país.")
    sys.exit(1)

country_key = sys.argv[1]
print(f"=== INICIANDO SCRAPER PARA: {country_key.upper()} ===")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
}

def extraer_contenido(soup):
    partes = []
    for seccion in soup.find_all('section'):
        texto_completo = seccion.get_text(separator=" ", strip=True)
        if texto_completo and len(texto_completo) > 40 and "También podría gustarte" not in texto_completo:
            encabezado = seccion.find(['h2', 'h3'])
            if encabezado:
                titulo = encabezado.get_text(strip=True)
                texto_limpio = texto_completo.replace(titulo, "", 1).strip()
                texto_limpio = re.sub(r'\s+', ' ', texto_limpio)
                partes.append(f"{titulo}: {texto_limpio}")
            else:
                partes.append(re.sub(r'\s+', ' ', texto_completo))
    texto_final = " | ".join(partes)
    return texto_final[:6000]

# Leer exclusivamente el CSV de este país
archivo_csv = f"tours_urls_{country_key}.csv"
try:
    df = pd.read_csv(archivo_csv, sep=";")
except FileNotFoundError:
    print(f"No se encontró {archivo_csv}. Saliendo...")
    sys.exit(0)

total = len(df)
if total == 0:
    print(f"No hay actividades para {country_key}. Finalizando Job.")
    sys.exit(0)

print(f"Total tours a procesar para {country_key}: {total}\n")

results = []
for i, row in df.iterrows():
    tour_id = row["tour_id"]
    url = row["url"]
    
    if i % 50 == 0:
        print(f"Procesando [{i+1}/{total}]...")

    try:
        r = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            results.append({"tour_id": tour_id, "precio_usd": None, "content": None})
            time.sleep(1)
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        
        # Precio
        precio_original = None
        for el in soup.find_all(class_=re.compile(r'price', re.I)):
            text = el.get_text(strip=True)
            if any(c.isdigit() for c in text) and len(text) < 60:
                precio_original = text
                break

        precio_usd = None
        if precio_original:
            texto_limpio = precio_original.replace(".", "").replace(",", "")
            numeros = re.findall(r'\d+', texto_limpio)
            if numeros:
                precio_usd = float(numeros[0])
                
        # Contenido
        contenido_html = extraer_contenido(soup)

        results.append({
            "tour_id": tour_id,
            "precio_usd": precio_usd,
            "content": contenido_html if contenido_html else "NO ENCONTRADO"
        })

    except Exception as e:
        results.append({"tour_id": tour_id, "precio_usd": None, "content": f"ERROR"})

    time.sleep(1.5)

df_precios = pd.DataFrame(results)
df_final = df.merge(df_precios, on="tour_id", how="left")

def clean_destino(text):
    if pd.isna(text): return text
    text = str(text)
    text = re.sub(r'-l\d+$', '', text) 
    return text.replace('-', ' ').title() 

df_final['ciudad_id'] = df_final['ciudad_id'].apply(clean_destino)

df_final = df_final.rename(columns={
    'tour_id': 'id',
    'ciudad_id': 'destino',
    'titulo_referencia': 'actividad'
})

df_final['id'] = pd.to_numeric(df_final['id'], errors='coerce').astype('Int64')
df_final['precio_usd'] = pd.to_numeric(df_final['precio_usd'], errors='coerce')

# Forzar explícitamente a tipo string las columnas de texto
df_final['pais'] = df_final['pais'].astype('string')
df_final['destino'] = df_final['destino'].astype('string')
df_final['actividad'] = df_final['actividad'].astype('string')
df_final['content'] = df_final['content'].astype('string')
df_final['url'] = df_final['url'].astype('string')

columnas_finales = ['id', 'pais', 'destino', 'actividad', 'precio_usd', 'content', 'url']
df_final = df_final[columnas_finales]

# Carga a BigQuery (AGREGANDO AL FINAL DE LA TABLA)
table_id = "datatur.supply.gyg_tours_precios_actual"
bq_client = bigquery.Client()

# ATENCIÓN AQUÍ: Usamos WRITE_APPEND para que los 13 servidores sumen sus datos sin borrarse entre sí
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.PARQUET,
)

parquet_buffer = io.BytesIO()
df_final.to_parquet(parquet_buffer, engine="pyarrow", index=False)
parquet_buffer.seek(0)

print(f"\nSubiendo lote de {country_key} a BigQuery (APPEND)...")
load_job = bq_client.load_table_from_file(parquet_buffer, table_id, job_config=job_config)
load_job.result() 

print(f"✅ ¡Proceso de {country_key} finalizado con éxito!")