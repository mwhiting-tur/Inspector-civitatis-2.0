import requests
import pandas as pd
import re
import time
from bs4 import BeautifulSoup
import io
from google.cloud import bigquery

# 1. Configuración
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Forzamos idioma US para asegurar precios en USD, pero el contenido HTML general lo intentará traer en español
    "Accept-Language": "es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
}

# --- FUNCIÓN DE EXTRACCIÓN DE CONTENIDO ---
def extraer_contenido(soup):
    partes = []
    # GYG utiliza etiquetas <section> para organizar "Qué harás", "En detalle", etc.
    for seccion in soup.find_all('section'):
        texto_completo = seccion.get_text(separator=" ", strip=True)
        
        # Filtramos secciones vacías o irrelevantes (como sugerencias de otros tours al final de la página)
        if texto_completo and len(texto_completo) > 40 and "También podría gustarte" not in texto_completo:
            
            # Si la sección tiene un título claro (h2 o h3), lo usamos para estructurar
            encabezado = seccion.find(['h2', 'h3'])
            if encabezado:
                titulo = encabezado.get_text(strip=True)
                # Quitamos el título del texto general para que no se repita
                texto_limpio = texto_completo.replace(titulo, "", 1).strip()
                # Limpiamos dobles espacios
                texto_limpio = re.sub(r'\s+', ' ', texto_limpio)
                partes.append(f"{titulo}: {texto_limpio}")
            else:
                partes.append(re.sub(r'\s+', ' ', texto_completo))
                
    # Unimos todo con el separador acordado y limitamos a 6000 caracteres por seguridad
    texto_final = " | ".join(partes)
    return texto_final[:6000]

# 2. Leer las URLs generadas por el Job 1
df = pd.read_csv("tours_latam_urls.csv", sep=";")
total = len(df)
print(f"Total tours a procesar: {total}\n")

# 3. Scraping de Precios y Contenido
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
        
        # A) Extracción de Precio
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
                
        # B) Extracción de Contenido Descriptivo
        contenido_html = extraer_contenido(soup)

        results.append({
            "tour_id": tour_id,
            "precio_usd": precio_usd,
            "content": contenido_html if contenido_html else "NO ENCONTRADO"
        })

    except Exception as e:
        results.append({"tour_id": tour_id, "precio_usd": None, "content": f"ERROR: {str(e)[:50]}"})

    time.sleep(1.5)

# 4. Transformación y Cruce de Datos
df_precios = pd.DataFrame(results)
df_final = df.merge(df_precios, on="tour_id", how="left")

# --- LIMPIEZA Y FORMATEO ---
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
# Aseguramos que content sea texto puro para Parquet
df_final['content'] = df_final['content'].astype(str)

# Orden exacto de columnas
columnas_finales = ['id', 'pais', 'destino', 'actividad', 'precio_usd', 'content', 'url']
df_final = df_final[columnas_finales]

print(f"\n✅ Scraping completado.")
print(f"   Precios encontrados: {df_final['precio_usd'].notna().sum()}/{total}")
print(f"   Contenidos extraídos: {(df_final['content'] != 'NO ENCONTRADO').sum()}/{total}")

# 5. Carga a BigQuery vía Parquet
# IMPORTANTE: Reemplaza con la ruta de tu tabla en BigQuery
table_id = "datatur.supply.gyg_tours_precios_actual"

bq_client = bigquery.Client()
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    source_format=bigquery.SourceFormat.PARQUET,
)

print("\nEmpaquetando datos a Parquet...")
parquet_buffer = io.BytesIO()
df_final.to_parquet(parquet_buffer, engine="pyarrow", index=False)
parquet_buffer.seek(0)

print(f"Subiendo tabla a BigQuery: {table_id}...")
load_job = bq_client.load_table_from_file(parquet_buffer, table_id, job_config=job_config)
load_job.result() 

print("¡Proceso finalizado con éxito! Tabla actualizada en BQ.")