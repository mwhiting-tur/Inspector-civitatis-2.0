import re
import time
import xml.etree.ElementTree as ET
import pandas as pd
import requests
import io
from google.cloud import bigquery

SITEMAP_BASE = "https://www.getyourguide.com/es-es/sitemap-activity-{index}.xml"
SITEMAP_INDICES = range(0, 45)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/xml, text/xml, */*",
}
REQUEST_DELAY = 1.5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# --- PEGA AQUÍ TUS LISTAS DE CIUDADES ---
ciudades_brasil = ["maxaranguape-l133602", "natal-l2112"] # Reemplaza con tu lista completa...
ciudades_mexico = ["puerto-angel-l193335"] # Reemplaza con tu lista completa...
ciudades_argentina = ["aeropuerto-internacional-de-ushuaia-malvinas-argentinas-l1851"]
ciudades_chile = ["cerro-castillo-l192281"]
ciudades_colombia = ["santa-marta-l32129"]
ciudades_peru = ["mollepata-l215653"]
ciudades_costa_rica = ["grecia-l146334"]
ciudades_panama = ["pedasi-l234818"]
ciudades_ecuador = ["ingapirca-ecuador-l184142"]
ciudades_paraguay = ["ciudad-del-este-l150275"]
ciudades_uruguay = ["ciudad-de-la-costa-l215277"]
ciudades_bolivia = ["uyuni-l32222"]
ciudades_republica_dominicana = ["bani-l107607"]

# Diccionario con llaves limpias (sin espacios) para empatar con la Matriz de GitHub
PAISES = {
    "Brasil": ciudades_brasil,
    "Mexico": ciudades_mexico,
    "Argentina": ciudades_argentina,
    "Chile": ciudades_chile,
    "Colombia": ciudades_colombia,
    "Peru": ciudades_peru,
    "Costa_Rica": ciudades_costa_rica,
    "Panama": ciudades_panama,
    "Ecuador": ciudades_ecuador,
    "Paraguay": ciudades_paraguay,
    "Uruguay": ciudades_uruguay,
    "Bolivia": ciudades_bolivia,
    "Republica_Dominicana": ciudades_republica_dominicana,
}

def fetch_sitemap(index: int) -> list[str]:
    url = SITEMAP_BASE.format(index=index)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            locs = [el.text.strip() for el in root.iter() if el.tag in ("{http://www.sitemaps.org/schemas/sitemap/0.9}loc", "loc") and el.text]
            print(f"  [{index:02d}] fetched {len(locs):,} URLs")
            return locs
        except Exception as exc:
            time.sleep(REQUEST_DELAY * attempt)
    return []

def match_cities(urls: list[str], city_slugs: list[str], pais_original: str) -> list[dict]:
    hits = []
    slug_set = set(city_slugs)
    for url in urls:
        for ciudad_slug in slug_set:
            if f"/{ciudad_slug}/" in url:
                match_id = re.search(r"-t(\d+)/?$", url)
                tour_id = match_id.group(1) if match_id else "N/A"
                try:
                    slug_actividad = url.split(f"/{ciudad_slug}/")[1]
                    slug_actividad = re.sub(r"-t\d+/?$", "", slug_actividad)
                    titulo_bruto = slug_actividad.replace("-", " ").title()
                except IndexError:
                    titulo_bruto = "Desconocido"
                hits.append({
                    "pais": pais_original.replace("_", " "), # Restaura el espacio (Ej: Costa Rica)
                    "ciudad_id": ciudad_slug,
                    "tour_id": tour_id,
                    "titulo_referencia": titulo_bruto,
                    "url": url,
                })
                break
    return hits

if __name__ == "__main__":
    # 1. Extraer todas las URLs
    all_hits = []
    for idx in SITEMAP_INDICES:
        urls = fetch_sitemap(idx)
        for pais_key, slugs in PAISES.items():
            all_hits.extend(match_cities(urls, slugs, pais_key))
        time.sleep(REQUEST_DELAY)

    # 2. Separar y guardar un CSV por cada país
    df_total = pd.DataFrame(all_hits).drop_duplicates(subset=["tour_id"])
    
    for pais_key in PAISES.keys():
        pais_real = pais_key.replace("_", " ")
        df_pais = df_total[df_total['pais'] == pais_real]
        nombre_archivo = f"tours_urls_{pais_key}.csv"
        df_pais.to_csv(nombre_archivo, index=False, sep=";", encoding="utf-8-sig")
        print(f"Generado {nombre_archivo} con {len(df_pais)} actividades.")

    # 3. Limpiar tabla de BigQuery para recibir las inserciones paralelas
    print("\nLimpiando tabla de BigQuery (Truncate) y fijando esquema...")
    table_id = "datatur.supply.gyg_tours_precios_actual"
    bq_client = bigquery.Client()
    
    # Creamos un DF vacío pero con la estructura estricta para asegurar que la tabla exista y esté en blanco
    df_schema = pd.DataFrame({
        'id': pd.Series(dtype='Int64'),
        'pais': pd.Series(dtype='string'),
        'destino': pd.Series(dtype='string'),
        'actividad': pd.Series(dtype='string'),
        'precio_usd': pd.Series(dtype='float64'),
        'content': pd.Series(dtype='string'),
        'url': pd.Series(dtype='string')
    })
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET,
    )
    parquet_buffer = io.BytesIO()
    df_schema.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)
    
    load_job = bq_client.load_table_from_file(parquet_buffer, table_id, job_config=job_config)
    load_job.result()
    print("Tabla limpia y lista para recibir datos en paralelo.")