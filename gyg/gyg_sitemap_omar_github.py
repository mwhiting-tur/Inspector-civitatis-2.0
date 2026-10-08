import io
import re
import sys
import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from google.cloud import bigquery

# Nota: los sitemaps de GYG son públicos (pensados para bots de búsqueda) y no
# bloquean a los runners de GitHub Actions, así que acá no hace falta Apify
# Proxy — a diferencia de scraper_gyg_omar_github.py, que sí lo necesita para
# la API de reviews.


# Destinos a procesar: slug de ciudad de GYG (confirmado contra el sitemap real) → país
DESTINOS = {
    "madrid-l46":        "España",
    "barcelona-l45":     "España",
    "sevilla-l48":       "España",
    "granada-l207":      "España",
    "cordoba-l1689":     "España",
    "roma-l33":          "Italia",
    "florencia-l32":     "Italia",
    "venecia-l35":       "Italia",
    "lisboa-l42":        "Portugal",
    "porto-l151":        "Portugal",
    "paris-l16":         "Francia",
    "londres-l57":       "Reino Unido",
    "amsterdam-l36":     "Países Bajos",
    "nueva-york-l59":    "Estados Unidos",
    "orlando-l191":      "Estados Unidos",
    "los-angeles-l179":  "Estados Unidos",
    "tokio-l193":        "Japón",
    "sidney-l200":       "Australia",
    "bangkok-l169":      "Tailandia",
    "singapur-l170":     "Singapur",
}

SITEMAP_BASE = "https://www.getyourguide.com/es-es/sitemap-activity-{index}.xml"
SITEMAP_INDICES = range(0, 45)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml, text/xml, */*",
}
REQUEST_DELAY = 2
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3


def fetch_sitemap(index):
    url = SITEMAP_BASE.format(index=index)
    for intento in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            locs = [
                el.text.strip()
                for el in root.iter()
                if el.tag in ("{http://www.sitemaps.org/schemas/sitemap/0.9}loc", "loc") and el.text
            ]
            print(f"  [{index:02d}] {len(locs):,} URLs")
            return locs
        except Exception as exc:
            print(f"  [{index:02d}] intento {intento} falló: {exc}")
            if intento < MAX_RETRIES:
                time.sleep(REQUEST_DELAY * intento)
    print(f"  [{index:02d}] ⚠️ se descarta tras {MAX_RETRIES} intentos")
    return []


if __name__ == "__main__":
    print(f"Descargando {len(list(SITEMAP_INDICES))} sitemaps de GYG para {len(DESTINOS)} destinos…\n")

    hits = []
    for idx in SITEMAP_INDICES:
        urls = fetch_sitemap(idx)
        for url in urls:
            for slug, pais in DESTINOS.items():
                if f"/{slug}/" in url:
                    match_id = re.search(r"-t(\d+)/?$", url)
                    if not match_id:
                        break
                    tour_id = match_id.group(1)
                    try:
                        slug_actividad = url.split(f"/{slug}/")[1]
                        slug_actividad = re.sub(r"-t\d+/?$", "", slug_actividad)
                        titulo_bruto = slug_actividad.replace("-", " ").title()
                    except IndexError:
                        titulo_bruto = "Desconocido"
                    hits.append({
                        "pais": pais,
                        "ciudad_id": slug,
                        "tour_id": tour_id,
                        "titulo_referencia": titulo_bruto,
                        "url": url,
                    })
                    break
        time.sleep(REQUEST_DELAY)

    if not hits:
        print("\n⚠️ No se encontró ninguna actividad. Revisá DESTINOS o la conectividad del proxy.")
        sys.exit(1)

    df_total = pd.DataFrame(hits).drop_duplicates(subset=["tour_id"])
    print(f"\n✅ {len(df_total):,} actividades únicas encontradas en total.")

    # Un CSV por destino, para que cada worker de la matriz lea solo el suyo
    for slug in DESTINOS:
        df_slug = df_total[df_total["ciudad_id"] == slug]
        nombre_archivo = f"tours_urls_{slug}.csv"
        df_slug.to_csv(nombre_archivo, index=False, sep=";", encoding="utf-8-sig")
        print(f"  {slug}: {len(df_slug):,} actividades → {nombre_archivo}")

    # Limpiar/crear la tabla de BigQuery con el esquema correcto antes de las cargas en paralelo
    print("\nLimpiando tabla de BigQuery (Truncate) y fijando esquema...")
    table_id = "datatur.supply.reviews_gyg_omar"
    bq_client = bigquery.Client()

    df_schema = pd.DataFrame({
        "review_id":     pd.Series(dtype="Int64"),
        "tour_id":       pd.Series(dtype="Int64"),
        "pais":          pd.Series(dtype="string"),
        "destino":       pd.Series(dtype="string"),
        "actividad":     pd.Series(dtype="string"),
        "rating":        pd.Series(dtype="Int64"),
        "fecha_review":  pd.Series(dtype="string"),
        "pais_usuario":  pd.Series(dtype="string"),
        "url_actividad": pd.Series(dtype="string"),
        "fecha_scraper": pd.Series(dtype="string"),
    })

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET,
    )
    buffer = io.BytesIO()
    df_schema.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    load_job = bq_client.load_table_from_file(buffer, table_id, job_config=job_config)
    load_job.result()
    print("Tabla limpia y lista para recibir datos en paralelo.")
