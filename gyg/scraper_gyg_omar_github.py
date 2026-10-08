import io
import json
import os
import random
import re
import sys
import time
import uuid
from datetime import datetime

import pandas as pd
import requests
from google.cloud import bigquery

if len(sys.argv) < 2:
    print("Error: no se proporcionó el slug de destino (ej: madrid-l46).")
    sys.exit(1)

slug = sys.argv[1]
print(f"=== INICIANDO SCRAPER DE REVIEWS PARA: {slug} ===")

fecha_inicio = datetime(2025, 5, 1)  # reviews desde esta fecha hasta ahora

# GYG bloquea las IPs de los runners de GitHub Actions. Enrutamos cada
# request por Apify Proxy (grupo residencial) con una sesión distinta.
APIFY_PROXY_PASSWORD = os.environ.get("APIFY_PROXY_PASSWORD")
APIFY_PROXY_GROUPS = os.environ.get("APIFY_PROXY_GROUPS", "RESIDENTIAL")

if not APIFY_PROXY_PASSWORD:
    print("Error: Falta la variable de entorno APIFY_PROXY_PASSWORD (secret de GitHub Actions).")
    sys.exit(1)


def build_proxies():
    session_id = uuid.uuid4().hex[:16]
    proxy_url = (
        f"http://groups-{APIFY_PROXY_GROUPS},session-{session_id}:"
        f"{APIFY_PROXY_PASSWORD}@proxy.apify.com:8000"
    )
    return {"http": proxy_url, "https": proxy_url}


url_api_post = "https://travelers-api.getyourguide.com/user-interface/activity-details-page/blocks?ranking_uuid=8db3d7f9-ae97-4e8e-9782-086c43dd5f1b"

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'Origin': 'https://www.getyourguide.com',
    'Referer': 'https://www.getyourguide.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'accept-currency': 'EUR',
    'accept-language': 'es-ES',
    'geo-ip-country': 'ES',
    'partner-id': 'CD951',
    'visitor-id': 'F97X0158YNG8999WZEDF645USNKBJAZD',
    'visitor-platform': 'desktop',
    'x-gyg-app-type': 'Web',
}


def buscar_reseñas_en_json(datos):
    """Busca recursivamente bloques de tipo 'review' en el JSON."""
    reseñas = []
    if isinstance(datos, dict):
        if datos.get("type") == "review" and "author" in datos:
            reseñas.append(datos)
        else:
            for value in datos.values():
                reseñas.extend(buscar_reseñas_en_json(value))
    elif isinstance(datos, list):
        for item in datos:
            reseñas.extend(buscar_reseñas_en_json(item))
    return reseñas


def parsear_fecha(review):
    tracker = review.get('onImpressionTrackingEvent', {}).get('properties', {})
    fecha_str = tracker.get('review_date', '')
    if not fecha_str:
        return None
    try:
        return datetime.strptime(fecha_str.split('T')[0], "%Y-%m-%d")
    except ValueError:
        return None


def hacer_peticion(tour_id, offset, orden, filtros):
    payload_dict = {
        "payload": {
            "activityId": tour_id,
            "templateName": "ActivityDetails",
            "contentIdentifier": "next-reviews-page",
            "additionalDetailsSelectedLanguage": "es-ES",
            "reviewsOffset": offset,
            "reviewsLimit": 20,
            "selectedReviewsSortingOrder": orden,
            "selectedReviewsFilters": filtros,
            "participantsLanguage": "es-ES",
            "reviewsExperiments": [{"key": "rvw-display-traveler-type-in-reviews", "isEnabled": False}],
        }
    }
    cuerpo = json.dumps(payload_dict, separators=(',', ':'))
    return requests.post(url_api_post, headers=headers, data=cuerpo, timeout=20, proxies=build_proxies())


def paginar(tour_id, orden, filtros, vistas):
    """
    Pagina en una dirección (date_desc o date_asc) con los filtros dados.
    Devuelve (lista de (review, fecha), tope_alcanzado).
    tope_alcanzado=True significa que la API cortó en offset ~320 sin haber
    llegado al límite real de fechas (o, para date_asc, sin reconectar con
    reviews ya vistas) — es decir, puede haber reviews sin capturar.
    """
    resultados = []
    offset = 0
    while True:
        try:
            resp = hacer_peticion(tour_id, offset, orden, filtros)
        except Exception as e:
            print(f"  ⚠️ Error en ID {tour_id} (offset {offset}, {orden}, {filtros}): {e}")
            return resultados, False

        if resp.status_code != 200:
            return resultados, False

        reseñas = buscar_reseñas_en_json(resp.json())
        if not reseñas:
            return resultados, True  # vacío: puede ser fin real o tope de la API

        detener = False
        for review in reseñas:
            review_id = review.get('reviewId')
            fecha_obj = parsear_fecha(review)
            if fecha_obj is None:
                continue

            if orden == "date_desc":
                if fecha_obj < fecha_inicio:
                    detener = True
                    break
                if review_id not in vistas:
                    vistas.add(review_id)
                    resultados.append((review, fecha_obj))
            else:  # date_asc
                if review_id in vistas:
                    detener = True  # reconectamos con territorio ya cubierto: sin huecos
                    break
                if fecha_obj >= fecha_inicio:
                    vistas.add(review_id)
                    resultados.append((review, fecha_obj))

        if detener:
            return resultados, False

        offset += 20
        time.sleep(random.uniform(0.8, 1.5))


def obtener_reviews_actividad(tour_id):
    """
    Trae todas las reviews en rango de fecha para una actividad. Primero sin
    filtro; si choca con el tope de ~320 antes de llegar a fecha_inicio,
    escala partiendo por cada calificación (1 a 5 estrellas), y si una
    partición individual también se topa, agrega un pase inverso (date_asc).
    """
    vistas = set()
    resultados, tope = paginar(tour_id, "date_desc", {}, vistas)

    if not tope:
        return resultados, False, False

    hubo_hueco = False
    for estrella in (1, 2, 3, 4, 5):
        sub, tope_estrella = paginar(tour_id, "date_desc", {"ratings": [estrella]}, vistas)
        resultados.extend(sub)
        if tope_estrella:
            sub_asc, tope_asc = paginar(tour_id, "date_asc", {"ratings": [estrella]}, vistas)
            resultados.extend(sub_asc)
            if tope_asc:
                hubo_hueco = True
                print(f"  ⚠️ ID {tour_id}: posible hueco de reviews ({estrella}★) — "
                      f"la API de GYG no deja traer más de ~320 resultados por combinación de filtros.")

    return resultados, True, hubo_hueco


def limpiar_destino(ciudad_id):
    texto = re.sub(r'-l\d+$', '', str(ciudad_id))
    return texto.replace('-', ' ').title()


# --- Leer el CSV de actividades de este destino (generado por gyg_sitemap_omar_github.py) ---
archivo_csv = f"tours_urls_{slug}.csv"
try:
    df_tours = pd.read_csv(archivo_csv, sep=";")
except FileNotFoundError:
    print(f"No se encontró {archivo_csv}. Saliendo...")
    sys.exit(0)

total = len(df_tours)
if total == 0:
    print(f"No hay actividades para {slug}. Finalizando job.")
    sys.exit(0)

print(f"Total actividades a procesar para {slug}: {total}\n")

filas = []
for i, row in df_tours.iterrows():
    tour_id = int(row['tour_id'])
    pais = str(row['pais'])
    destino = limpiar_destino(row['ciudad_id'])
    actividad = str(row['titulo_referencia'])
    url_act = str(row['url'])

    if i % 25 == 0:
        print(f"Procesando [{i + 1}/{total}]...")

    resultados, escalo, hueco = obtener_reviews_actividad(tour_id)

    for review, fecha_obj in resultados:
        autor_texto = review.get('author', {}).get('title', {}).get('text', '')
        autor_texto = autor_texto.replace(' – ', ' - ').replace(' — ', ' - ')
        pais_usuario = autor_texto.split(' - ')[-1].strip() if ' - ' in autor_texto else "Desconocido"

        filas.append({
            "review_id": review.get('reviewId'),
            "tour_id": tour_id,
            "pais": pais,
            "destino": destino,
            "actividad": actividad,
            "rating": review.get('rating'),
            "fecha_review": fecha_obj.strftime("%Y-%m-%d"),
            "pais_usuario": pais_usuario,
            "url_actividad": url_act,
        })

    etiqueta = " [escaló]" if escalo else ""
    etiqueta += " [hueco residual]" if hueco else ""
    print(f"  ✅ ID {tour_id}: {len(resultados)} reviews{etiqueta}")

df_final = pd.DataFrame(filas, columns=[
    "review_id", "tour_id", "pais", "destino", "actividad",
    "rating", "fecha_review", "pais_usuario", "url_actividad",
])

df_final['review_id'] = pd.to_numeric(df_final['review_id'], errors='coerce').astype('Int64')
df_final['tour_id'] = pd.to_numeric(df_final['tour_id'], errors='coerce').astype('Int64')
df_final['rating'] = pd.to_numeric(df_final['rating'], errors='coerce').astype('Int64')
for col in ['pais', 'destino', 'actividad', 'fecha_review', 'pais_usuario', 'url_actividad']:
    df_final[col] = df_final[col].astype('string')
df_final['fecha_scraper'] = datetime.now().strftime('%Y-%m-%d')
df_final['fecha_scraper'] = df_final['fecha_scraper'].astype('string')

print(f"\nTotal reviews recolectadas para {slug}: {len(df_final):,}")

if df_final.empty:
    print("Nada para subir a BigQuery. Finalizando.")
    sys.exit(0)

# Carga a BigQuery (sumando al final de la tabla, junto con los demás destinos de la matriz)
table_id = "datatur.supply.reviews_gyg_omar"
bq_client = bigquery.Client()
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.PARQUET,
)
buffer = io.BytesIO()
df_final.to_parquet(buffer, engine="pyarrow", index=False)
buffer.seek(0)

print(f"Subiendo lote de {slug} a BigQuery (APPEND)...")
load_job = bq_client.load_table_from_file(buffer, table_id, job_config=job_config)
load_job.result()
print(f"✅ ¡Proceso de {slug} finalizado con éxito!")
