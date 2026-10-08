import re
import time
import random
import os
import json
import threading
import concurrent.futures
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from datetime import datetime

# ── SSL: usa el llavero del sistema (maneja certificados de proxys corporativos) ──
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print("⚠️  truststore no instalado — verificación SSL desactivada. Instalalo con: pip3 install truststore")

# --- CONFIGURACIÓN ---
archivo_tours = 'gyg/tours_omar_IDs.csv'
archivo_salida = 'gyg/reviews_omar_FINAL.csv'

fecha_inicio = datetime(2025, 5, 1)  # reviews desde esta fecha hasta ahora

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

# ── Sitemaps (para armar la lista de actividades de estos destinos) ──
SITEMAP_BASE = "https://www.getyourguide.com/es-es/sitemap-activity-{index}.xml"
SITEMAP_INDICES = range(0, 45)
SITEMAP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml, text/xml, */*",
}
SITEMAP_REQUEST_DELAY = 2
SITEMAP_TIMEOUT = 30
SITEMAP_MAX_RETRIES = 3

# ── API de reviews ──
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
    'x-gyg-app-type': 'Web'
}

lock_csv = threading.Lock()

# ──────────────────────────────────────────────────────────────────────────────
# PASO 1: armar la lista de actividades de los destinos pedidos (vía sitemaps)
# ──────────────────────────────────────────────────────────────────────────────

def fetch_sitemap(index):
    url = SITEMAP_BASE.format(index=index)
    for intento in range(1, SITEMAP_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=SITEMAP_HEADERS, timeout=SITEMAP_TIMEOUT)
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
            if intento < SITEMAP_MAX_RETRIES:
                time.sleep(SITEMAP_REQUEST_DELAY * intento)
    print(f"  [{index:02d}] ⚠️ se descarta tras {SITEMAP_MAX_RETRIES} intentos")
    return []


def construir_lista_tours():
    if os.path.exists(archivo_tours):
        print(f"📄 Reutilizando lista de actividades existente: {archivo_tours}")
        return

    print(f"Descargando {len(list(SITEMAP_INDICES))} sitemaps de GYG para ubicar los destinos pedidos…\n")
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
        time.sleep(SITEMAP_REQUEST_DELAY)

    if not hits:
        print("\n⚠️ No se encontró ninguna actividad para estos destinos. Verificá los slugs en DESTINOS.")
        exit()

    df = pd.DataFrame(hits).drop_duplicates(subset=["tour_id"])
    df.to_csv(archivo_tours, index=False, sep=";", encoding="utf-8-sig")
    print(f"\n✅ {len(df):,} actividades únicas encontradas → {archivo_tours}")


# ──────────────────────────────────────────────────────────────────────────────
# PASO 2: scraping de reviews, con escalado automático al tope de 320
# ──────────────────────────────────────────────────────────────────────────────

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
            "reviewsExperiments": [{"key": "rvw-display-traveler-type-in-reviews", "isEnabled": False}]
        }
    }
    cuerpo = json.dumps(payload_dict, separators=(',', ':'))
    return requests.post(url_api_post, headers=headers, data=cuerpo, timeout=15)


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
                    # Reconectamos con territorio ya cubierto por el pase date_desc: sin huecos.
                    detener = True
                    break
                if fecha_obj >= fecha_inicio:
                    vistas.add(review_id)
                    resultados.append((review, fecha_obj))
                # si fecha_obj < fecha_inicio seguimos avanzando sin guardar

        if detener:
            return resultados, False  # llegamos al límite real de fechas (o reconectamos): no hay hueco

        offset += 20
        time.sleep(random.uniform(0.8, 1.5))


def obtener_reviews_actividad(tour_id):
    """
    Intenta traer todas las reviews en rango de fecha para una actividad.
    Primero sin filtro; si choca con el tope de 320 antes de llegar a
    fecha_inicio, escala partiendo por cada calificación (1 a 5 estrellas),
    y si una partición individual también se topa, agrega un pase inverso
    (date_asc) para esa partición.
    """
    vistas = set()
    resultados, tope = paginar(tour_id, "date_desc", {}, vistas)

    if not tope:
        return resultados, False, False

    # El pase sin filtro chocó con el tope de ~320 antes de llegar a fecha_inicio:
    # escalamos partiendo por calificación para recuperar lo que quedó afuera.
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


# --- PREPARACIÓN DEL ARCHIVO Y MEMORIA ---
if not os.path.exists(archivo_salida):
    with open(archivo_salida, 'w', encoding='utf-8-sig') as f:
        f.write("pais;destino;actividad;url_actividad;fecha;pais_usuario\n")

tours_procesados = set()
if os.path.exists(archivo_salida):
    try:
        df_existente = pd.read_csv(archivo_salida, sep=';')
        tours_procesados = set(df_existente['url_actividad'].unique())
        print(f"🔄 Modo continuación: {len(tours_procesados)} tours ya procesados.")
    except Exception:
        print("Empezando desde cero.")


def procesar_tour(row):
    tour_id = int(row['tour_id'])
    pais = str(row['pais'])
    destino = str(row['ciudad_id']).split('-l')[0].capitalize().replace('-', ' ')
    actividad = str(row['titulo_referencia'])
    url_act = str(row['url'])

    print(f"▶️ Iniciando ID {tour_id} ({destino}): {actividad[:25]}...")

    resultados, escalo, hubo_hueco = obtener_reviews_actividad(tour_id)

    for review, fecha_obj in resultados:
        autor_texto = review.get('author', {}).get('title', {}).get('text', '')
        autor_texto = autor_texto.replace(' – ', ' - ').replace(' — ', ' - ')
        pais_usuario = autor_texto.split(' - ')[-1].strip() if ' - ' in autor_texto else "Desconocido"

        linea_csv = f"{pais};{destino};{actividad};{url_act};{fecha_obj.strftime('%d/%m/%Y')};{pais_usuario}\n"

        with lock_csv:
            with open(archivo_salida, 'a', encoding='utf-8-sig') as f:
                f.write(linea_csv)

    etiqueta = " [escaló por tope de 320]" if escalo else ""
    etiqueta += " [posible hueco residual]" if hubo_hueco else ""
    return f"✅ Fin ID {tour_id}: {len(resultados)} guardadas{etiqueta}."


# --- EJECUCIÓN ---
if __name__ == "__main__":
    construir_lista_tours()

    df_tours = pd.read_csv(archivo_tours, sep=';').fillna("Desconocido")
    # Para probar con un subconjunto antes de la corrida completa, descomentá:
    # df_tours = df_tours.iloc[:50]

    tours_pendientes = [row for _, row in df_tours.iterrows() if str(row['url']) not in tours_procesados]
    print(f"\nIniciando extracción multihilo para {len(tours_pendientes)} actividades pendientes "
          f"(de {len(df_tours):,} totales) en {len(DESTINOS)} destinos...\n")

    # ⚠️ Sin proxy rotativo: no subas 'maximos_hilos' o tu IP local puede ser bloqueada.
    maximos_hilos = 4

    with concurrent.futures.ThreadPoolExecutor(max_workers=maximos_hilos) as executor:
        for resultado in executor.map(procesar_tour, tours_pendientes):
            print(resultado)

    print("\n🎉 PROCESO COMPLETADO EXITOSAMENTE.")
