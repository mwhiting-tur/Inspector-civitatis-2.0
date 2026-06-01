"""
gyg_prices_api.py
Extrae precio (USD) de actividades de GetYourGuide vía HTML scraping
con curl_cffi (impersonate="chrome") para evadir detección de bots.

Modos de uso:
  # Correr completo (local)
  python gyg/gyg_prices_api.py

  # Batch específico (GitHub Actions / proxy rotation)
  python gyg/gyg_prices_api.py --archivo tours_chile_IDs_2026-05-25.csv \
      --offset 0 --limit 25 --output gyg_batch_0.csv

  # Con proxy
  python gyg/gyg_prices_api.py --proxy http://user:pass@host:port
"""

import argparse
import pandas as pd
from curl_cffi import requests as cffi_requests
import time
import random
import os
import concurrent.futures
import threading
from bs4 import BeautifulSoup
import re

# ── CLI ────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--archivo",  default=None,  help="CSV de entrada (nombre de archivo, sin ruta)")
parser.add_argument("--offset",   type=int, default=0,    help="Índice de inicio")
parser.add_argument("--limit",    type=int, default=None, help="Máximo de tours a procesar")
parser.add_argument("--output",   default=None,  help="Archivo CSV de salida")
parser.add_argument("--proxy",    default=None,  help="Proxy URL (http://user:pass@host:port)")
parser.add_argument("--threads",  type=int, default=3,    help="Hilos paralelos")
parser.add_argument("--delay-min",type=float, default=3.0, help="Pausa mínima entre requests (s)")
parser.add_argument("--delay-max",type=float, default=5.0, help="Pausa máxima entre requests (s)")
args = parser.parse_args()

# ── Configuración ──────────────────────────────────────────────────────────────
archivos_paises = [args.archivo] if args.archivo else [
    "tours_chile_IDs_2026-05-25.csv",
]

archivo_salida  = args.output or "gyg_mayo_chile_api.csv"
maximos_hilos   = args.threads
PROXY           = {"http": args.proxy, "https": args.proxy} if args.proxy else None

lock_csv = threading.Lock()

# ── Preparar salida y memoria ──────────────────────────────────────────────────
HEADER = "pais;destino;tour_id;nombre_actividad;url;proveedor;total_reseñas;precio_original;precio_promocion;moneda\n"

if not os.path.exists(archivo_salida):
    with open(archivo_salida, "w", encoding="utf-8-sig") as f:
        f.write(HEADER)

urls_procesadas = set()
if os.path.exists(archivo_salida):
    try:
        df_exist = pd.read_csv(archivo_salida, sep=";")
        urls_procesadas = set(df_exist["url"].unique())
        print(f"🔄 Modo continuación: {len(urls_procesadas)} tours ya procesados.\n")
    except Exception:
        pass


# ── Extractor HTML ──────────────────────────────────────────────────────────────
def extraer_metadata(html, url, tour_id, pais, destino):
    soup = BeautifulSoup(html, "html.parser")

    meta_title    = soup.find("meta", property="og:title")
    nombre        = meta_title["content"].replace(" | GetYourGuide", "").replace(";", ",") if meta_title else "Desconocido"

    meta_brand    = soup.find("meta", property="og:brand")
    proveedor     = meta_brand["content"].replace(";", ",") if meta_brand else "Desconocido"

    meta_amount   = soup.find("meta", property="product:price:amount")
    meta_standard = soup.find("meta", property="og:price:standard_amount")
    meta_currency = soup.find("meta", property="product:price:currency")

    precio_final    = meta_amount["content"]   if meta_amount   else "0"
    precio_original = meta_standard["content"] if meta_standard else precio_final

    try:
        precio_promo = precio_final if float(precio_original) > float(precio_final) else "0"
    except ValueError:
        precio_promo = "0"

    moneda = meta_currency["content"] if meta_currency else "USD"

    total_reseñas = "0"
    res = soup.find(class_="simple-activity-rating--reviews-count")
    if res:
        m = re.search(r"(\d+)", res.get_text().replace(".", "").replace(",", ""))
        if m:
            total_reseñas = m.group(1)

    linea = f"{pais};{destino};{tour_id};{nombre};{url};{proveedor};{total_reseñas};{precio_original};{precio_promo};{moneda}\n"

    with lock_csv:
        with open(archivo_salida, "a", encoding="utf-8-sig") as f:
            f.write(linea)


# ── Request con reintentos ─────────────────────────────────────────────────────
def fetch_con_reintentos(url, reintentos=2):
    """Intenta la petición hasta `reintentos` veces; rota proxy si está disponible."""
    for intento in range(reintentos + 1):
        try:
            kwargs = dict(
                headers={"Accept-Language": "es-ES,es;q=0.9"},
                impersonate="chrome",
                timeout=20,
                verify=False,
            )
            if PROXY:
                kwargs["proxies"] = PROXY

            resp = cffi_requests.get(f"{url}?currency=USD", **kwargs)
            return resp
        except Exception as e:
            if intento < reintentos:
                time.sleep(3)
            else:
                raise e


# ── Procesar un tour ───────────────────────────────────────────────────────────
def procesar_tour(row, pais):
    tour_id = str(row["tour_id"])
    url     = str(row["url"])
    destino = str(row["ciudad_id"]).split("-l")[0].capitalize().replace("-", " ")

    if url in urls_procesadas:
        return None

    try:
        resp = fetch_con_reintentos(url)

        if resp.status_code == 200:
            extraer_metadata(resp.text, url, tour_id, pais, destino)
            time.sleep(random.uniform(args.delay_min, args.delay_max))
            return f"✅ {pais} - {tour_id}: OK"

        elif resp.status_code == 404:
            return f"🚫 {pais} - {tour_id}: 404"

        elif resp.status_code in (403, 429):
            time.sleep(30)
            return f"⛔ {pais} - {tour_id}: bloqueado ({resp.status_code})"

        else:
            return f"⚠️ {pais} - {tour_id}: HTTP {resp.status_code}"

    except Exception as e:
        return f"❌ {pais} - {tour_id}: {e}"


# ── Bucle maestro ──────────────────────────────────────────────────────────────
for archivo in archivos_paises:
    ruta = f"gyg/{archivo}"
    if not os.path.exists(ruta):
        print(f"⚠️  No encontrado: {ruta}. Saltando...")
        continue

    pais_actual = archivo.split("_")[1].capitalize()
    df          = pd.read_csv(ruta, sep=";").fillna("Desconocido")

    # Aplicar offset / limit para modo batch (GitHub Actions)
    df_slice    = df.iloc[args.offset : (args.offset + args.limit) if args.limit else None]
    pendientes  = [r for _, r in df_slice.iterrows() if str(r["url"]) not in urls_procesadas]

    if not pendientes:
        print(f"⏩ {pais_actual} batch ya completo.")
        continue

    print(f"\n🌍 {pais_actual} — offset={args.offset} limit={args.limit or 'todo'} — {len(pendientes)} tours pendientes")

    with concurrent.futures.ThreadPoolExecutor(max_workers=maximos_hilos) as executor:
        futures = {executor.submit(procesar_tour, row, pais_actual): row for row in pendientes}
        for future in concurrent.futures.as_completed(futures):
            resultado = future.result()
            if resultado:
                print(resultado)

print("\n🎉 BATCH COMPLETADO.")
