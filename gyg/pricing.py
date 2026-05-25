import pandas as pd
import time
import random
import os
import concurrent.futures
import threading
from bs4 import BeautifulSoup
import re
import glob

# ── HTTP client: curl_cffi impersonates Chrome's real TLS fingerprint ─────────
# This is the primary fix for Cloudflare/bot-detection blocks.
# Install: pip3 install curl_cffi
from curl_cffi.requests import Session as CurlSession
_SESSION = CurlSession(impersonate="chrome124")

# --- 1. CARGAR ARCHIVO COMBINADO ---

_combined = sorted(glob.glob("gyg/tours_all_IDs*.csv"))
if not _combined:
    raise FileNotFoundError(
        "No se encontró gyg/tours_all_IDs*.csv — ejecuta gyg_sitemap.py primero."
    )
archivo_input = _combined[-1]   # most recent (YYYY-MM-DD suffix sorts lexicographically)
print(f"📂 Usando: {archivo_input}")

archivo_salida = 'metadata_latam_FINAL_mayo.csv'

# --- 2. CABECERAS (Y COOKIES PARA FORZAR USD) ---
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'accept-language': 'es-ES,es;q=0.9'
}

# Cookie mágica para obligar a GYG a mostrarnos Dólares
cookies_usd = {'currency': 'USD'} 

lock_csv = threading.Lock()

# --- 3. PREPARAR ARCHIVO DE SALIDA Y MEMORIA ---
if not os.path.exists(archivo_salida):
    with open(archivo_salida, 'w', encoding='utf-8-sig') as f:
        # AÑADIDO: Columna "destino"
        f.write("pais;destino;tour_id;nombre_actividad;url;proveedor;total_reseñas;precio_original;precio_promocion;moneda\n")

urls_procesadas = set()
if os.path.exists(archivo_salida):
    try:
        df_existente = pd.read_csv(archivo_salida, sep=';')
        urls_procesadas = set(df_existente['url'].unique())
        print(f"🔄 Modo continuación: {len(urls_procesadas)} actividades ya procesadas.\n")
    except Exception:
        pass


def extraer_metadata(html, url, tour_id, pais, destino):
    soup = BeautifulSoup(html, 'html.parser')

    # 1. Extraer Título LIMPIO (Sin el "| GetYourGuide")
    meta_title = soup.find('meta', property='og:title')
    nombre = meta_title['content'].replace(' | GetYourGuide', '').replace(';', ',') if meta_title else "Desconocido"

    # 2. Extraer Proveedor
    meta_brand = soup.find('meta', property='og:brand')
    proveedor = meta_brand['content'].replace(';', ',') if meta_brand else "Desconocido"

    # 3. Extraer Precios y Moneda
    meta_price_amount = soup.find('meta', property='product:price:amount')
    meta_price_standard = soup.find('meta', property='og:price:standard_amount')
    meta_currency = soup.find('meta', property='product:price:currency')

    precio_final = meta_price_amount['content'] if meta_price_amount else "0"
    precio_original = meta_price_standard['content'] if meta_price_standard else precio_final
    
    precio_promocion = precio_final if float(precio_original) > float(precio_final) else "0"
    moneda = meta_currency['content'] if meta_currency else "USD"

    # 4. Extraer Total de Reseñas
    total_reseñas = "0"
    res_count = soup.find(class_='simple-activity-rating--reviews-count')
    if res_count:
        match = re.search(r'(\d+)', res_count.get_text().replace('.', '').replace(',', ''))
        if match: 
            total_reseñas = match.group(1)

    # 5. Construir y guardar la fila
    linea = f"{pais};{destino};{tour_id};{nombre};{url};{proveedor};{total_reseñas};{precio_original};{precio_promocion};{moneda}\n"
    
    with lock_csv:
        with open(archivo_salida, 'a', encoding='utf-8-sig') as f:
            f.write(linea)


MAX_REINTENTOS_403 = 3
PAUSA_403_BASE   = 3   # seconds — doubles on each retry (30 → 60 → 120)

def procesar_tour(row, pais):
    tour_id = str(row['tour_id'])
    url = str(row['url'])
    destino = str(row['ciudad_id']).split('-l')[0].capitalize().replace('-', ' ')

    if url in urls_procesadas:
        return None

    for intento in range(1, MAX_REINTENTOS_403 + 1):
        try:
            respuesta = _SESSION.get(
                url,
                headers=headers,
                cookies=cookies_usd,
                params={'currency': 'USD'},
                timeout=15,
            )

            if respuesta.status_code == 200:
                extraer_metadata(respuesta.text, url, tour_id, pais, destino)
                time.sleep(random.uniform(1.0, 2.5))   # human-like pacing
                return f"✅ {pais} - {tour_id}: Completado"

            elif respuesta.status_code == 403:
                pausa = PAUSA_403_BASE * (2 ** (intento - 1))   # 30 → 60 → 120 s
                print(f"🚦 403 en {tour_id} (intento {intento}/{MAX_REINTENTOS_403}) — esperando {pausa}s…")
                time.sleep(pausa)
                # last attempt still 403 → skip
                if intento == MAX_REINTENTOS_403:
                    return f"🚫 {pais} - {tour_id}: 403 sin resolver tras {MAX_REINTENTOS_403} intentos"

            elif respuesta.status_code == 404:
                return f"🚫 {pais} - {tour_id}: Inactivo/404"

            else:
                return f"⚠️ {pais} - {tour_id}: HTTP {respuesta.status_code}"

        except Exception as e:
            return f"❌ {pais} - {tour_id}: {type(e).__name__}: {e}"


# --- 4. PROCESAMIENTO COMBINADO ---
# 3 threads is the safe default. Raise to 5 only if you stop seeing 403s.
maximos_hilos = 3

df_tours = pd.read_csv(archivo_input, sep=';').fillna("Desconocido")
tours_pendientes = [
    row for _, row in df_tours.iterrows()
    if str(row['url']) not in urls_procesadas
]

if not tours_pendientes:
    print("⏩ Todo ya está procesado. Nada pendiente.")
else:
    # Summary by country before starting
    pendientes_por_pais = df_tours[~df_tours['url'].isin(urls_procesadas)].groupby('pais').size()
    for pais_nombre, n in pendientes_por_pais.items():
        print(f"  🌍 {pais_nombre}: {n:,} pendientes")

    print(f"\n▶ Total: {len(tours_pendientes):,} actividades — iniciando con {maximos_hilos} hilos…\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=maximos_hilos) as executor:
        resultados = executor.map(
            lambda row: procesar_tour(row, str(row['pais'])),
            tours_pendientes
        )
        for resultado in resultados:
            if resultado:
                print(resultado)

print("\n🎉 EXTRACCIÓN MAESTRA COMPLETADA PARA TODOS LOS PAÍSES.")