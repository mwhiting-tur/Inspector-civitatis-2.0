import pandas as pd
import time
import random
import os
import concurrent.futures
import threading
from bs4 import BeautifulSoup
import re
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

# Stealth instance reutilizable: spoofa plataforma/UA como Mac Chrome
_stealth = Stealth(
    navigator_platform_override="MacIntel",
    navigator_user_agent_override=(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
)

# --- 1. LISTA DE TUS 13 PAÍSES ---
archivos_paises = [
    "tours_chile_IDs_2026-05-25.csv"
]

archivo_salida = 'gyg_mayo_chile.csv'

lock_csv = threading.Lock()

# --- 2. PREPARAR ARCHIVO DE SALIDA Y MEMORIA ---
if not os.path.exists(archivo_salida):
    with open(archivo_salida, 'w', encoding='utf-8-sig') as f:
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


# --- PLAYWRIGHT POR HILO ---
# sync_playwright usa greenlets internamente: NO se puede compartir entre hilos.
# Cada hilo del pool tiene su propio _playwright + _browser vía threading.local().
_tls = threading.local()


def _get_tls_browser():
    """Devuelve el browser del hilo actual, creándolo la primera vez."""
    if not hasattr(_tls, 'browser'):
        _tls.pw = sync_playwright().start()
        _tls.browser = _tls.pw.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
            ]
        )
    return _tls.browser



def procesar_tour(row, pais):
    tour_id = str(row['tour_id'])
    url = str(row['url'])
    destino = str(row['ciudad_id']).split('-l')[0].capitalize().replace('-', ' ')

    if url in urls_procesadas:
        return None

    browser = _get_tls_browser()

    # Contexto aislado con cookie de moneda USD
    context = browser.new_context(
        locale='es-ES',
        extra_http_headers={'Accept-Language': 'es-ES,es;q=0.9'},
    )
    context.add_cookies([{
        'name': 'currency',
        'value': 'USD',
        'domain': '.getyourguide.com',
        'path': '/',
    }])

    page = context.new_page()
    _stealth.apply_stealth_sync(page)

    try:
        response = page.goto(
            f"{url}?currency=USD",
            wait_until='domcontentloaded',
            timeout=30_000,
        )

        status = response.status if response else 0

        if status == 200:
            html = page.content()
            extraer_metadata(html, url, tour_id, pais, destino)
            time.sleep(random.uniform(2.5, 5.0))
            return f"✅ {pais} - {tour_id}: Completado"

        elif status == 404:
            return f"🚫 {pais} - {tour_id}: Inactivo/404"

        else:
            return f"⚠️ {pais} - {tour_id}: Error {status}"

    except Exception as e:
        return f"❌ {pais} - {tour_id}: Error de red — {e}"

    finally:
        page.close()
        context.close()




# --- 4. BUCLE MAESTRO POR PAÍSES ---
# 3 hilos = 3 browsers independientes. Más seguro y evita bloqueos de IP.
maximos_hilos = 3

for archivo in archivos_paises:
    ruta_archivo = f"gyg/{archivo}"

    if not os.path.exists(ruta_archivo):
        print(f"⚠️ Archivo no encontrado: {ruta_archivo}. Saltando...")
        continue

    pais_actual = archivo.split('_')[1].capitalize()
    df_tours = pd.read_csv(ruta_archivo, sep=';').fillna("Desconocido")
    tours_pendientes = [row for _, row in df_tours.iterrows() if str(row['url']) not in urls_procesadas]

    if not tours_pendientes:
        print(f"⏩ {pais_actual} ya está 100% completo. Saltando...")
        continue

    print(f"\n🌍 INICIANDO PAÍS: {pais_actual} ({len(tours_pendientes)} tours pendientes)")

    # Cada hilo del pool abre su propio Chromium y lo reutiliza para todos sus tours.
    # Al hacer shutdown del executor los hilos terminan; sus browsers se liberan con el proceso.
    with concurrent.futures.ThreadPoolExecutor(max_workers=maximos_hilos) as executor:
        futures = {executor.submit(procesar_tour, row, pais_actual): row for row in tours_pendientes}
        for future in concurrent.futures.as_completed(futures):
            resultado = future.result()
            if resultado:
                print(resultado)

    # Cerrar los browsers de cada hilo al terminar el país
    # (los hilos ya terminaron, así que llamamos _close_tls_browser desde el hilo principal
    #  no aplica — los browsers se liberan cuando el proceso termina normalmente)

print("\n🎉 EXTRACCIÓN MAESTRA COMPLETADA PARA TODOS LOS PAÍSES.")
