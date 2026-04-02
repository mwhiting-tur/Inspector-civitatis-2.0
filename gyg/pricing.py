import pandas as pd
import requests
import time
import random
import os
import concurrent.futures
import threading
from bs4 import BeautifulSoup
import re

# --- 1. LISTA DE TUS 13 PAÍSES ---
archivos_paises = [
    "tours_argentina_IDs.csv", "tours_bolivia_IDs.csv", "tours_brasil_IDs.csv",
    "tours_chile_IDs.csv", "tours_colombia_IDs.csv", "tours_costarica_IDs.csv",
    "tours_ecuador_IDs.csv", "tours_mexico_IDs.csv", "tours_panama_IDs.csv",
    "tours_paraguay_IDs.csv", "tours_peru_IDs.csv", "tours_republica_dominicana_IDs.csv",
    "tours_uruguay_IDs.csv"
]

archivo_salida = 'metadata_latam_FINAL.csv'

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


def procesar_tour(row, pais):
    tour_id = str(row['tour_id'])
    url = str(row['url'])
    
    # AÑADIDO: Extracción limpia del destino
    destino = str(row['ciudad_id']).split('-l')[0].capitalize().replace('-', ' ')
    
    if url in urls_procesadas:
        return None

    try:
        # AÑADIDO: params={'currency': 'USD'} y cookies para doble confirmación del dólar
        respuesta = requests.get(
            url, 
            headers=headers, 
            cookies=cookies_usd, 
            params={'currency': 'USD'}, 
            timeout=15
        )
        
        if respuesta.status_code == 200:
            extraer_metadata(respuesta.text, url, tour_id, pais, destino)
            
            # TURBO ACTIVADO: Tiempo de espera drásticamente reducido
            time.sleep(random.uniform(0.2, 0.7)) 
            return f"✅ {pais} - {tour_id}: Completado"
            
        elif respuesta.status_code == 404:
            return f"🚫 {pais} - {tour_id}: Inactivo/404"
        else:
            return f"⚠️ {pais} - {tour_id}: Error {respuesta.status_code}"
            
    except Exception as e:
        return f"❌ {pais} - {tour_id}: Error de red"


# --- 4. BUCLE MAESTRO POR PAÍSES ---
# TURBO ACTIVADO: 8 Hilos simultáneos (Si ves errores 403, bájalo a 5)
maximos_hilos = 8

for archivo in archivos_paises:
    ruta_archivo = f"gyg/{archivo}" 
    
    if not os.path.exists(ruta_archivo):
        print(f"⚠️ Archivo no encontrado: {ruta_archivo}. Saltando...")
        continue
        
    pais_actual = archivo.split('_')[1].capitalize()
    df_tours = pd.read_csv(ruta_archivo, sep=';').fillna("Desconocido")
    tours_pendientes = [row for index, row in df_tours.iterrows() if str(row['url']) not in urls_procesadas]
    
    if not tours_pendientes:
        print(f"⏩ {pais_actual} ya está 100% completo. Saltando...")
        continue
        
    print(f"\n🌍 INICIANDO PAÍS: {pais_actual} ({len(tours_pendientes)} tours pendientes)")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=maximos_hilos) as executor:
        resultados = executor.map(lambda row: procesar_tour(row, pais_actual), tours_pendientes)
        for resultado in resultados:
            if resultado:
                print(resultado)

print("\n🎉 EXTRACCIÓN MAESTRA COMPLETADA PARA TODOS LOS PAÍSES.")