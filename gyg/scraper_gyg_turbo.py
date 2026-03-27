import pandas as pd
import requests
import time
import random
import os
import json
import concurrent.futures
import threading
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
archivo_entrada = 'gyg/tours_republica_dominicana_IDs.csv'
archivo_salida = 'gyg/reviews_republica_dominicana_FINAL.csv'

url_api_post = "https://travelers-api.getyourguide.com/user-interface/activity-details-page/blocks?ranking_uuid=8db3d7f9-ae97-4e8e-9782-086c43dd5f1b"
hace_5_anos = datetime.now() - timedelta(days=5*365)

# TUS CABECERAS EXACTAS
headers = {
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'Origin': 'https://www.getyourguide.com',
    'Referer': 'https://www.getyourguide.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'accept-currency': 'CLP',
    'accept-language': 'es-ES',
    'geo-ip-country': 'CL',
    'partner-id': 'CD951',
    'visitor-id': 'F97X0158YNG8999WZEDF645USNKBJAZD', # Tu llave maestra 
    'visitor-platform': 'desktop',
    'x-gyg-app-type': 'Web'
}

# 🔒 Cerrojo de seguridad para escribir en el CSV sin que los hilos se pisen
lock_csv = threading.Lock()

def buscar_reseñas_en_json(datos):
    """Busca recursivamente bloques de tipo 'review' en el JSON."""
    reseñas = []
    if isinstance(datos, dict):
        if datos.get("type") == "review" and "author" in datos:
            reseñas.append(datos)
        else:
            for key, value in datos.items():
                reseñas.extend(buscar_reseñas_en_json(value))
    elif isinstance(datos, list):
        for item in datos:
            reseñas.extend(buscar_reseñas_en_json(item))
    return reseñas

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

try:
    df_tours = pd.read_csv(archivo_entrada, sep=';').fillna("Desconocido")
    #df_tours = df_tours.iloc[:1500]
except FileNotFoundError:
    print(f"❌ Archivo no encontrado.")
    exit()

# Filtramos la lista para dejar SOLO los que no hemos procesado aún
tours_pendientes = [row for index, row in df_tours.iterrows() if str(row['url']) not in tours_procesados]

print(f"Iniciando extracción multihilo para {len(tours_pendientes)} tours pendientes...\n")

# --- LA FUNCIÓN DE TRABAJO (Lo que hará cada hilo) ---
def procesar_tour(row):
    tour_id = int(row['tour_id'])
    destino = str(row['ciudad_id']).split('-l')[0].capitalize().replace('-', ' ')
    actividad = str(row['titulo_referencia'])
    url_act = str(row['url'])

    print(f"▶️ Iniciando ID {tour_id}: {actividad[:25]}...")
    
    offset = 0
    limite_paginas = 20
    continuar_paginando = True
    reseñas_guardadas = 0
    reseñas_vistas = set() # Escudo anti-bucles
    
    while continuar_paginando:
        # EL PAYLOAD PERFECTO Y DEFINITIVO
        payload_dict = {
            "payload": {
                "activityId": tour_id,
                "templateName": "ActivityDetails",
                "contentIdentifier": "next-reviews-page", 
                "additionalDetailsSelectedLanguage": "es-ES",
                "reviewsOffset": offset,
                "reviewsLimit": limite_paginas,
                "selectedReviewsSortingOrder": "date_desc", 
                "selectedReviewsFilters": {}, 
                "participantsLanguage": "es-ES",
                "reviewsExperiments": [{"key": "rvw-display-traveler-type-in-reviews", "isEnabled": False}]
            }
        }
        
        cuerpo_peticion_raw = json.dumps(payload_dict, separators=(',', ':'))
        
        try:
            respuesta = requests.post(url_api_post, headers=headers, data=cuerpo_peticion_raw, timeout=15)
            
            if respuesta.status_code != 200:
                print(f"  ⚠️ Error {respuesta.status_code} en ID {tour_id}")
                break
                
            reseñas = buscar_reseñas_en_json(respuesta.json())
            
            if not reseñas:
                break 
                
            nuevas_en_esta_pagina = 0
            
            for review in reseñas:
                review_id = review.get('reviewId')
                if review_id in reseñas_vistas: continue
                    
                reseñas_vistas.add(review_id)
                nuevas_en_esta_pagina += 1
                
                tracker = review.get('onImpressionTrackingEvent', {}).get('properties', {})
                fecha_str = tracker.get('review_date', '')
                if not fecha_str: continue
                    
                try:
                    fecha_obj = datetime.strptime(fecha_str.split('T')[0], "%Y-%m-%d")
                except ValueError: continue 
                
                if fecha_obj < hace_5_anos:
                    continuar_paginando = False 
                    break
                    
                autor_texto = review.get('author', {}).get('title', {}).get('text', '')
                autor_texto = autor_texto.replace(' – ', ' - ').replace(' — ', ' - ')
                pais_usuario = autor_texto.split(' - ')[-1].strip() if ' - ' in autor_texto else "Desconocido"
                
                linea_csv = f"República Dominicana;{destino};{actividad};{url_act};{fecha_obj.strftime('%d/%m/%Y')};{pais_usuario}\n"
                
                # 🔒 USAMOS EL CERROJO PARA GUARDAR EN EL CSV DE FORMA SEGURA
                with lock_csv:
                    with open(archivo_salida, 'a', encoding='utf-8-sig') as f:
                        f.write(linea_csv)
                    
                reseñas_guardadas += 1
            
            # Detector de bucles infinitos
            if nuevas_en_esta_pagina == 0 and continuar_paginando:
                break 
            
            if continuar_paginando:
                offset += limite_paginas
                time.sleep(random.uniform(1.0, 2.0))
            
        except Exception as e:
            print(f"  ⚠️ Error en ID {tour_id}: {e}")
            break
            
    return f"✅ Fin ID {tour_id}: {reseñas_guardadas} guardadas."

# --- EJECUCIÓN MULTIHILO ---
# ⚠️ ADVERTENCIA: No subas 'max_workers' a más de 4 o 5, o tu IP local será bloqueada.
maximos_hilos = 4 

with concurrent.futures.ThreadPoolExecutor(max_workers=maximos_hilos) as executor:
    resultados = executor.map(procesar_tour, tours_pendientes)
    
    # Imprimimos los resultados a medida que cada hilo va terminando
    for resultado in resultados:
        print(resultado)

print("\n🎉 PROCESO COMPLETADO EXITOSAMENTE.")