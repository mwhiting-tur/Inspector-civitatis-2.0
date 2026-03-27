import pandas as pd
import requests
import time
import random
import os
import json
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
archivo_entrada = 'gyg/tours_mexico_IDs.csv'
archivo_salida = 'gyg/reviews_mexico_FINAL.csv'

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
    'x-gyg-app-type': 'Web',
    'x-gyg-geoip-city': 'Santiago',
    'x-gyg-geoip-country': 'CL',
    'x-gyg-is-new-visitor': 'false',
    'x-gyg-partner-hash': 'CD951',
    'x-gyg-time-zone': 'America/Santiago'
}

def buscar_reseñas_en_json(datos):
    """Busca recursivamente bloques de tipo 'review' en el JSON de GetYourGuide."""
    reseñas = []
    if isinstance(datos, dict):
        # Si encontramos un bloque que es explícitamente una reseña, lo atrapamos
        if datos.get("type") == "review" and "author" in datos:
            reseñas.append(datos)
        else:
            # Si no, seguimos escarbando en las ramas del diccionario
            for key, value in datos.items():
                reseñas.extend(buscar_reseñas_en_json(value))
    elif isinstance(datos, list):
        # Si es una lista, escarbamos en cada elemento
        for item in datos:
            reseñas.extend(buscar_reseñas_en_json(item))
    return reseñas

if not os.path.exists(archivo_salida):
    with open(archivo_salida, 'w', encoding='utf-8-sig') as f:
        f.write("pais;destino;actividad;url_actividad;fecha;pais_usuario\n")

# --- CARGAMOS LA BASE DE DATOS ---
try:
    df_tours = pd.read_csv(archivo_entrada, sep=';')
    
    # 1. EL PARCHE DEFINITIVO PARA LOS NAN: 
    # Rellena cualquier celda vacía de todo el archivo con texto, matando el error de 'float' de raíz.
    df_tours = df_tours.fillna("Desconocido")
    
    # 2. EL ATAJO DE TIEMPO:
    # Rebanamos el archivo para que empiece exactamente desde el índice 818.
    # (Ojo: el contador en tu consola se verá raro, dirá [819/1359] porque el archivo ahora es más corto, ignóralo).
    #df_tours = df_tours.iloc[818:] 
    
except FileNotFoundError:
    print(f"❌ No se encontró el archivo {archivo_entrada}.")
    exit()

# 🛑 INYECCIÓN DE PRUEBA: Añadimos tu ID activo al principio de la lista
tour_prueba = pd.DataFrame([{
    "pais": "Brasil", "ciudad_id": "prueba-activa", "tour_id": 546054, 
    "titulo_referencia": "Tour de Prueba (cURL)", "url": "https://getyourguide.com/..."
}])
# Comenta la siguiente línea cuando quieras correr los 2177 tours completos en GitHub Actions
#df_tours = pd.concat([tour_prueba, df_tours.head(30)], ignore_index=True)


# --- SISTEMA DE MEMORIA: SALTAR TOURS YA PROCESADOS ---
tours_procesados = set()
if os.path.exists(archivo_salida):
    try:
        df_existente = pd.read_csv(archivo_salida, sep=';')
        # Guardamos las URLs que ya están en el CSV (o puedes usar el nombre de la actividad)
        tours_procesados = set(df_existente['url_actividad'].unique())
        print(f"🔄 Modo continuación: Se encontraron {len(tours_procesados)} tours ya procesados en el CSV.")
    except Exception as e:
        print("No se pudo leer el CSV existente, empezando desde cero.")

print(f"Iniciando extracción para {len(df_tours)} tours...\n")


for index, row in df_tours.iterrows():
    tour_id = int(row['tour_id'])
    destino = row['ciudad_id'].split('-l')[0].capitalize().replace('-', ' ')
    actividad = row['titulo_referencia']
    url_act = row['url']

    if actividad == 'nan':
        actividad = "Titulo desconocido"

    # --- LA MAGIA: COMPROBAMOS SI YA LO HICIMOS ---
    if url_act in tours_procesados:
        print(f"[{index + 1}/{len(df_tours)}] ⏭️ Saltando (Ya procesado): {actividad[:30]}...")
        continue # Salta al siguiente tour inmediatamente


    print(f"[{index + 1}/{len(df_tours)}] Tour ID: {tour_id} - {actividad[:30]}...", end=" ")
    
    offset = 0
    limite_paginas = 20
    continuar_paginando = True
    reseñas_guardadas = 0
    
    # 🛡️ ESCUDO ANTI-BUCLES: Memoria de reseñas de este tour
    reseñas_vistas = set()
    
    while continuar_paginando:
            # 1. PAYLOAD CORREGIDO: Sin simular clics, ordenamiento directo.
            # 1. PAYLOAD CORREGIDO: El clon exacto de tu captura
            payload_dict = {
                "payload": {
                    "activityId": tour_id,
                    "templateName": "ActivityDetails",
                    "contentIdentifier": "next-reviews-page", # Volvemos a la paginación normal
                    "additionalDetailsSelectedLanguage": "es-ES",
                    "reviewsOffset": offset,
                    "reviewsLimit": limite_paginas,
                    "selectedReviewsSortingOrder": "date_desc", # El orden cronológico correcto
                    "selectedReviewsFilters": {}, # Confirmamos que no hay otros filtros
                    "participantsLanguage": "es-ES",
                    "reviewsExperiments": [
                        {
                            "key": "rvw-display-traveler-type-in-reviews",
                            "isEnabled": False
                        }
                    ]
                }
            }
            
            cuerpo_peticion_raw = json.dumps(payload_dict, separators=(',', ':'))
            
            try:
                respuesta = requests.post(url_api_post, headers=headers, data=cuerpo_peticion_raw, timeout=10)
                
                if respuesta.status_code != 200:
                    print(f"⚠️ Error {respuesta.status_code}.")
                    break
                    
                reseñas = buscar_reseñas_en_json(respuesta.json())
                
                if not reseñas:
                    if reseñas_guardadas == 0:
                        print("📭 0 reseñas encontradas.")
                    else:
                        print(f"✅ {reseñas_guardadas} guardadas en total.")
                    break 
                
                nuevas_en_esta_pagina = 0
                
                for review in reseñas:
                    # 2. IDENTIFICADOR ÚNICO: Comprobamos si ya vimos esta reseña
                    review_id = review.get('reviewId')
                    
                    if review_id in reseñas_vistas:
                        continue # Si ya la vimos, pasamos de largo
                        
                    reseñas_vistas.add(review_id)
                    nuevas_en_esta_pagina += 1
                    
                    tracker = review.get('onImpressionTrackingEvent', {}).get('properties', {})
                    fecha_str = tracker.get('review_date', '')
                    
                    if not fecha_str: continue
                        
                    try:
                        fecha_obj = datetime.strptime(fecha_str.split('T')[0], "%Y-%m-%d")
                    except ValueError: continue 
                    
                    if fecha_obj < hace_5_anos:
                        print(f"  ⏳ {reseñas_guardadas} guardadas (Límite 5 años).")
                        continuar_paginando = False 
                        break
                        
                    autor_texto = review.get('author', {}).get('title', {}).get('text', '')
                    autor_texto = autor_texto.replace(' – ', ' - ').replace(' — ', ' - ')
                    
                    if ' - ' in autor_texto:
                        pais_usuario = autor_texto.split(' - ')[-1].strip()
                    else:
                        pais_usuario = "Desconocido"
                    
                    linea_csv = f"Mexico;{destino};{actividad};{url_act};{fecha_obj.strftime('%d/%m/%Y')};{pais_usuario}\n"
                    with open(archivo_salida, 'a', encoding='utf-8-sig') as f:
                        f.write(linea_csv)
                        
                    reseñas_guardadas += 1
                
                # 3. DETECTOR DE BUCLES: Si procesamos la página y no hubo NINGUNA reseña nueva...
                if nuevas_en_esta_pagina == 0 and continuar_paginando:
                    print(f"  🔄 Bucle detectado o no hay más recientes. ✅ {reseñas_guardadas} guardadas.")
                    break # Rompemos el bucle para pasar al siguiente tour
                
                if continuar_paginando:
                    offset += limite_paginas
                    time.sleep(random.uniform(1.0, 2.0))
                
            except Exception as e:
                print(f"⚠️ Error de ejecución: {e}")
                break
            
print("\n🎉 PROCESO COMPLETADO.")