import pandas as pd
import requests
import time
import random
import os
import json
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
archivo_entrada = 'gyg/tours_brasil_IDs.csv'
archivo_salida = 'gyg/reviews_brasil_FINAL.csv'

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

print(f"Iniciando extracción para {len(df_tours)} tours...\n")

for index, row in df_tours.iterrows():
    tour_id = int(row['tour_id'])
    destino = row['ciudad_id'].split('-l')[0].capitalize().replace('-', ' ')
    actividad = row['titulo_referencia']
    url_act = row['url']
    
    print(f"[{index + 1}/{len(df_tours)}] Tour ID: {tour_id} - {actividad[:30]}...", end=" ")
    
    offset = 0
    limite_paginas = 20
    continuar_paginando = True
    reseñas_guardadas = 0
    
    while continuar_paginando:
            payload_dict = {
                "payload": {
                    "activityId": tour_id,
                    "templateName": "ActivityDetails",
                    "contentIdentifier": "next-reviews-page",
                    "rankingUuid": "8db3d7f9-ae97-4e8e-9782-086c43dd5f1b",
                    "additionalDetailsSelectedLanguage": "es-ES",
                    "reviewsOffset": offset,
                    "reviewsLimit": limite_paginas,
                    "participantsLanguage": "es-ES",
                    "reviewsExperiments": [{"key": "rvw-display-traveler-type-in-reviews", "isEnabled": False}]
                }
            }
            
            # Truco Jedi: Convertimos el diccionario a un string exacto sin espacios (igual que tu cURL)
            cuerpo_peticion_raw = json.dumps(payload_dict, separators=(',', ':'))
            
            try:
                # OJO AQUI: Cambiamos json= por data= para enviar el texto crudo
                respuesta = requests.post(url_api_post, headers=headers, data=cuerpo_peticion_raw, timeout=10)
                
                if respuesta.status_code != 200:
                    print(f"⚠️ Error {respuesta.status_code}.")
                    break
                    
                datos_json = respuesta.json()
                


                    
                reseñas = buscar_reseñas_en_json(respuesta.json())
                
                if not reseñas:
                    if reseñas_guardadas == 0:
                        print("📭 0 reseñas encontradas.")
                    else:
                        print(f"✅ {reseñas_guardadas} guardadas en total.")
                    break 
                    
                for review in reseñas:
                    # 1. Extraer la fecha limpia desde el tracker oculto
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
                        
                    # 2. Extraer el país del autor ("Celeste – México")
                    autor_texto = review.get('author', {}).get('title', {}).get('text', '')
                    
                    # Reemplazamos diferentes tipos de guiones largos/cortos por seguridad
                    autor_texto = autor_texto.replace(' – ', ' - ').replace(' — ', ' - ')
                    
                    if ' - ' in autor_texto:
                        pais_usuario = autor_texto.split(' - ')[-1].strip()
                    else:
                        pais_usuario = "Desconocido"
                    
                    # 3. Escribir en el CSV
                    linea_csv = f"Brasil;{destino};{actividad};{url_act};{fecha_obj.strftime('%d/%m/%Y')};{pais_usuario}\n"
                    with open(archivo_salida, 'a', encoding='utf-8-sig') as f:
                        f.write(linea_csv)
                        
                    reseñas_guardadas += 1
                
                if continuar_paginando:
                    offset += limite_paginas
                    time.sleep(random.uniform(1.0, 2.0))
                
            except Exception as e:
                print(f"⚠️ Error de ejecución: {e}")
                break
            
print("\n🎉 PROCESO COMPLETADO.")