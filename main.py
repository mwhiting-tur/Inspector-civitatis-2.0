import asyncio
import os
import json
import sys
from datetime import datetime
from drivers.civitatis import CivitatisScraper
from drivers.civitatis_semanal import CivitatisScraperSemanal
from drivers.nomades import NomadesScraper

# --- (Civitatis) ---
def cargar_destinos_civitatis(paises):
    with open('destinos_civitatis.json', 'r', encoding='utf-8') as f:
        todos = json.load(f)
    return [d for d in todos if d['nameCountry'].lower() in [p.lower() for p in paises]]

# --- (Nomades) ---
def parsear_destinos_nomades(ruta):
    import re
    tareas = []
    pais_actual = "Desconocido"
    with open(ruta, 'r', encoding='utf-8') as f:
        for linea in f:
            linea = linea.strip()
            if not linea: continue
            match = re.search(r'---\s*(.*?)\s*---', linea)
            if match: pais_actual = match.group(1).title()
            elif linea.startswith("http"): tareas.append({"pais": pais_actual, "url": linea})
    return tareas

async def ejecutar_civitatis(pais_objetivo):
    # Cargamos solo el país que viene desde GitHub Actions
    destinos = cargar_destinos_civitatis([pais_objetivo])
    
    # Creamos un archivo CSV específico para este país
    # Esto evita conflictos cuando varias máquinas intentan escribir el mismo archivo
    nombre_archivo = f"data/operadores_{pais_objetivo.lower()}.csv"
    
    print(f"🚀 Iniciando scraping para {pais_objetivo} ({len(destinos)} destinos)")
    
    # LÓGICA DE CHECKPOINT (OPCIONAL PERO RECOMENDADA)
    # Aquí podrías filtrar destinos que ya existan en nombre_archivo
    
    scraper = CivitatisScraper()
    await scraper.extract_list(destinos, nombre_archivo, currency_code="CLP")

if __name__ == "__main__":
    if not os.path.exists('data'): os.makedirs('data')
    
    # Capturamos el país desde los argumentos del comando
    if len(sys.argv) > 1:
        pais = sys.argv[1]
        asyncio.run(ejecutar_civitatis(pais))
    else:
        print("❌ Error: No se especificó un país.")

"""       
async def ejecutar_civitatis():
    PAISES = [
    "Argentina", "Bolivia", "Brasil", "Chile", "Colombia", "Costa Rica", 
    "Cuba", "Ecuador", "El Salvador", "Guatemala", "Haití", "Honduras", 
    "México", "Nicaragua", "Panamá", "Paraguay", "Perú", "República Dominicana", 
    "Uruguay", "Venezuela", "EEUU", "España", "Italia", "Francia", 
    "Países Bajos", "Reino Unido", "Alemania", "Bélgica", "Portugal", 
    "Turquia", "Grecia", "Austria", "Japón", "China", "Tailandia", "Australia"] # Configurar
    destinos = cargar_destinos_civitatis(PAISES)
    #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #output = f"data/precios_civitatis_{timestamp}.csv"
    #output = f"data/operadores_civitatis_{timestamp}.csv"
    
    # IMPORTANTE: Nombre fijo para que GitHub pueda acumular datos
    output = "data/operadores_civitatis_incremental.csv"
    
    # Lógica de Checkpoint: Leer qué URLs ya procesamos
    urls_procesadas = set()
    if os.path.exists(output):
        try:
            df_existente = pd.read_csv(output)
            if 'url_fuente' in df_existente.columns:
                urls_procesadas = set(df_existente['url_fuente'].unique())
                print(f"✅ Se encontraron {len(urls_procesadas)} destinos ya procesados. Saltando...")
        except Exception:
            pass

    # Filtramos la lista de destinos para procesar solo los pendientes
    destinos_pendientes = [d for d in destinos if f"https://www.civitatis.com/es/{d['url']}/" not in urls_procesadas]
    
    if not destinos_pendientes:
        print("🙌 ¡Todos los destinos ya están scrapeados!")
        return

    scraper = CivitatisScraper()
    # Tu scraper ya tiene el método _save_incremental que usa mode='a' (append)
    # así que esto escribirá línea por línea sin borrar lo anterior.
    await scraper.extract_list(destinos_pendientes, output, currency_code="CLP")

async def ejecutar_nomades():
    tareas = parsear_destinos_nomades('destinos_nomades.txt')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = f"data/precios_nomades_{timestamp}.csv"
    scraper = NomadesScraper()
    await scraper.extract_list(tareas, output)

if __name__ == "__main__":
    if not os.path.exists('data'): os.makedirs('data')
    
    print("--- INSPECTOR DE PRECIOS ---")
    print("1. Ejecutar Scraper Civitatis")
    print("2. Ejecutar Scraper Nomades")
    #opcion = input("Selecciona una opción (1 o 2): ")
    opcion = "1"

    if opcion == "1":
        asyncio.run(ejecutar_civitatis())
    elif opcion == "2":
        asyncio.run(ejecutar_nomades())
    else:
        print("Opción no válida.")
"""