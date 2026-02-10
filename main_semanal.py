import asyncio
import os
import json
import sys
from datetime import datetime
from drivers.civitatis_semanal import CivitatisScraperSemanal

# --- Funciones Auxiliares ---
def cargar_destinos_civitatis(paises):
    # Asegúrate de que este archivo existe en tu repo
    ruta_json = 'destinos_civitatis.json'
    if not os.path.exists(ruta_json):
        print(f"❌ Error: No se encontró {ruta_json}")
        return []
        
    with open(ruta_json, 'r', encoding='utf-8') as f:
        todos = json.load(f)
    
    # Filtramos normalizando a minúsculas para evitar errores de mayúsculas/tildes
    paises_lower = [p.lower() for p in paises]
    return [d for d in todos if d.get('nameCountry', '').lower() in paises_lower]

async def ejecutar_civitatis_semanal(pais_objetivo, moneda_objetivo):
    # 1. Cargar destinos para el país específico
    destinos = cargar_destinos_civitatis([pais_objetivo])
    
    if not destinos:
        print(f"⚠️ No se encontraron destinos para {pais_objetivo}. Revisa destinos_civitatis.json")
        return

    # 2. Generar nombre de archivo único
    timestamp = datetime.now().strftime("%Y%m%d")
    # Incluimos la moneda en el nombre del archivo para mayor claridad
    nombre_archivo = f"data/precios_{pais_objetivo.lower()}_{moneda_objetivo.lower()}_{timestamp}.csv"
    
    print(f"🚀 Iniciando scraping para {pais_objetivo} usando {moneda_objetivo}")
    print(f"📂 Archivo de salida: {nombre_archivo} ({len(destinos)} destinos)")
    
    # 3. Ejecutar Scraper
    scraper = CivitatisScraperSemanal()
    await scraper.extract_list(destinos, nombre_archivo, currency_code=moneda_objetivo)

if __name__ == "__main__":
    # Crear carpeta data si no existe
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # Verificamos que lleguen los argumentos desde GitHub Actions
    # sys.argv[0] es el nombre del script
    # sys.argv[1] es el País
    # sys.argv[2] es la Moneda
    if len(sys.argv) >= 3:
        pais_arg = sys.argv[1]
        moneda_arg = sys.argv[2]
        asyncio.run(ejecutar_civitatis_semanal(pais_arg, moneda_arg))
    else:
        print("❌ Error: Faltan argumentos.")
        print("Uso: python main.py \"Pais\" \"Moneda\"")
        print("Ejemplo: python main.py \"Chile\" \"CLP\"")
        sys.exit(1)