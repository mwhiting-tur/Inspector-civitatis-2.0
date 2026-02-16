import asyncio
import os
import json
from drivers.civitatis_operadores import CivitatisScraper

# --- Configuración de la Prueba ---
PAIS_DE_PRUEBA = "Chile"  # <--- Cambia esto por el país que quieras
MONEDA_DE_PRUEBA = "USD"
# ----------------------------------

def cargar_destinos_civitatis(paises):
    if not os.path.exists('destinos_civitatis.json'):
        print("❌ Error: No se encontró destinos_civitatis.json")
        return []
    with open('destinos_civitatis.json', 'r', encoding='utf-8') as f:
        todos = json.load(f)
    paises_lower = [p.lower() for p in paises]
    return [d for d in todos if d.get('nameCountry', '').lower() in paises_lower]

async def ejecutar_test():
    if not os.path.exists('data'): 
        os.makedirs('data')

    # 1. Cargar destinos del país
    destinos = cargar_destinos_civitatis([PAIS_DE_PRUEBA])
    
    if not destinos:
        print(f"⚠️ No se encontraron destinos para {PAIS_DE_PRUEBA}")
        return

    # 2. MODO TURBO: Nos quedamos solo con el PRIMER destino para no esperar horas
    # (Si quieres que recorra todo el país, borra el "[:1]")
    destinos_prueba = destinos[:1] 

    nombre_archivo = f"data/test_operadores_{PAIS_DE_PRUEBA.lower()}.csv"
    
    # Limpiamos el archivo de prueba anterior si existe para ver datos frescos
    if os.path.exists(nombre_archivo):
        os.remove(nombre_archivo)

    print(f"🚀 Iniciando TEST LOCAL para: {PAIS_DE_PRUEBA}")
    print(f"📍 Destino a procesar en esta prueba: {destinos_prueba[0]['name']}")
    print(f"📂 El resultado se guardará en: {nombre_archivo}\n")

    # 3. Ejecutar Scraper
    scraper = CivitatisScraper()
    await scraper.extract_list(destinos_prueba, nombre_archivo, currency_code=MONEDA_DE_PRUEBA)

    print(f"\n✅ Test finalizado. Revisa el archivo: {nombre_archivo}")

if __name__ == "__main__":
    asyncio.run(ejecutar_test())