import asyncio
import os
import json
import sys
from datetime import datetime
from drivers.civitatis_operadores import CivitatisScraper

def cargar_destinos_civitatis(paises):
    """Carga y filtra destinos basados en el JSON local."""
    if not os.path.exists('destinos_civitatis.json'):
        print("❌ Error: No se encontró destinos_civitatis.json")
        return []
        
    with open('destinos_civitatis.json', 'r', encoding='utf-8') as f:
        todos = json.load(f)
    
    paises_lower = [p.lower() for p in paises]
    return [d for d in todos if d.get('nameCountry', '').lower() in paises_lower]

async def ejecutar_civitatis_operadores(pais_objetivo):
    """Ejecuta el scraper de operadores para un país específico."""
    destinos = cargar_destinos_civitatis([pais_objetivo])
    
    if not destinos:
        print(f"⚠️ No se encontraron destinos para {pais_objetivo}.")
        return
        
    # Nombre de archivo único por país para evitar choques en ejecución paralela
    nombre_archivo = f"data/operadores_{pais_objetivo.lower().replace(' ', '_')}.csv"
    
    print(f"🚀 Iniciando scraping de operadores para {pais_objetivo} ({len(destinos)} destinos)")
    
    scraper = CivitatisScraper()
    # Ejecutamos el scraper (Ajusta la moneda si prefieres otra)
    await scraper.extract_list(destinos, nombre_archivo, currency_code="CLP")

if __name__ == "__main__":
    # Asegurar que la carpeta data existe
    if not os.path.exists('data'): 
        os.makedirs('data')
    
    # Capturar el país enviado por GitHub Actions (scraper_aut.yml)
    if len(sys.argv) > 1:
        pais_arg = sys.argv[1]
        asyncio.run(ejecutar_civitatis_operadores(pais_arg))
    else:
        print("❌ Error: Faltan argumentos. Se debe enviar el nombre del país.")
        print("Ejemplo: python main.py 'Chile'")
        sys.exit(1)