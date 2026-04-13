import asyncio
import os
import json
import sys
from datetime import datetime
from drivers.civitatis_cutoff import CivitatisCutoffScraper

def cargar_destinos_civitatis(paises):
    """Carga y filtra destinos basados en el JSON local."""
    if not os.path.exists('destinos_civitatis.json'):
        print("❌ Error: No se encontró destinos_civitatis.json")
        return []

    with open('destinos_civitatis.json', 'r', encoding='utf-8') as f:
        todos = json.load(f)

    paises_lower = [p.lower() for p in paises]
    return [d for d in todos if d.get('nameCountry', '').lower() in paises_lower]

async def ejecutar_civitatis_cutoff(pais_objetivo):
    """Ejecuta el scraper de cutoff para un país específico."""
    destinos = cargar_destinos_civitatis([pais_objetivo])

    if not destinos:
        print(f"⚠️ No se encontraron destinos para {pais_objetivo}.")
        return

    nombre_archivo = f"data/cutoff_{pais_objetivo.lower().replace(' ', '_')}.csv"

    print(f"🚀 Iniciando scraping de cutoff para {pais_objetivo} ({len(destinos)} destinos)")

    scraper = CivitatisCutoffScraper()
    await scraper.extract_list(destinos, nombre_archivo, currency_code="CLP")

if __name__ == "__main__":
    if not os.path.exists('data'):
        os.makedirs('data')

    if len(sys.argv) > 1:
        pais_arg = sys.argv[1]
        asyncio.run(ejecutar_civitatis_cutoff(pais_arg))
    else:
        print("❌ Error: Faltan argumentos. Se debe enviar el nombre del país.")
        print("Ejemplo: python main_cutoff.py 'Chile'")
        sys.exit(1)
