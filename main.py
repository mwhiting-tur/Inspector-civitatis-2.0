import asyncio
import os
import json
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

async def ejecutar_civitatis():
    PAISES = [
    "Argentina", "Bolivia", "Brasil", "Chile", "Colombia", "Costa Rica", 
    "Cuba", "Ecuador", "El Salvador", "Guatemala", "Haití", "Honduras", 
    "México", "Nicaragua", "Panamá", "Paraguay", "Perú", "República Dominicana", 
    "Uruguay", "Venezuela", "EEUU", "España", "Italia", "Francia", 
    "Países Bajos", "Reino Unido", "Alemania", "Bélgica", "Portugal", 
    "Turquia", "Grecia", "Austria", "Japón", "China", "Tailandia", "Australia"] # Configurar
    destinos = cargar_destinos_civitatis(PAISES)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #output = f"data/precios_civitatis_{timestamp}.csv"
    output = f"data/operadores_civitatis_{timestamp}.csv"
    scraper = CivitatisScraper()
    await scraper.extract_list(destinos, output, currency_code="CLP")

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