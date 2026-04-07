import asyncio
import os
import json
import sys
import math
import shutil
from datetime import datetime
from drivers.civitatis_semanal import CivitatisScraperSemanal

# --- Configuración ---
MAX_CONCURRENTE = 3  # 3 navegadores paralelos evitan que GitHub Actions se quede sin memoria (OOM)

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

async def procesar_chunk(id_chunk, destinos_chunk, pais_objetivo, moneda_objetivo, timestamp):
    """Ejecuta una instancia aislada del scraper para un bloque de destinos."""
    if not destinos_chunk:
        return None
        
    nombre_archivo_temp = f"data/temp_precios_{pais_objetivo.lower()}_{id_chunk}_{timestamp}.csv"
    print(f"🔄 [Chunk {id_chunk}] Iniciando con {len(destinos_chunk)} destinos...")
    
    scraper = CivitatisScraperSemanal()
    await scraper.extract_list(destinos_chunk, nombre_archivo_temp, currency_code=moneda_objetivo)
    
    print(f"✅ [Chunk {id_chunk}] Finalizado.")
    return nombre_archivo_temp

async def ejecutar_civitatis_semanal(pais_objetivo, moneda_objetivo):
    # 1. Cargar destinos para el país específico
    destinos = cargar_destinos_civitatis([pais_objetivo])
    
    if not destinos:
        print(f"⚠️ No se encontraron destinos para {pais_objetivo}. Revisa destinos_civitatis.json")
        return

    # 2. Generar nombre de archivo único
    timestamp = datetime.now().strftime("%Y%m%d")
    # Incluimos la moneda en el nombre del archivo para mayor claridad
    nombre_archivo_final = f"data/precios_{pais_objetivo.lower()}_{moneda_objetivo.lower()}_{timestamp}.csv"
    
    print(f"🚀 Iniciando scraping TURBO para {pais_objetivo} usando {moneda_objetivo}")
    print(f"📂 Archivo de salida consolidado: {nombre_archivo_final} ({len(destinos)} destinos totales)")
    
    # 3. Dividir el trabajo (Chunks) para procesar en paralelo
    chunk_size = max(1, math.ceil(len(destinos) / MAX_CONCURRENTE))
    chunks = [destinos[i:i + chunk_size] for i in range(0, len(destinos), chunk_size)]
    
    # 4. Iniciar tareas asíncronas
    tareas = []
    for i, chunk in enumerate(chunks):
        tareas.append(procesar_chunk(i + 1, chunk, pais_objetivo, moneda_objetivo, timestamp))
        
    archivos_temp = await asyncio.gather(*tareas)
    
    # 5. Combinar los CSV resultantes nativamente (Más robusto y ligero que Pandas)
    print("\n🔀 Combinando archivos temporales de los chunks...")
    archivos_validos = [f for f in archivos_temp if f and os.path.exists(f)]
    
    if archivos_validos:
        with open(nombre_archivo_final, 'w', encoding='utf-8-sig') as outfile:
            for i, file in enumerate(archivos_validos):
                with open(file, 'r', encoding='utf-8-sig') as infile:
                    if i != 0:
                        infile.readline() # Omitir los encabezados de los archivos subsecuentes
                    shutil.copyfileobj(infile, outfile)
                os.remove(file) # Limpiar el chunk temporal
        print(f"🎉 Scraping paralelo completado con éxito. Todo guardado en {nombre_archivo_final}")
    else:
        print("⚠️ No se generaron datos en esta ejecución.")

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