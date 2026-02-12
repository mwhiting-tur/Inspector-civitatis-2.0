import asyncio
import os
import pandas as pd
from drivers.civitatis_semanal import CivitatisScraperSemanal

# --- Configuración del Test ---
DESTINO_PRUEBA = [
    {
        "name": "Santiago",        # Nombre
        "nameCountry": "Chile",    # País
        "url": "santiago-de-chile" # El slug de la URL de Civitatis
    }
]
ARCHIVO_SALIDA = "data/test_debug.csv"

async def test_rapido():
    # 1. Crear carpeta data si no existe
    if not os.path.exists('data'):
        os.makedirs('data')
        
    # 2. Limpiar archivo previo si existe
    if os.path.exists(ARCHIVO_SALIDA):
        os.remove(ARCHIVO_SALIDA)

    print("🚀 Iniciando Test Rápido...")
    
    # 3. Inicializar Scraper
    scraper = CivitatisScraperSemanal()
    
    # 4. Ejecutar scraping (Solo para el destino de prueba)
    # NOTA: Esto abrirá el navegador, extraerá datos y guardará el CSV
    await scraper.extract_list(DESTINO_PRUEBA, ARCHIVO_SALIDA, currency_code="CLP")
    
    print("\n✅ Scraping finalizado. Verificando datos...\n")

    # 5. Auditoría de datos
    if os.path.exists(ARCHIVO_SALIDA):
        df = pd.read_csv(ARCHIVO_SALIDA)
        
        # Mostrar las primeras 3 filas
        print("--- MUESTRA DE DATOS ---")
        print(df.head(3).to_markdown(index=False))
        
        print("\n--- TIPOS DE DATOS DETECTADOS ---")
        print(df.dtypes)
        
        # Verificación específica de tus requerimientos
        print("\n--- VALIDACIÓN DE REGLAS ---")
        
        # Check Fecha
        fecha_ejemplo = df['fecha_scan'].iloc[0]
        print(f"1. Formato Fecha (debe ser YYYY-MM-DD): {fecha_ejemplo} -> {'CORRECTO' if len(fecha_ejemplo) == 10 else 'ERROR'}")
        
        # Check Precios (Float)
        es_float_precio = pd.api.types.is_float_dtype(df['precio_real'])
        print(f"2. Precio Real es Float: {es_float_precio}")
        
        # Check Viajeros (Int)
        es_int_viajeros = pd.api.types.is_integer_dtype(df['viajeros'])
        print(f"3. Viajeros es Int: {es_int_viajeros}")
        
    else:
        print("❌ Error: No se generó el archivo CSV.")

if __name__ == "__main__":
    asyncio.run(test_rapido())