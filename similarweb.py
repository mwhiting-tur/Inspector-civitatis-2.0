import pandas as pd
import requests
import numpy as np

# ==========================================
# 1. CONFIGURACIÓN DEL ENTORNO
# ==========================================
API_KEY = "88a5369ee04943cba850fe2422d54400"
START_DATE = "2025-10" # Formato YYYY-MM (Ajusta a tu histórico)
END_DATE = "2025-12"   # Formato YYYY-MM
MOCK_MODE = False       # ¡Déjalo en True para probar el código AHORA MISMO!

# Diccionario Maestro: Países -> Destinos -> Segment IDs
# IMPORTANTE: Reemplaza los 'segment_id_...' con los IDs reales de Similarweb.
destinos_por_pais = {
    "Argentina": {
        "Buenos Aires": "1a0921bf-f631-481d-9eac-2d9dee972dbd",
        "San Carlos de Bariloche": "9e81d410-5bc3-4608-ba9e-7ee6121b29de",
        "Mendoza": "c6cd2695-fd2d-4a28-9ee4-35ce92c9976c",
        "El Calafate": "b31a5deb-1c68-4712-aa0e-ec16f2ed4cf7"
    },
    "Brasil": {
        "Río de Janeiro": "8ff88dc8-4e42-4bfc-a6d3-a6f701c898b7",
        "Foz de Iguazú": "c97f2fc9-fb64-4762-a809-c7dfe1b648e3",
        "Florianópolis": "4d29bca9-53c7-4565-bf38-60034c8a6cc0",
        "São Paulo": "a9dfb78b-702b-454a-87d9-38540ddf05bf",
        "Salvador de Bahía": "29ff3d4c-020b-4872-b791-00b02489b5d5"
    },
    "Chile": {
        "San Pedro de Atacama": "baf171f0-1b1f-40a8-b269-a79668213005",
        "Puerto Natales": "3c51342c-de8c-4754-90e4-a73983b1c217",
        "Santiago": "02429446-f7dc-4911-85f5-54b9ea2b8198",
        "Rapa Nui": "1435d0d6-bf30-45ea-a91d-3e0714b50084"
    },
    "Colombia": {
        "Cartagena de Indias": "56ebc016-21a1-499a-9f8d-cff46c0c8ef7",
        "Medellín": "603f22d9-7071-4b3a-a4c5-9e2e59b915cd",
        "Bogotá": "81e8123f-4dfe-4bf7-b6c8-695c35cd9c2e"
    },
    "México": {
        "Cancún / Riviera Maya": "857956eb-ea1c-434b-82b6-73cf71b26324",
        "Ciudad de México": "50f30ece-8fee-48e2-ba44-f3ea16b69954"
    },
    "Perú": {
        "Cusco": "ce57ffe0-14bf-48fe-805c-ba44014ed782",
        "Lima": "4166d9a9-3e84-4bec-b3ad-bd2b1c3e4fea"
    }
}

# ==========================================
# 2. MOTOR DE EXTRACCIÓN (API)
# ==========================================
def obtener_visitas_mensuales(segment_id, destino):
    if MOCK_MODE:
        # (Tu código de simulación...)
        return {f"2023-{str(m).zfill(2)}": 1000 for m in range(1, 13)}

    # CAMBIO 1: La URL debe terminar en /query
    url = f"https://api.similarweb.com/v1/segment/{segment_id}/traffic-and-engagement/query"
    
    params = {
        "api_key": API_KEY,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "country": "world",
        "granularity": "monthly",
        "metrics": "visits",  # CAMBIO 2: Parámetro obligatorio según doc
        "format": "json"
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 404:
            print(f"❌ Error 404: El ID '{segment_id}' no existe o la URL es incorrecta.")
            return {}
            
        response.raise_for_status()
        data = response.json()
        
        # CAMBIO 3: Según la doc, los datos vienen en la lista 'segments'
        visitas_mensuales = {}
        for item in data.get('segments', []):
            mes = item['date'][:7] # Extrae YYYY-MM
            visitas_mensuales[mes] = item.get('visits', 0)
            
        return visitas_mensuales
        
    except Exception as e:
        print(f"Error en {destino}: {e}")
        return {}

# ==========================================
# 3. PROCESAMIENTO MATEMÁTICO
# ==========================================
def procesar_modelo_supply():
    tablas_finales = {}
    print("Extrayendo datos y calculando Market Share Estacionalizado...\n")
    
    for pais, destinos in destinos_por_pais.items():
        data_absoluta = {}
        
        # 1. Extraer datos crudos (absolutos)
        for destino, segment_id in destinos.items():
            data_absoluta[destino] = obtener_visitas_mensuales(segment_id, destino)
            
        # 2. Crear DataFrame (Destinos x Meses con tráfico absoluto)
        df_absoluto = pd.DataFrame.from_dict(data_absoluta, orient='index')
        df_absoluto = df_absoluto.fillna(0)
        
        # 3. EL CÁLCULO CLAVE: Share vertical por columna
        # Dividimos cada celda por la suma total de su columna. 
        # Esto aplica la estacionalidad de golpe y obliga a sumar 1.0 (100%)
        df_share = df_absoluto.div(df_absoluto.sum(axis=0), axis=1)
        
        tablas_finales[pais] = df_share
        
    return tablas_finales

# ==========================================
# 4. VISUALIZACIÓN Y EXPORTACIÓN
# ==========================================
if __name__ == "__main__":
    resultados = procesar_modelo_supply()
    
    # Imprimir en consola para revisión
    for pais, df in resultados.items():
        print(f"{'='*70}")
        print(f"🌍 PAÍS: {pais.upper()} - SHARE ESTACIONAL (%)")
        print(f"{'='*70}")
        
        # Multiplicar por 100 y formatear visualmente a porcentaje con 1 decimal
        df_impresion = (df * 100).map(lambda x: f"{x:.1f}%")
        
        # Fila de comprobación (siempre debe mostrar 100.0% abajo)
        df_impresion.loc['TOTAL PAÍS'] = (df.sum(axis=0) * 100).apply(lambda x: f"{round(x, 1)}%")
        df_impresion.index.name = 'Ciudad / Destino'
        
        print(df_impresion.to_markdown())
        print("\n")

    # =====================================================================
    # OPCIONAL: Generar Excel (Descomenta las 4 líneas de abajo para usar)
    # =====================================================================
    with pd.ExcelWriter("Metas_Supply_Mensuales.xlsx") as writer:
        for pais, df in resultados.items():
    #         # Exportar valores numéricos puros (0.0 a 1.0) para multiplicar en Excel
            df.to_excel(writer, sheet_name=pais[:31], index_label="Ciudad / Destino")
    print("✅ Exportado exitosamente a 'Metas_Supply_Mensuales.xlsx'")