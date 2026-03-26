import pandas as pd
import requests
import numpy as np

# ==========================================
# 1. CONFIGURACIÓN DEL ENTORNO
# ==========================================
API_KEY = "88a5369ee04943cba850fe2422d54400"
START_DATE = "2024-12" # Formato YYYY-MM (Ajusta a tu histórico)
END_DATE = "2026-02"   # Formato YYYY-MM
MOCK_MODE = False       # ¡Déjalo en True para probar el código AHORA MISMO!

# Diccionario Maestro: Países -> Destinos -> Segment IDs
# IMPORTANTE: Reemplaza los 'segment_id_...' con los IDs reales de Similarweb.
destinos_por_pais = {
    "Argentina": {
        "Buenos Aires": "0ded623b-908e-4840-95cf-1df83c06ae0a",
        "San Carlos de Bariloche": "e379c8c7-87dc-46db-ac6d-9b44dc578433",
        "Mendoza": "bdcf3560-b760-4ad2-8a23-be82e1a57145",
        "El Calafate": "6fd97fb4-7384-42f0-a73d-3108315e4f22",
        "Resto Argentina": "ea6f193a-87ae-4aa3-8968-3202eb1532af"
    },
    "Brasil": {
        "Río de Janeiro": "dd791aea-c066-42c5-a9d1-51763f030278",
        "Foz de Iguazú": "49a08e9f-9178-4bed-85c0-661a7da39632",
        "Florianópolis": "5a1c72ff-d3e4-4119-bb8d-da49f015cb0a",
        "São Paulo": "974a21b4-ea83-42e7-b04e-04318763a0a5",
        "Salvador de Bahía": "f884226f-850e-4eb5-b40a-b422fa58d2bf",
        "Resto Brasil": "cac9e699-6835-45af-8b43-079bac70ae2e"
    },
    "Chile": {
        "San Pedro de Atacama": "047be2e9-8270-4c16-8a32-7008cb595f5e",
        "Puerto Natales": "358683e9-4243-4ab7-b5a9-c9b85e06f50e",
        "Santiago": "41150621-a451-4e12-91a3-ddf684f83f68",
        "Rapa Nui": "49a6ae55-d3b5-433a-9af9-d48c3fb408cc",
        "Resto Chile": "d699b4de-5693-4c7f-b10c-2b376a988061"
    },    
    "Colombia": {
        "Cartagena de Indias": "351698d9-1371-4776-9934-ec551a4be4fe",
        "Medellín": "807743eb-9744-4aba-b2f0-22ddaa24741b",
        "Bogotá": "153e79be-dbb5-4e14-8dea-e9bd2a1fb894",
        "Resto Colombia": "89d9c940-e2d3-47d1-939b-6c4a7d61c47e"
    },
    "México": {
        "Cancún / Riviera Maya": "3dfd87c3-195f-4616-aa7c-58c2ff783cdc",
        "Ciudad de México": "ab034d78-c60c-4bba-816f-9272333f32d9",
        "Resto México": "ea24b13d-0765-4fb5-9dfa-b4acf7810515"
    },
    "Perú": {
        "Cusco": "b4c16cec-eac1-47c7-b952-7b836cfb15d7",
        "Lima": "6c045d7a-f721-447f-afda-2b89959bb8ca",
        "Resto Peru": "5cd90675-9436-4e05-b2ec-7b62313b6fb2"
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
    # Exportar a CSV en la misma carpeta
    # =====================================================================
    dfs_para_csv = []
    for pais, df in resultados.items():
        df_pais = df.copy()
        df_pais.insert(0, 'País', pais)
        dfs_para_csv.append(df_pais)
        
    df_consolidado = pd.concat(dfs_para_csv)
    df_consolidado.to_csv("Metas_Supply_Mensuales.csv", index_label="Ciudad / Destino", encoding="utf-8-sig")
    print("✅ Exportado exitosamente a 'Metas_Supply_Mensuales.csv'")