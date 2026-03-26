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
        "Buenos Aires": "baffdddb-d777-4799-861b-69f2bf126d25",
        "San Carlos de Bariloche": "63981e4f-ab7d-4f34-b0f1-1f1cd33bd7a1",
        "Mendoza": "3fc10ae2-18c3-4b56-9743-6bc0f97aad5a",
        "El Calafate": "7b6c1217-0cd2-4349-bdac-2935fce883ec",
        "Resto Argentina": "d61b4417-418d-4a9c-a29c-956a6db8a8f7"
    },
    "Brasil": {
        "Río de Janeiro": "2bb31de3-6034-410c-a697-609d7444f5c1",
        "Foz de Iguazú": "6b6a092a-0d73-47ba-a76b-6980ddb48d09",
        "Florianópolis": "392e6b82-0159-4440-bf1c-aae02f02eaa5",
        "São Paulo": "a6d03545-03b3-4ee8-8c33-0a4cf051e547",
        "Salvador de Bahía": "16049be6-86a8-4c43-96a1-9adeb90568c5",
        "Resto Brasil": "d2e9c919-967d-4110-b2e5-7321f71ed89f"
    },

    "Chile": {
        "San Pedro de Atacama": "da445c09-8365-459c-9365-8c3b15055cfc",
        "Puerto Natales": "b0d03bad-1111-49b0-ae73-1d231a14a29a",
        "Santiago": "18c2a2ee-a72f-4604-839b-4f63c979301c",
        "Rapa Nui": "65e14459-b8b8-40d8-9678-6f1e02870a59",
        "Resto Chile": "0a4051f1-ea4b-4d84-b959-0d22ad47ad34"
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