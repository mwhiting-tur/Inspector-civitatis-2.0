import requests
import csv
import datetime

def generar_historico_usd_clp():
    archivo_salida = "historial_usd_clp_2024_2025.csv"
    fecha_inicio_global = "2024-01-01"
    fecha_fin_global = "2025-03-31" 
    
    # Ajustar la fecha final si estamos en una fecha anterior al 31 de marzo de 2025
    hoy = datetime.date.today()
    fecha_fin_dt = datetime.date.fromisoformat(fecha_fin_global)
    if hoy < fecha_fin_dt:
        fecha_fin_dt = hoy
    fecha_fin_global = fecha_fin_dt.isoformat()
    
    # Incluimos 2023 para tener el dato de cierre de año y rellenar el 1 de enero de 2024 (que es feriado)
    anos_clp = [2023, 2024, 2025] 
    
    filas_csv = []
    print(f"--- Generando historial USD-CLP desde {fecha_inicio_global} hasta {fecha_fin_global} ---")

    print(f"1. Descargando USD->CLP de mindicador.cl años {anos_clp} y rellenando vacíos...")
    clp_historico = {}
    
    for anio in anos_clp:
        try:
            url_mindicador = f"https://mindicador.cl/api/dolar/{anio}"
            res_min = requests.get(url_mindicador)
            res_min.raise_for_status()
            for registro in res_min.json().get("serie", []):
                clp_historico[registro["fecha"][:10]] = registro["valor"]
        except Exception as e:
            print(f"   Error descargando año {anio}: {e}")

    fecha_actual = datetime.date.fromisoformat(fecha_inicio_global)
    fecha_fin = datetime.date.fromisoformat(fecha_fin_global)
    ultimo_valor_conocido = None
    
    # Buscar el último valor conocido justo antes de la fecha de inicio
    fecha_busqueda = fecha_actual
    while ultimo_valor_conocido is None:
        if fecha_busqueda.isoformat() in clp_historico:
            ultimo_valor_conocido = clp_historico[fecha_busqueda.isoformat()]
        else:
            fecha_busqueda -= datetime.timedelta(days=1)
            if fecha_busqueda.year < min(anos_clp):
                break

    while fecha_actual <= fecha_fin:
        fecha_str = fecha_actual.isoformat()
        if fecha_str in clp_historico:
            ultimo_valor_conocido = clp_historico[fecha_str]
            
        if ultimo_valor_conocido is not None:
            filas_csv.append({"fecha": fecha_str, "moneda_base": "USD", "moneda_destino": "CLP", "tasa_cambio": ultimo_valor_conocido})
        fecha_actual += datetime.timedelta(days=1)

    # Ordenamos y guardamos el CSV
    if filas_csv:
        filas_csv.sort(key=lambda x: x['fecha'], reverse=True)
        print(f"2. Guardando archivo '{archivo_salida}'...")
        with open(archivo_salida, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["fecha", "moneda_base", "moneda_destino", "tasa_cambio"])
            writer.writeheader()
            writer.writerows(filas_csv)
        print(f"¡Éxito! CSV '{archivo_salida}' generado con {len(filas_csv)} registros.")

if __name__ == "__main__":
    generar_historico_usd_clp()
