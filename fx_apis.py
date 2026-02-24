import requests
import csv
import datetime

def generar_historial_2025_2026():
    # --- CONFIGURACIÓN ---
    archivo_salida = "historial_monedas_2025_2026.csv"
    fecha_inicio_global = "2025-03-01" 
    
    hoy = datetime.date.today()
    ayer = hoy - datetime.timedelta(days=1)
    fecha_fin_global = ayer.isoformat()
    anos_clp = [2025, 2026] 
    
    filas_csv = []
    monedas_destino = "COP,BRL,ARS,PEN,MXN"
    print(f"--- Generando historial desde {fecha_inicio_global} hasta {fecha_fin_global} ---")

    # ---------------------------------------------------------
    # PARTE 1.1: Monedas Generales (Base USD)
    # ---------------------------------------------------------
    print(f"1.1 Descargando {monedas_destino} (Base USD) desde fxratesapi...")
    url_usd = f"https://api.fxratesapi.com/timeseries?start_date={fecha_inicio_global}&end_date={fecha_fin_global}&base=USD&currencies={monedas_destino}&format=json"
    
    try:
        res_usd = requests.get(url_usd)
        if res_usd.status_code == 200 and res_usd.json().get("success"):
            for fecha, monedas in res_usd.json().get("rates", {}).items():
                fecha_limpia = fecha.split("T")[0]
                for moneda, valor in monedas.items():
                    if valor is not None:
                        filas_csv.append({"fecha": fecha_limpia, "moneda_base": "USD", "moneda_destino": moneda, "tasa_cambio": valor})
        else:
            print("   Error API FX (USD).")
    except Exception as e:
        print(f"   Error de conexión FX (USD): {e}")

    # ---------------------------------------------------------
    # PARTE 1.2: Monedas Generales (Base CLP)
    # ---------------------------------------------------------
    print(f"1.2 Descargando {monedas_destino} (Base CLP) desde fxratesapi...")
    url_clp = f"https://api.fxratesapi.com/timeseries?start_date={fecha_inicio_global}&end_date={fecha_fin_global}&base=CLP&currencies={monedas_destino}&format=json"
    
    try:
        res_clp_fx = requests.get(url_clp)
        if res_clp_fx.status_code == 200 and res_clp_fx.json().get("success"):
            for fecha, monedas in res_clp_fx.json().get("rates", {}).items():
                fecha_limpia = fecha.split("T")[0]
                for moneda, valor in monedas.items():
                    if valor is not None:
                        filas_csv.append({"fecha": fecha_limpia, "moneda_base": "CLP", "moneda_destino": moneda, "tasa_cambio": valor})
        else:
            print("   Error API FX (CLP).")
    except Exception as e:
        print(f"   Error de conexión FX (CLP): {e}")

    # ---------------------------------------------------------
    # PARTE 2: Peso Chileno (Mindicador.cl) con Relleno
    # ---------------------------------------------------------
    print(f"2. Descargando USD->CLP de mindicador.cl años {anos_clp} y rellenando vacíos...")
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

    # ---------------------------------------------------------
    # PARTE 3: Guardar CSV
    # ---------------------------------------------------------
    if filas_csv:
        filas_csv.sort(key=lambda x: (x['fecha'], x['moneda_base']), reverse=True)
        print(f"3. Guardando archivo '{archivo_salida}'...")
        try:
            with open(archivo_salida, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=["fecha", "moneda_base", "moneda_destino", "tasa_cambio"])
                writer.writeheader()
                writer.writerows(filas_csv)
            print("¡Éxito! CSV generado.")
        except Exception as e:
            print(f"Error escribiendo archivo: {e}")

if __name__ == "__main__":
    generar_historial_2025_2026()