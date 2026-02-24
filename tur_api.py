import requests
import json

def descargar_y_limpiar_json():
    url = "https://www.tur.com/tur/api/v1/marketplace/octo/products?perPage=3000"
    archivo_salida = "productos_bigquery.jsonl"
    
    print(f"Descargando datos desde {url}...")
    
    try:
        response = requests.get(url)
        response.raise_for_status() # Lanza error si la descarga falla
        
        datos_completos = response.json()
        
        # Extraemos solo la lista de productos
        productos = datos_completos.get("data", [])
        
        print(f"Procesando {len(productos)} productos...")
        
        # Guardamos en formato JSON Lines (necesario para BigQuery manual)
        with open(archivo_salida, "w", encoding="utf-8") as f:
            for item in productos:
                # Convertimos cada objeto a string en una sola línea
                linea = json.dumps(item, ensure_ascii=False)
                f.write(linea + "\n")
        
        print(f"¡Éxito! Archivo guardado como: {archivo_salida}")
        print("Ahora puedes subir este archivo manualmente a BigQuery.")

    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    descargar_y_limpiar_json()