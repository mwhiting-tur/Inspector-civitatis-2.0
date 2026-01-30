import requests
from bs4 import BeautifulSoup
import csv
import time
import urllib3

# Silencia la advertencia de "Insecure Request" al usar verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def scraping_triviantes():
    base_url = "https://www.triviantes.com/"
    

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print("Obteniendo lista de destinos...")
    try:
        response = requests.get(base_url, headers=header, verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        menu_items = soup.find_all('li', class_='menu-item')
        destinos_urls = []
        
        for item in menu_items:
            link = item.find('a', href=True)
            if link and 'location_id' in link['href']:
                nombre_destino = link.text.strip()
                
                # --- LÓGICA DE FILTRADO ---
                # Si el nombre contiene "Todos", lo ignoramos
                if "todos" in nombre_destino.lower():
                    continue
                
                destinos_urls.append({
                    "nombre": nombre_destino,
                    "url": link['href']
                })
        
        # Eliminar duplicados de la lista
        destinos_urls = [dict(t) for t in {tuple(d.items()) for d in destinos_urls}]
        print(f"Se encontraron {len(destinos_urls)} destinos específicos (excluyendo 'Todos').")

    except Exception as e:
        print(f"Error crítico: {e}")
        return
    
    # Preparar el archivo CSV
    with open('data/actividades_triviantes.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Destino', 'Nombre Actividad', 'Precio'])

        for destino in destinos_urls:
            print(f"Procesando: {destino['nombre']}...")
            try:
                response = requests.get(destino['url'], headers=header, verify=False)
                soup = BeautifulSoup(response.text, 'html.parser')

                # Encontrar todos los contenedores de actividades
                # Ajustado a las clases comunes de este sitio (item o st-service-item)
                items = soup.find_all('div', class_='item-service') 
                
                if not items:
                    # Intento alternativo por si la estructura cambia ligeramente
                    items = soup.select('.st-search-result .item-service')

                for item in items:
                    # Extraer nombre (clase plr15 mencionada)
                    nombre_el = item.find(class_='service-title')
                    nombre_txt = nombre_el.get_text(strip=True) if nombre_el else "N/A"

                    # Extraer precio
                    precio_el = item.find(class_='price')
                    # Intentamos limpiar el texto del precio si trae basura
                    precio_txt = precio_el.get_text(strip=True).replace('\n', '').strip() if precio_el else "N/A"

                    writer.writerow([destino['nombre'], nombre_txt, precio_txt])
                
                # Respeto al servidor
                time.sleep(0.5) 

            except Exception as e:
                print(f"Error en {destino['nombre']}: {e}")

    print("\nArchivo 'actividades_triviantes.csv' generado con éxito.")

if __name__ == "__main__":
    scraping_triviantes()