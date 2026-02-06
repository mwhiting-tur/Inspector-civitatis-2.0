import requests
from bs4 import BeautifulSoup
import csv
import time
import urllib3

# Silenciar advertencias SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def scraping_triviantes():
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    base_url = "https://www.triviantes.com/"
    
    print("--- INICIANDO SCRAPING ---")
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
                
                # Filtrar "Todos"
                if "todos" in nombre_destino.lower():
                    continue
                
                destinos_urls.append({
                    "nombre": nombre_destino,
                    "url": link['href']
                })
        
        # Eliminar duplicados
        destinos_urls = [dict(t) for t in {tuple(d.items()) for d in destinos_urls}]
        print(f"Destinos encontrados: {len(destinos_urls)}")

    except Exception as e:
        print(f"Error crítico inicial: {e}")
        return

    # Guardar en CSV con la nueva columna 'Descripción'
    with open('actividades_detalladas.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Destino', 'Nombre Actividad', 'Precio', 'Descripción'])

        for destino in destinos_urls:
            print(f"\nEntrando a destino: {destino['nombre']}...")
            try:
                res = requests.get(destino['url'], headers=header, verify=False)
                s = BeautifulSoup(res.text, 'html.parser')

                actividades = s.find_all('div', class_='item-service')
                
                if not actividades:
                    print(f"  - No se encontraron actividades en {destino['nombre']}")

                for act in actividades:
                    # 1. Extraer datos básicos
                    nombre_el = act.find(class_='service-title')
                    nombre_txt = nombre_el.get_text(strip=True) if nombre_el else "N/A"

                    precio_el = act.find(class_='price')
                    precio_txt = precio_el.get_text(strip=True).replace('\n', ' ').strip() if precio_el else "N/A"

                    # 2. Buscar el LINK para entrar al detalle
                    # Buscamos el enlace 'a' dentro del título o del contenedor
                    link_detalle = None
                    if nombre_el and nombre_el.find('a', href=True):
                         link_detalle = nombre_el.find('a', href=True)['href']
                    elif act.find('a', href=True):
                         link_detalle = act.find('a', href=True)['href']

                    descripcion_txt = "No disponible"

                    # 3. Entrar a la página interna si existe el link
                    if link_detalle:
                        try:
                            # A veces el link viene relativo, asegurar que sea completo (aunque en WP suele ser absoluto)
                            if not link_detalle.startswith('http'):
                                link_detalle = base_url.rstrip('/') + link_detalle

                            # Petición a la página interna
                            res_detalle = requests.get(link_detalle, headers=header, verify=False)
                            s_detalle = BeautifulSoup(res_detalle.text, 'html.parser')

                            # Extraer la descripción con la clase que indicaste
                            desc_el = s_detalle.find(class_='st-description')
                            
                            if desc_el:
                                # get_text con separador de espacio para evitar palabras pegadas
                                descripcion_txt = desc_el.get_text(separator=' ', strip=True)
                                # Limpieza extra de espacios
                                descripcion_txt = " ".join(descripcion_txt.split())
                            else:
                                descripcion_txt = "Sin descripción (clase no encontrada)"

                        except Exception as e_det:
                            descripcion_txt = f"Error al leer detalle: {e_det}"
                            print(f"  x Error leyendo detalle de '{nombre_txt}': {e_det}")

                    # Escribir fila
                    writer.writerow([destino['nombre'], nombre_txt, precio_txt, descripcion_txt])
                    print(f"  > Guardado: {nombre_txt[:30]}...")
                    
                    # Pausa pequeña entre actividades para no saturar
                    time.sleep(0.5)

            except Exception as e:
                print(f"Error procesando destino {destino['nombre']}: {e}")

    print("\n¡Proceso finalizado! Archivo: 'actividades_detalladas.csv'")

if __name__ == "__main__":
    scraping_triviantes()

# abajo solo por actividad, sin descripcion 
"""
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
"""