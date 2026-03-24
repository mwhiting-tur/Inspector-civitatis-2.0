import requests
import xml.etree.ElementTree as ET
import os
import time

# Configuración
SITEMAP_URL = "https://www.civitatis.com/sitemap.xml"
DATA_FILE = "civitatis_baseline.txt"
CHANGES_FILE = "civitatis_cambios.txt"

# Headers para engañar al firewall (User-Agent de un Chrome real)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
    'Referer': 'https://www.google.com/',
    'Connection': 'keep-alive'
}

# Usamos una sesión para mantener cookies y mejorar la velocidad
session = requests.Session()
session.headers.update(HEADERS)

def get_urls_from_xml(url):
    urls = set()
    print(f"Procesando: {url}")
    
    try:
        # Añadimos un pequeño delay para no ser agresivos
        time.sleep(0.5) 
        response = session.get(url, timeout=30)
        
        # Si el error persiste, lanzará una excepción aquí
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        # 1. Buscar sub-sitemaps (Sitemap Index)
        sub_sitemaps = root.findall('ns:sitemap', namespace)
        for sitemap in sub_sitemaps:
            loc = sitemap.find('ns:loc', namespace).text
            # Omitimos sitemaps de imágenes si solo quieres URLs de actividades
            if "images" not in loc:
                urls.update(get_urls_from_xml(loc))

        # 2. Buscar URLs finales
        entries = root.findall('ns:url', namespace)
        for url_entry in entries:
            loc = url_entry.find('ns:loc', namespace).text
            if "/es/" in loc:
                urls.add(loc)
            
    except Exception as e:
        print(f"Error procesando {url}: {e}")
    
    return urls

def run_comparison():
    # Cargar baseline
    old_urls = set()
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            old_urls = set(line.strip() for line in f if "/es/" in line)
        print(f"Cargadas {len(old_urls)} URLs previas.")

    # Obtener actuales
    print("Iniciando descarga de sitemaps...")
    current_urls = get_urls_from_xml(SITEMAP_URL)
    
    if not current_urls:
        print("CRÍTICO: No se pudieron recuperar URLs. Revisa el bloqueo del servidor.")
        return

    print(f"Total URLs encontradas: {len(current_urls)}")

    # Comparar
    nuevas = current_urls - old_urls
    bajas = old_urls - current_urls

    # Resultados
    print("\n" + "="*30)
    print(f"RESUMEN DE CAMBIOS")
    print("="*30)
    print(f"NUEVAS: {len(nuevas)}")
    print(f"BAJAS:  {len(bajas)}")
    
    if nuevas:
        print("\n[+] Muestra de nuevas actividades:")
        for url in list(nuevas)[:5]: print(f" - {url}")
        
    if bajas:
        print("\n[-] Muestra de actividades eliminadas:")
        for url in list(bajas)[:5]: print(f" - {url}")

    # Guardar reporte de cambios en archivo
    with open(CHANGES_FILE, "w", encoding="utf-8") as f:
        f.write("=== REPORTE DE CAMBIOS ===\n")
        f.write(f"Total Nuevas: {len(nuevas)}\n")
        f.write(f"Total Eliminadas (Bajas): {len(bajas)}\n\n")
        
        if nuevas:
            f.write("--- NUEVAS ACTIVIDADES ---\n")
            for url in sorted(nuevas):
                f.write(f"{url}\n")
            f.write("\n")
            
        if bajas:
            f.write("--- ACTIVIDADES ELIMINADAS ---\n")
            for url in sorted(bajas):
                f.write(f"{url}\n")
    print(f"\nReporte completo de cambios guardado en '{CHANGES_FILE}'")

    # Guardar nuevo baseline
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        for url in sorted(current_urls):
            f.write(f"{url}\n")
    print(f"\nBaseline actualizado exitosamente en {DATA_FILE}")

if __name__ == "__main__":
    run_comparison()