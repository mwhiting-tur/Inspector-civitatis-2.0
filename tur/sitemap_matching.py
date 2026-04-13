import pandas as pd
import xml.etree.ElementTree as ET

def extract_spanish_urls(xml_file):
    """Extrae las URLs preferentemente en español del sitemap."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        namespaces = {
            'smp': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'xhtml': 'http://www.w3.org/1999/xhtml'
        }
        
        url_list = set()
        
        for url_tag in root.findall('smp:url', namespaces):
            # Prioridad 1: El contenido de <loc> (que en tu caso es /es/)
            loc = url_tag.find('smp:loc', namespaces)
            if loc is not None and '/es/' in loc.text:
                url_list.add(loc.text)
                continue # Si ya encontramos la /es/ en loc, pasamos al siguiente producto
            
            # Prioridad 2: Buscar en los xhtml:link si el loc no era /es/
            links = url_tag.findall('xhtml:link', namespaces)
            for link in links:
                href = link.get('href', '')
                if '/es/' in href:
                    url_list.add(href)
                    
        return list(url_list)
    except Exception as e:
        print(f"Error al procesar el XML: {e}")
        return []

def run_matching(csv_input, sitemap_input, csv_output):
    # 1. Obtener URLs de interés
    urls_del_sitio = extract_spanish_urls(sitemap_input)
    print(f"Se cargaron {len(urls_del_sitio)} URLs en español.")

    # 2. Leer productos
    df = pd.read_csv(csv_input)

    # 3. Lógica de matching
    def find_match(slug):
        if pd.isna(slug) or slug == "":
            return None
        
        # Buscamos el slug dentro de nuestra lista de URLs filtradas
        for url in urls_del_sitio:
            if str(slug) in url:
                return url
        return "No encontrada"

    print("Emparejando slugs con URLs...")
    df['url'] = df['slug'].apply(find_match)

    # 4. Exportar
    df.to_csv(csv_output, index=False, encoding='utf-8-sig')
    print(f"Proceso completado. Resultado en: {csv_output}")

if __name__ == "__main__":
    # Configura aquí tus nombres de archivo
    run_matching(
        csv_input='tur/productos_tur.csv', 
        sitemap_input='tur/sitemap_tur.xml', 
        csv_output='tur/productos_con_url_es.csv'
    )