import csv
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

def scrape_exhibitors():
    url = "https://vitrinaturistica.anato.org/directorio-preliminar-de-expositores/"
    csv_filename = 'expositores_anato_final.csv'
    
    # 1. PREPARAMOS EL CSV AL INICIO (Escribimos solo los encabezados)
    with open(csv_filename, mode='w', newline='', encoding='utf-8-sig') as f:
        fieldnames = ['Título', 'Categoría', 'Origen', 'Sitio Web', 'Descripción']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        
        # Bloqueamos recursos visuales para máxima velocidad
        context.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,mp4}", lambda route: route.abort())
        
        page = context.new_page()
        page.set_default_timeout(60000)
        
        print("Cargando el directorio principal...")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_selector('.exhibitor-card', timeout=30000)
        except PlaywrightTimeoutError:
            print("La página principal tardó en cargar, pero intentaremos continuar...")
        
        # OBTENEMOS TODAS LAS TARJETAS
        cards = page.locator('.exhibitor-card').all()
        total_cards = len(cards)
        print(f"¡Éxito! Se encontraron {total_cards} expositores listos para extraer.")
        
        for i in range(total_cards):
            card = page.locator('.exhibitor-card').nth(i)
            
            # Datos básicos
            title_loc = card.locator('.exhibitor-title')
            cat_loc = card.locator('.exhibitor-category')
            
            title = title_loc.inner_text().strip() if title_loc.count() > 0 else "Sin título"
            category = cat_loc.inner_text().strip() if cat_loc.count() > 0 else "Sin categoría"
            
            origen, sitio_web, descripcion = "N/A", "N/A", "N/A"
            
            # Buscamos el enlace de detalles
            detail_link = card.locator('a.btn-details')
            
            if detail_link.count() > 0:
                href = detail_link.first.get_attribute('href')
                
                if href:
                    detail_page = context.new_page()
                    try:
                        detail_page.goto(href, wait_until="domcontentloaded", timeout=45000)
                        
                        # --- EXTRACCIÓN CON TUS NUEVOS SELECTORES HTML ---
                        
                        # ORIGEN: Buscamos el div que tiene un label con "Origen" y sacamos su info-value
                        origen_loc = detail_page.locator('.info-content:has(span.info-label:has-text("Origen")) span.info-value')
                        if origen_loc.count() > 0:
                            origen = origen_loc.first.inner_text().strip()
                            
                        # SITIO WEB: Buscamos el div que tiene un label con "Sitio Web" y sacamos el href de su etiqueta <a>
                        web_loc = detail_page.locator('.info-content:has(span.info-label:has-text("Sitio Web")) span.info-value a')
                        if web_loc.count() > 0:
                            sitio_web = web_loc.first.get_attribute('href').strip()
                            
                        # DESCRIPCIÓN: Directo por su clase
                        desc_loc = detail_page.locator('p.exhibitor-description-detail')
                        if desc_loc.count() > 0:
                            # Reemplazamos saltos de línea para que no rompa el CSV
                            descripcion = desc_loc.first.inner_text().replace('\n', ' ').strip()
                            
                    except PlaywrightTimeoutError:
                        print(f"⚠️ El servidor ignoró la petición para {title} (Timeout).")
                    except Exception as e:
                        print(f"⚠️ Error inesperado en {title}: {e}")
                    finally:
                        detail_page.close() 
            
            print(f"[{i+1}/{total_cards}] {title} | Origen: {origen} | Web: {sitio_web}")
            
            # 2. GUARDADO INCREMENTAL: Escribimos esta fila en el CSV inmediatamente
            # Usamos mode='a' (append) para añadir al final del archivo sin borrar lo anterior
            with open(csv_filename, mode='a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow({
                    'Título': title,
                    'Categoría': category,
                    'Origen': origen,
                    'Sitio Web': sitio_web,
                    'Descripción': descripcion
                })
            
            # 3. PROTECCIÓN ANTI-BLOQUEO: Pausa de 1 segundo obligatoria
            time.sleep(1)

        print(f"\n✅ ¡Scraping completado 100%! Revisa tu archivo '{csv_filename}'.")
        browser.close()

if __name__ == "__main__":
    scrape_exhibitors()
