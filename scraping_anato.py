import asyncio
import csv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# --- CONFIGURACIÓN PARA GITHUB ACTIONS ---
MAX_CONCURRENTE = 5  # Pestañas simultáneas (GitHub aguanta 5 sin problema)
CSV_FILENAME = 'expositores_anato_final.csv'
TIMEOUT_MS = 15000   # 15 segundos (Si no carga en 15s, es mejor reintentar)
MAX_RETRIES = 2      # Cuántas veces reintentar si da timeout

async def extraer_detalle(exhibitor, context, sem, csv_lock, fieldnames):
    """Procesa una sola página de detalle de forma concurrente con reintentos."""
    async with sem:
        href = exhibitor['href']
        
        if not href:
            await guardar_en_csv(exhibitor, csv_lock, fieldnames)
            return

        for intento in range(MAX_RETRIES):
            detail_page = await context.new_page()
            try:
                # Navegamos al detalle con un timeout más corto
                await detail_page.goto(href, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                
                # --- EXTRACCIÓN DE DATOS ---
                origen_loc = detail_page.locator('.info-content:has(span.info-label:has-text("Origen")) span.info-value')
                if await origen_loc.count() > 0:
                    exhibitor['Origen'] = (await origen_loc.first.inner_text()).strip()
                    
                web_loc = detail_page.locator('.info-content:has(span.info-label:has-text("Sitio Web")) span.info-value a')
                if await web_loc.count() > 0:
                    exhibitor['Sitio Web'] = (await web_loc.first.get_attribute('href')).strip()
                    
                desc_loc = detail_page.locator('p.exhibitor-description-detail')
                if await desc_loc.count() > 0:
                    exhibitor['Descripción'] = (await desc_loc.first.inner_text()).replace('\n', ' ').strip()
                    
                print(f"✅ [ÉXITO] Extraído: {exhibitor['Título']}")
                break # Si tuvo éxito, rompemos el bucle de reintentos
                    
            except PlaywrightTimeoutError:
                if intento < MAX_RETRIES - 1:
                    print(f"🔄 Reintentando {exhibitor['Título']} (Intento {intento + 2}/{MAX_RETRIES})...")
                else:
                    print(f"⚠️ [TIMEOUT] Se agotaron los reintentos para: {exhibitor['Título']}")
            except Exception as e:
                print(f"❌ [ERROR] {exhibitor['Título']}: {e}")
                break # Si es un error raro (no timeout), no reintentamos
            finally:
                await detail_page.close() 
                
        # Guardamos el resultado (ya sea exitoso o N/A por timeout)
        await guardar_en_csv(exhibitor, csv_lock, fieldnames)

async def guardar_en_csv(exhibitor, csv_lock, fieldnames):
    """Escritura segura en el CSV para evitar que dos procesos choquen."""
    async with csv_lock:
        with open(CSV_FILENAME, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            row = {k: exhibitor.get(k, "N/A") for k in fieldnames}
            writer.writerow(row)

async def main():
    fieldnames = ['Título', 'Categoría', 'Origen', 'Sitio Web', 'Descripción']
    
    # Preparamos el archivo
    with open(CSV_FILENAME, mode='w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

    csv_lock = asyncio.Lock()
    sem = asyncio.Semaphore(MAX_CONCURRENTE)

    async with async_playwright() as p:
        # Lanzamos el navegador con un User-Agent de Windows/Chrome real para evitar bloqueos
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        # Bloqueamos recursos pesados
        await context.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,mp4}", lambda route: route.abort())
        context.set_default_timeout(30000)
        
        main_page = await context.new_page()
        
        print("Cargando el directorio principal...")
        try:
            # Damos bastante tiempo a la página principal porque es la más pesada
            await main_page.goto("https://vitrinaturistica.anato.org/directorio-preliminar-de-expositores/", wait_until="domcontentloaded", timeout=90000)
            await main_page.wait_for_selector('.exhibitor-card', timeout=30000)
        except Exception as e:
            print(f"Error fatal cargando la página principal: {e}")
            return
        
        cards = await main_page.locator('.exhibitor-card').all()
        total_cards = len(cards)
        print(f"\n🚀 ¡Se encontraron {total_cards} expositores! Iniciando extracción paralela (Bloques de {MAX_CONCURRENTE})...")
        
        exhibitors_lista = []
        for i in range(total_cards):
            card = main_page.locator('.exhibitor-card').nth(i)
            
            titulo = (await card.locator('.exhibitor-title').inner_text()).strip() if await card.locator('.exhibitor-title').count() > 0 else "Sin título"
            categoria = (await card.locator('.exhibitor-category').inner_text()).strip() if await card.locator('.exhibitor-category').count() > 0 else "Sin categoría"
            
            detail_link = card.locator('a.btn-details')
            href = await detail_link.first.get_attribute('href') if await detail_link.count() > 0 else None
            
            exhibitors_lista.append({
                'Título': titulo,
                'Categoría': categoria,
                'Origen': "N/A",
                'Sitio Web': "N/A",
                'Descripción': "N/A",
                'href': href
            })

        await main_page.close()
        
        # Ejecutamos las tareas concurrentes
        tareas = [asyncio.create_task(extraer_detalle(exh, context, sem, csv_lock, fieldnames)) for exh in exhibitors_lista]
        await asyncio.gather(*tareas)
        
        print(f"\n🎉 ¡Scraping paralelo completado! Revisa los artefactos en GitHub.")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())