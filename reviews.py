import asyncio
import pandas as pd
import json
import os
import csv
import sys
from datetime import datetime, timedelta
from urllib.parse import urljoin
from playwright.async_api import async_playwright

# --- CONFIGURACIÓN ---
CONCURRENCIA_MAXIMA = 5   # Pestañas simultáneas
TIMEOUT_ACTIVIDAD = 120   # 2 Minutos máx por actividad
MAX_PAGINAS_REVIEWS = 100 # Evita bucles infinitos

# --- DICCIONARIO MESES ---
MESES_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12
}

def parsear_fecha_civitatis(texto_fecha):
    try:
        texto = texto_fecha.strip()
        partes = texto.split('/')
        if len(partes) != 3: return None
        dia = int(partes[0].strip())
        mes_txt = partes[1].strip().lower()[:3] 
        anio = int(partes[2].strip())
        numero_mes = MESES_ES.get(mes_txt, 1) 
        return datetime(anio, numero_mes, dia)
    except:
        return None

def cargar_destinos_civitatis(paises):
    try:
        with open('destinos_civitatis.json', 'r', encoding='utf-8') as f:
            todos = json.load(f)
        paises_lower = [p.lower() for p in paises]
        return [d for d in todos if d.get('nameCountry', '').lower() in paises_lower]
    except Exception as e:
        print(f"❌ Error leyendo JSON: {e}", flush=True)
        return []

class CivitatisTurboScraper:
    SELECTORS = {
        "container": ".o-search-list__item",
        "title": ".comfort-card__title",
        "link": ".comfort-card__title a",
        "next_btn_list": "a.next-element",
        "view_all_btn": "a.button-list-footer",
        "review_container": ".o-container-opiniones-small", 
        "location": ".opi-location",                         
        "date_text": ".a-opiniones-date",                   
        "next_btn_reviews": ".o-pagination .next-element:not(.--deactivated)",
    }

    def __init__(self, output_file):
        self.output_file = output_file
        self.fecha_corte = datetime.now() - timedelta(days=730)
        self.semaphore = asyncio.Semaphore(CONCURRENCIA_MAXIMA)

    async def run(self, nombre_pais):
        # Crear CSV
        if not os.path.exists(self.output_file):
            encabezados = ["pais", "destino", "actividad", "url_actividad", "fecha", "pais_usuario"]
            pd.DataFrame(columns=encabezados).to_csv(
                self.output_file, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_MINIMAL
            )

        print(f"🚀 Iniciando scraper para: {nombre_pais}", flush=True)
        destinos = cargar_destinos_civitatis([nombre_pais])
        
        if not destinos:
            print("⚠️ No se encontraron destinos.", flush=True)
            return

        print(f"✅ {len(destinos)} destinos encontrados.", flush=True)

        async with async_playwright() as p:
            # Lanzar navegador optimizado para servidor
            browser = await p.chromium.launch(
                headless=True, 
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )

            # BLOQUEO DE RECURSOS (Velocidad x10)
            await context.route("**/*", self._block_heavy_resources)

            for destino in destinos:
                await self._procesar_destino_completo(context, nombre_pais, destino)

            await browser.close()

    async def _block_heavy_resources(self, route):
        """Bloquea imágenes, fuentes y trackers."""
        if route.request.resource_type in ["image", "media", "font", "stylesheet", "other"]:
            await route.abort()
        else:
            await route.continue_()

    async def _procesar_destino_completo(self, context, pais, destino_obj):
        page = await context.new_page()
        nombre_destino = destino_obj['name']
        slug_destino = destino_obj['url'] # <--- Extraemos el slug del JSON (ej: santiago-de-chile)
        
        # La URL base siempre termina en slash
        url_destino_base = f"https://www.civitatis.com/es/{slug_destino}/"
        actividades = []

        print(f"\n🌍 {nombre_destino.upper()}: Buscando actividades...", flush=True)
        
        try:
            # Pasamos el slug_destino a la función de listado para filtrar
            actividades = await self._get_activities_list(page, url_destino_base, slug_destino)
        except Exception as e:
            print(f"⚠️ Error listando {nombre_destino}: {e}", flush=True)
        finally:
            await page.close()

        if not actividades: return

        print(f"   ↳ {len(actividades)} actividades válidas (Filtradas por URL). Procesando...", flush=True)

        # Crear tareas concurrentes
        tareas = []
        for act in actividades:
            base_data = {
                "pais": pais, "destino": nombre_destino,
                "actividad": act['titulo'], "url_actividad": act['url']
            }
            tareas.append(self._scrape_reviews_safe(context, base_data))
        
        # Ejecutar lote
        await asyncio.gather(*tareas)

    async def _get_activities_list(self, page, url_destino_base, slug_destino):
        """
        Recorre la paginación y extrae URLs, validando que contengan el slug del destino.
        """
        actividades = []
        try:
            await page.goto(url_destino_base, wait_until="domcontentloaded", timeout=60000)
            
            # Limpieza rápida JS
            await page.evaluate("() => { document.querySelectorAll('.lottie-reveal-overlay, #lottie-modal, ._cookies-banner').forEach(e => e.remove()); }")
            
            # Expandir "Ver todo"
            try:
                await page.click(self.SELECTORS["view_all_btn"], timeout=2000)
            except: pass

            while True:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                items = await page.query_selector_all(self.SELECTORS["container"])
                if not items: break

                for item in items:
                    try:
                        title_el = await item.query_selector(self.SELECTORS["title"])
                        link_el = await item.query_selector(self.SELECTORS["link"])
                        if not link_el: link_el = await item.query_selector("a:not([href='#'])")

                        if title_el and link_el:
                            title = (await title_el.inner_text()).strip()
                            href = await link_el.get_attribute("href")
                            
                            if href:
                                full_url = urljoin(url_destino_base, href)
                                
                                # --- MODIFICACIÓN: FILTRO DE URL ---
                                # 1. Evitar la propia URL del destino
                                if full_url == url_destino_base:
                                    continue
                                
                                # 2. Validar que el slug del JSON (ej: 'santiago-de-chile') esté en la URL
                                if slug_destino not in full_url:
                                    continue
                                    
                                actividades.append({"titulo": title, "url": full_url})
                                
                    except: continue

                next_btn = await page.query_selector(self.SELECTORS["next_btn_list"])
                if next_btn and await next_btn.is_visible():
                    await page.evaluate("(el) => el.click()", next_btn)
                    await page.wait_for_load_state("domcontentloaded")
                else: break
        except: pass
        return actividades

    async def _scrape_reviews_safe(self, context, base_data):
        """Wrapper con Semáforo y Timeout"""
        async with self.semaphore:
            try:
                await asyncio.wait_for(self._extract_reviews_logic(context, base_data), timeout=TIMEOUT_ACTIVIDAD)
            except asyncio.TimeoutError:
                print(f"     ⏰ Timeout en {base_data['actividad']}. Saltando.", flush=True)
            except Exception: pass

    async def _extract_reviews_logic(self, context, base_data):
        page = await context.new_page()
        try:
            url = base_data['url_actividad']
            url_opiniones = f"{url}opiniones/" if not url.endswith("opiniones/") else url
            if not url_opiniones.endswith("/") and "opiniones" not in url_opiniones:
                 url_opiniones = f"{url}/opiniones/"

            await page.goto(url_opiniones, wait_until="domcontentloaded", timeout=45000)
            
            # Limpieza JS
            await page.evaluate("() => { document.querySelectorAll('.lottie-reveal-overlay, #lottie-modal, ._cookies-banner').forEach(e => e.remove()); }")

            if "opiniones" not in page.url: return

            paginas = 0
            while paginas < MAX_PAGINAS_REVIEWS:
                batch = []
                elements = await page.query_selector_all(self.SELECTORS["review_container"])
                if not elements: break

                for el in elements:
                    try:
                        date_el = await el.query_selector(self.SELECTORS["date_text"])
                        date_text = await date_el.inner_text() if date_el else ""
                        fecha_dt = parsear_fecha_civitatis(date_text)
                        
                        if fecha_dt:
                            if fecha_dt < self.fecha_corte: continue # Ignorar vieja
                            fecha_csv = fecha_dt.strftime("%Y-%m-%d")
                        else:
                            fecha_csv = date_text

                        loc_el = await el.query_selector(self.SELECTORS["location"])
                        if loc_el:
                            raw_loc = await loc_el.inner_text()
                            pais_usuario = raw_loc.replace("-", ",").replace("\n", " ").split(",")[-1].strip()
                        else: pais_usuario = "N/A"

                        batch.append({**base_data, "fecha": fecha_csv, "pais_usuario": pais_usuario})
                    except: continue

                self._save_incremental(batch)
                
                next_btn = await page.query_selector(self.SELECTORS["next_btn_reviews"])
                if next_btn and await next_btn.is_visible():
                    await page.evaluate("(el) => el.click()", next_btn)
                    await asyncio.sleep(0.5) # Espera ligera
                    paginas += 1
                else: break
        finally:
            await page.close()

    def _save_incremental(self, data):
        if not data: return
        try:
            df = pd.DataFrame(data)
            df.to_csv(self.output_file, mode='a', index=False, header=False, 
                      encoding='utf-8-sig', sep=';', quoting=csv.QUOTE_MINIMAL)
        except: pass

if __name__ == "__main__":
    if len(sys.argv) > 1:
        PAIS = sys.argv[1]
    else:
        PAIS = "Chile"

    nombre_archivo = f"reviews_{PAIS.lower().replace(' ', '_')}_paises.csv"
    scraper = CivitatisTurboScraper(nombre_archivo)
    asyncio.run(scraper.run(PAIS))