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
CONCURRENCIA_MAXIMA = 5   # Pestañas simultáneas (Seguro para GitHub)
TIMEOUT_ACTIVIDAD = 120   # 2 Minutos máx por actividad antes de abandonarla
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
            
            # Contexto persistente
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
        # La URL base siempre termina en slash, ej: .../es/rio-de-janeiro/
        url_destino_base = f"https://www.civitatis.com/es/{destino_obj['url']}/"
        actividades = []

        print(f"\n🌍 {nombre_destino.upper()}: Buscando actividades...", flush=True)
        
        try:
            # Lista de actividades (Secuencial)
            actividades = await self._get_activities_list(page, url_destino_base)
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
            # Envolver en timeout para evitar bloqueos
            tareas.append(self._scrape_reviews_safe(context, base_data))
        
        # Ejecutar lote
        await asyncio.gather(*tareas)

    async def _get_activities_list(self, page, url_destino_base):
        actividades = []
        try:
            # Usamos domcontentloaded en vez de networkidle para evitar bloqueos
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
                                
                                # --- FILTRO DE URL ---
                                # Solo aceptamos la actividad si la URL comienza con la URL del destino.
                                # Ejemplo válido: .../es/rio-de-janeiro/tour-pan-de-azucar/
                                # Ejemplo inválido: .../es/niteroi/tour-niteroi/ (aunque aparezca en la página de Río)
                                # Ejemplo inválido: .../es/tarjeta-esim-brasil/ (no tiene el slug del destino)
                                if full_url.startswith(url_destino_base) and full_url != url_destino_base:
                                    actividades.append({"titulo": title, "url": full_url})
                                # ---------------------
                                
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

"""
import asyncio
import pandas as pd
import json
import os
import csv
from datetime import datetime, timedelta
from urllib.parse import urljoin
from drivers.base_driver import BaseScraper 

# --- DICCIONARIO PARA TRADUCIR MESES ---
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

# --- CARGA DE DESTINOS ---
def cargar_destinos_civitatis(paises):
    try:
        with open('destinos_civitatis.json', 'r', encoding='utf-8') as f:
            todos = json.load(f)
        paises_lower = [p.lower() for p in paises]
        return [d for d in todos if d.get('nameCountry', '').lower() in paises_lower]
    except Exception as e:
        print(f"❌ Error leyendo JSON: {e}")
        return []

class CivitatisCountryReviewScraper(BaseScraper):
    SELECTORS = {
        # LISTA DE ACTIVIDADES
        "container": ".o-search-list__item",
        "title": ".comfort-card__title",
        "link": ".comfort-card__title a",
        "next_btn": "a.next-element",
        "view_all_btn": "a.button-list-footer",
        
        # --- REVIEWS ---
        "review_container": ".o-container-opiniones-small", 
        "location": ".opi-location",                        
        "date_text": ".a-opiniones-date",                   
        
        # PAGINACIÓN DE REVIEWS
        "next_btn_reviews": ".o-pagination .next-element:not(.--deactivated)",
        
        # COOKIES Y POPUPS
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button",
    }

    def __init__(self):
        super().__init__()
        # Configuración: Últimos 2 años
        self.fecha_corte = datetime.now() - timedelta(days=730)
        self.output_file = ""

    async def run_country_scraping(self, nombre_pais, output_csv):
        self.output_file = output_csv
        
        # CREACIÓN INICIAL DEL CSV
        if not os.path.exists(output_csv):
            encabezados = ["pais", "destino", "actividad", "url_actividad", "fecha", "pais_usuario"]
            pd.DataFrame(columns=encabezados).to_csv(
                output_csv, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL
            )
            print(f"📁 Archivo creado: {output_csv}")

        print(f"📂 Cargando destinos para: {nombre_pais}...")
        
        lista_destinos = cargar_destinos_civitatis([nombre_pais])
        if not lista_destinos:
            print(f"⚠️ No hay destinos.")
            return

        print(f"✅ Destinos encontrados: {len(lista_destinos)}. Iniciando...")
        
        await self.init_browser(headless=True)
        self.page = await self.context.new_page()

        try:
            for destino in lista_destinos:
                nombre_destino = destino['name']
                url_destino_base = f"https://www.civitatis.com/es/{destino['url']}/"
                
                print(f"\n🌍 --- {nombre_destino.upper()} ---")
                
                # 1. LISTA DE ACTIVIDADES
                actividades = await self._get_activities_list_robust(url_destino_base)
                print(f"   ↳ {len(actividades)} actividades detectadas.")

                # 2. REVIEWS DE CADA ACTIVIDAD
                for i, act in enumerate(actividades):
                    print(f"   ⭐ [{i+1}/{len(actividades)}] {act['titulo']}")
                    base_data = {
                        "pais": nombre_pais,
                        "destino": nombre_destino,
                        "actividad": act['titulo'],
                        "url_actividad": act['url']
                    }
                    await self._scrape_reviews_for_activity(base_data)

        finally:
            await self.close_browser()

    async def _handle_overlays(self):
        try:
            await self.page.evaluate('() => { document.querySelectorAll(".lottie-reveal-overlay, #lottie-modal, ._cookies-banner, #didomi-host").forEach(el => el.remove()); }')
            cookie = await self.page.query_selector(self.SELECTORS["cookie_btn"])
            if cookie and await cookie.is_visible(): await cookie.click()
        except: pass

    async def _scroll_to_bottom(self):
        await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)

    async def _get_activities_list_robust(self, url_destino):
        actividades_encontradas = []
        try:
            await self.page.goto(url_destino, wait_until="networkidle", timeout=60000)
            await self._handle_overlays()

            view_all = await self.page.query_selector(self.SELECTORS["view_all_btn"])
            if view_all and await view_all.is_visible():
                await self.page.evaluate("(el) => el.click()", view_all)
                await self.page.wait_for_load_state("networkidle")
                await asyncio.sleep(1)

            while True:
                await self._scroll_to_bottom()
                items = await self.page.query_selector_all(self.SELECTORS["container"])
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
                                full_url = urljoin(self.page.url, href)
                                actividades_encontradas.append({"titulo": title, "url": full_url})
                    except: continue

                next_btn = await self.page.query_selector(self.SELECTORS["next_btn"])
                if next_btn and await next_btn.is_visible():
                    await self.page.evaluate("(el) => el.click()", next_btn)
                    await self.page.wait_for_load_state("networkidle")
                    await asyncio.sleep(2)
                else: break

        except: pass
        return actividades_encontradas

    async def _scrape_reviews_for_activity(self, base_data):
        url = base_data['url_actividad']
        url_opiniones = f"{url}opiniones/" if not url.endswith("opiniones/") else url
        if not url_opiniones.endswith("/") and "opiniones" not in url_opiniones:
             url_opiniones = f"{url}/opiniones/"

        try:
            await self.page.goto(url_opiniones, wait_until="domcontentloaded", timeout=45000)
            await self._handle_overlays()

            if "opiniones" not in self.page.url: return 

            # YA NO USAMOS stop_scraping
            
            while True:
                batch = []
                elements = await self.page.query_selector_all(self.SELECTORS["review_container"])
                
                if not elements: break

                for el in elements:
                    try:
                        # 1. FECHA
                        date_el = await el.query_selector(self.SELECTORS["date_text"])
                        date_text = await date_el.inner_text() if date_el else ""
                        
                        fecha_dt = parsear_fecha_civitatis(date_text)
                        
                        # --- MODIFICACIÓN: FILTRADO ---
                        # Si tiene fecha y es vieja, la ignoramos (continue), PERO NO ROMPEMOS EL BUCLE
                        # Si no tiene fecha, asumimos que se guarda por si acaso.
                        if fecha_dt:
                            if fecha_dt < self.fecha_corte:
                                continue # Ignorar esta review, ir a la siguiente
                            fecha_csv = fecha_dt.strftime("%Y-%m-%d")
                        else:
                            fecha_csv = date_text

                        # 2. UBICACIÓN (SOLO PAÍS)
                        loc_el = await el.query_selector(self.SELECTORS["location"])
                        if loc_el:
                            raw_loc = await loc_el.inner_text()
                            texto_normalizado = raw_loc.replace("-", ",").replace("\n", " ").strip()
                            partes = texto_normalizado.split(",")
                            pais_usuario = partes[-1].strip()
                        else:
                            pais_usuario = "N/A"

                        batch.append({
                            "pais": base_data["pais"],
                            "destino": base_data["destino"],
                            "actividad": base_data["actividad"],
                            "url_actividad": base_data["url_actividad"],
                            "fecha": fecha_csv,
                            "pais_usuario": pais_usuario
                        })
                    except: continue

                self._save_incremental(batch)
                
                if len(batch) > 0:
                    print(f"      💾 +{len(batch)} reviews recientes guardadas.")

                # PAGINACIÓN: SIEMPRE CONTINUAMOS MIENTRAS HAYA BOTÓN SIGUIENTE
                next_btn = await self.page.query_selector(self.SELECTORS["next_btn_reviews"])
                
                if next_btn and await next_btn.is_visible():
                    await self.page.evaluate("(el) => el.click()", next_btn)
                    await self.page.wait_for_load_state("networkidle")
                    await asyncio.sleep(1) 
                else:
                    break 

        except Exception as e:
            pass

    def _save_incremental(self, data):
        if not data: return
        df = pd.DataFrame(data)
        file_exists = os.path.isfile(self.output_file)
        df.to_csv(self.output_file, mode='a', index=False, header=not file_exists, encoding='utf-8-sig', sep=';', quoting=csv.QUOTE_ALL)


if __name__ == "__main__":
    import sys
    # Si recibimos argumento (desde GitHub Actions)
    if len(sys.argv) > 1:
        PAIS = sys.argv[1]
    else:
        PAIS = "colombia" # Default para pruebas en tu PC

    print(f"🚀 Iniciando scraper para: {PAIS}")
    
    # Nombre del archivo de salida
    nombre_archivo = f"reviews_{PAIS.lower().replace(' ', '_')}_paises.csv"
    
    scraper = CivitatisCountryReviewScraper()
    asyncio.run(scraper.run_country_scraping(PAIS, nombre_archivo))

"""