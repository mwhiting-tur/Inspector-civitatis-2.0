import asyncio
import pandas as pd
import json
import os
import csv
import sys
import unicodedata
from datetime import datetime, timedelta
from urllib.parse import urljoin
from playwright.async_api import async_playwright

# --- CONFIGURACIÓN OPTIMIZADA ---
CONCURRENCIA_MAXIMA = 3      # Pestañas simultáneas
TIMEOUT_ACTIVIDAD = 600      # 10 Minutos máx por actividad
MAX_PAGINAS_REVIEWS = 5000   # Prácticamente sin límite
DIAS_HISTORIA = 1825         # 5 años (365 * 5)
MAX_REINTENTOS = 3           # Intentos si una página falla

# --- DICCIONARIO MESES ---
MESES_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12
}

def normalizar_texto(texto):
    """Limpia acentos, mayúsculas y espacios para una comparación perfecta."""
    if not texto: return ""
    texto_limpio = unicodedata.normalize('NFKD', str(texto)).encode('ASCII', 'ignore').decode('utf-8')
    return texto_limpio.lower().strip()

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

def cargar_destino_civitatis(input_destino):
    """Busca el destino exacto cruzando la ciudad y el país."""
    try:
        input_limpio = input_destino.split(':')[0].strip()
        partes = [p.strip() for p in input_limpio.split(',')]
        
        nombre_ciudad = normalizar_texto(partes[0])
        nombre_pais = normalizar_texto(partes[1]) if len(partes) > 1 else ""

        with open('destinos_civitatis.json', 'r', encoding='utf-8') as f:
            todos = json.load(f)
            
        for d in todos:
            d_name = normalizar_texto(d.get('name', ''))
            d_country = normalizar_texto(d.get('nameCountry', ''))
            if nombre_ciudad == d_name and (not nombre_pais or nombre_pais == d_country):
                return d
                
        for d in todos:
            d_name = normalizar_texto(d.get('name', ''))
            d_country = normalizar_texto(d.get('nameCountry', ''))
            if nombre_ciudad in d_name and (not nombre_pais or nombre_pais in d_country):
                return d
                
        return None
    except Exception as e:
        print(f"❌ Error leyendo JSON: {e}", flush=True)
        return None

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


    def __init__(self, destino_input):
            self.destino_input = destino_input
            self.destino_limpio = normalizar_texto(destino_input.split(':')[0]).replace(", ", "_").replace(" ", "_")
            
            # --- NUEVO: CREAR CARPETA AISLADA ---
            os.makedirs("resultados", exist_ok=True)
            
            # Guardamos dentro de la carpeta resultados/
            self.output_file = f"resultados/reviews_{self.destino_limpio}.csv"
            self.progress_file = f"resultados/progreso_{self.destino_limpio}.txt"
            
            self.fecha_corte = datetime.now() - timedelta(days=DIAS_HISTORIA)
            self.semaphore = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
            self.actividades_completadas = self._cargar_progreso()

    def _cargar_progreso(self):
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return set(line.strip() for line in f if line.strip())
        return set()

    def _guardar_progreso(self, url):
        with open(self.progress_file, 'a', encoding='utf-8') as f:
            f.write(url + '\n')
        self.actividades_completadas.add(url)

    async def run(self):
        if not os.path.exists(self.output_file):
            encabezados = ["pais", "destino", "actividad", "url_actividad", "fecha", "pais_usuario"]
            pd.DataFrame(columns=encabezados).to_csv(
                self.output_file, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_MINIMAL
            )

        print(f"🚀 Iniciando scraper para el destino: {self.destino_input}", flush=True)
        destino_obj = cargar_destino_civitatis(self.destino_input)
        
        if not destino_obj:
            print(f"⚠️ No se encontró el destino '{self.destino_input}' en el JSON.", flush=True)
            return

        nombre_pais = destino_obj.get('nameCountry', 'Desconocido')
        print(f"✅ Destino encontrado: {destino_obj['name']} ({nombre_pais}).", flush=True)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, 
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            )
            
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )

            await context.route("**/*", self._block_heavy_resources)
            await self._procesar_destino_completo(context, nombre_pais, destino_obj)
            await browser.close()

    async def _block_heavy_resources(self, route):
        if route.request.resource_type in ["image", "media", "font", "stylesheet", "other"]:
            await route.abort()
        else:
            await route.continue_()

    async def _procesar_destino_completo(self, context, pais, destino_obj):
        page = await context.new_page()
        nombre_destino = destino_obj['name']
        slug_destino = destino_obj['url'] 
        
        url_destino_base = f"https://www.civitatis.com/es/{slug_destino}/"
        actividades = []

        print(f"\n🌍 Buscando actividades en la URL base...", flush=True)
        
        try:
            actividades = await self._get_activities_list(page, url_destino_base, slug_destino)
        except Exception as e:
            print(f"⚠️ Error listando {nombre_destino}: {e}", flush=True)
        finally:
            await page.close()

        if not actividades: return

        actividades_pendientes = [act for act in actividades if act['url'] not in self.actividades_completadas]
        
        print(f"   ↳ {len(actividades)} actividades en total detectadas.")
        print(f"   ↳ {len(actividades) - len(actividades_pendientes)} ya completadas previamente.")
        print(f"   ↳ {len(actividades_pendientes)} pendientes por procesar...", flush=True)

        tareas = []
        for act in actividades_pendientes:
            base_data = {
                "pais": pais, "destino": nombre_destino,
                "actividad": act['titulo'], "url_actividad": act['url']
            }
            tareas.append(self._scrape_reviews_safe(context, base_data))
        
        await asyncio.gather(*tareas)

    async def _get_activities_list(self, page, url_destino_base, slug_destino):
        actividades = []
        try:
            await page.goto(url_destino_base, wait_until="networkidle", timeout=90000)
            await page.evaluate("() => { document.querySelectorAll('.lottie-reveal-overlay, #lottie-modal, ._cookies-banner, #didomi-host').forEach(e => e.remove()); }")
            
            try:
                view_all = await page.query_selector(self.SELECTORS["view_all_btn"])
                if view_all and await view_all.is_visible():
                    await view_all.click()
                    await page.wait_for_load_state("networkidle")
            except: pass

            try:
                await page.wait_for_selector(self.SELECTORS["container"], timeout=15000)
            except:
                return []

            while True:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1.5) 
                
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
                                if full_url == url_destino_base: continue
                                if normalizar_texto(slug_destino) not in normalizar_texto(full_url): continue
                                    
                                actividades.append({"titulo": title, "url": full_url})
                    except: continue

                next_btn = await page.query_selector(self.SELECTORS["next_btn_list"])
                if next_btn and await next_btn.is_visible():
                    await page.evaluate("(el) => el.click()", next_btn)
                    await page.wait_for_load_state("networkidle")
                else: break
        except Exception as e: 
            print(f"     ❌ Error listando actividades: {e}", flush=True)
            
        actividades_unicas = []
        urls_vistas = set()
        for act in actividades:
            if act['url'] not in urls_vistas:
                actividades_unicas.append(act)
                urls_vistas.add(act['url'])

        return actividades_unicas

    async def _scrape_reviews_safe(self, context, base_data):
        async with self.semaphore:
            for intento in range(1, MAX_REINTENTOS + 1):
                try:
                    completado = await asyncio.wait_for(self._extract_reviews_logic(context, base_data), timeout=TIMEOUT_ACTIVIDAD)
                    if completado:
                        self._guardar_progreso(base_data['url_actividad']) 
                        break
                except asyncio.TimeoutError:
                    print(f"     ⏰ Timeout en {base_data['actividad']} (Intento {intento}/{MAX_REINTENTOS}).", flush=True)
                except Exception as e:
                    # Ignorar los errores crudos si se interrumpe y simplemente reintentar
                    pass 
                
                if intento < MAX_REINTENTOS:
                    await asyncio.sleep(3) 

    async def _extract_reviews_logic(self, context, base_data):
        page = await context.new_page()
        try:
            url = base_data['url_actividad']
            
            # Construir URL de opiniones de forma segura
            if "opiniones" not in url:
                url_opiniones = url if url.endswith('/') else url + '/'
                url_opiniones += "opiniones/"
            else:
                url_opiniones = url

            # Esperar a "load" garantiza que todas las redirecciones fuertes terminen
            response = await page.goto(url_opiniones, wait_until="load", timeout=60000)
            
            if response and response.status == 404:
                return True 
            
            # ESPERA VITAL: Da tiempo a los redirects basados en JS de Civitatis
            await page.wait_for_timeout(2500)

            # Validar si tras la espera seguimos en la sección de opiniones
            if "opiniones" not in page.url: 
                return True

            # Limpieza protegida. Si la página se mueve aquí, no matará el bot
            try:
                await page.evaluate("() => { document.querySelectorAll('.lottie-reveal-overlay, #lottie-modal, ._cookies-banner').forEach(e => e.remove()); }")
            except Exception:
                pass 

            paginas = 0
            alcanzo_limite_fecha = False

            while paginas < MAX_PAGINAS_REVIEWS and not alcanzo_limite_fecha:
                batch = []
                
                # Búsqueda protegida de contenedores
                try:
                    elements = await page.query_selector_all(self.SELECTORS["review_container"])
                except Exception:
                    break # Si el contexto muere leyendo, salimos limpiamente con lo que ya tenemos
                    
                if not elements: break

                for el in elements:
                    try:
                        date_el = await el.query_selector(self.SELECTORS["date_text"])
                        date_text = await date_el.inner_text() if date_el else ""
                        fecha_dt = parsear_fecha_civitatis(date_text)
                        
                        if fecha_dt:
                            if fecha_dt < self.fecha_corte:
                                alcanzo_limite_fecha = True
                                continue 
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
                
                if alcanzo_limite_fecha: break

                # Click de siguiente página protegido
                try:
                    next_btn = await page.query_selector(self.SELECTORS["next_btn_reviews"])
                    if next_btn and await next_btn.is_visible():
                        await page.evaluate("(el) => el.click()", next_btn)
                        await page.wait_for_timeout(1500) # Dejar que carguen las nuevas opiniones
                        paginas += 1
                    else: break
                except Exception:
                    break 
            
            return True 
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
        DESTINO = sys.argv[1]
    else:
        DESTINO = "Punta Cana, República Dominicana"

    scraper = CivitatisTurboScraper(DESTINO)
    asyncio.run(scraper.run())