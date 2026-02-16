import asyncio
import pandas as pd
import os
import re
import csv
from datetime import datetime
from urllib.parse import urljoin
from .base_driver import BaseScraper

class CivitatisScraper(BaseScraper):
    SELECTORS = {
        "currency_nav": "#page-nav__currency",
        "currency_option": ".o-page-nav__dropdown__body span[data-value='{code}']",
        "container": ".o-search-list__item",
        "title": ".comfort-card__title",
        "price": ".comfort-card__price__text",
        "rating_opiniones": ".text--rating-total", # Cantidad de opiniones
        "rating_val": ".m-rating--text",           # Puntaje Trustpilot
        "viajeros": "span._full",
        "next_btn": "a.next-element",
        "view_all_btn": "a.button-list-footer",
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button",
        
        # --- SELECTORES DESCRIPCIÓN Y DETALLE ---
        "full_description_container": "#descripcion",
        "view_more_trigger": "#view-more-trigger",
        "provider_link": "a.o-answers-provider__name",
        "contact_btn_sub": "a.o-answers-provider__link", 
        "info_lines": ".o-answers-provider__info"
    }

    def __init__(self):
        super().__init__()
        self.seen_items = set()

    def _clean_data(self, text, data_type='float'):
        if not text:
            return 0 if data_type == 'int' else 0.0
        clean_text = re.sub(r'[^\d,]', '', text)
        clean_text = clean_text.replace(',', '.')
        try:
            val = float(clean_text)
            if data_type == 'int': return int(val)
            return val
        except ValueError:
            return 0 if data_type == 'int' else 0.0

    def _clean_rating(self, text):
        if not text: return 0.0
        if '/' in text: text = text.split('/')[0]
        text = text.replace(',', '.')
        text = re.sub(r'[^\d.]', '', text)
        try: return float(text)
        except ValueError: return 0.0

    async def _block_heavy_resources(self, route):
        # Permite CSS para que los clics en dropdowns no fallen
        if route.request.resource_type in ["image", "media", "font", "other"]:
            await route.abort()
        else:
            await route.continue_()

    async def extract_list(self, lista_destinos, output_file, currency_code="USD"):
        await self.init_browser(headless=True) 
        page_lista = await self.context.new_page()
        
        # Habilitar bloqueo de recursos en la pestaña principal
        await page_lista.route("**/*", self._block_heavy_resources)
        
        try:
            await page_lista.goto("https://www.civitatis.com/es/", wait_until="domcontentloaded")
            await self._handle_overlays(page_lista)
            await self._change_currency(page_lista, currency_code)

            # Lógica de resumen (Checkpoint)
            destinos_completados = set()
            if os.path.exists(output_file):
                try:
                    df_check = pd.read_csv(output_file, sep=';', usecols=['destino'])
                    destinos_completados = set(df_check['destino'].unique())
                    print(f"🔄 Se encontraron {len(destinos_completados)} destinos ya listos en el archivo.")
                except: pass

            destinos_pendientes = [d for d in lista_destinos if d['name'] not in destinos_completados]
            print(f"📋 Destinos totales: {len(lista_destinos)} | Pendientes: {len(destinos_pendientes)}")

            for destino in destinos_pendientes:
                slug_destino = destino['url']
                url_destino = f"https://www.civitatis.com/es/{slug_destino}/"
                print(f"\n🌍 Procesando Destino: {destino['name']}")
                
                try:
                    await page_lista.goto(url_destino, wait_until="networkidle", timeout=60000)
                    await self._handle_overlays(page_lista)

                    # Expandir lista
                    try:
                        view_all = await page_lista.query_selector(self.SELECTORS["view_all_btn"])
                        if view_all and await view_all.is_visible():
                            await page_lista.evaluate("(el) => el.click()", view_all)
                            await page_lista.wait_for_load_state("networkidle")
                            await asyncio.sleep(1)
                    except: pass

                    # ESPERA CRÍTICA
                    try:
                        await page_lista.wait_for_selector(self.SELECTORS["container"], state="attached", timeout=15000)
                    except:
                        print(f"     ⚠️ No se detectaron actividades para {destino['name']}. Saltando...")
                        continue

                    while True:
                        await self._scroll_to_bottom(page_lista)
                        items = await page_lista.query_selector_all(self.SELECTORS["container"])
                        if not items: break

                        items_data_batch = []

                        for item in items:
                            try:
                                title = await self.get_safe_text(item, self.SELECTORS["title"])
                                
                                # Obtener y Validar URL
                                link_element = await item.query_selector(".comfort-card__title a")
                                if not link_element: link_element = await item.query_selector("a:not([href='#'])")

                                url_actividad = None
                                if link_element:
                                    href = await link_element.get_attribute("href")
                                    if href: url_actividad = urljoin(page_lista.url, href)

                                if not url_actividad: continue
                                if slug_destino.lower() not in url_actividad.lower(): continue
                                if url_actividad == url_destino: continue

                                identifier = f"{destino['name']}-{title}".lower()
                                
                                if identifier not in self.seen_items:
                                    self.seen_items.add(identifier)
                                    print(f"   ↳ Scrapeando Operadores: {title}")

                                    # Extracción de métricas
                                    precio_txt = await self.get_safe_text(item, self.SELECTORS["price"])
                                    viajeros_txt = await self.get_safe_text(item, self.SELECTORS["viajeros"])
                                    opiniones_txt = await self.get_safe_text(item, self.SELECTORS["rating_opiniones"])
                                    rating_txt = await self.get_safe_text(item, self.SELECTORS["rating_val"])

                                    # Visitar pestaña de detalle
                                    lista_operadores, descripcion_full = await self._scrape_details_in_new_tab(url_actividad)

                                    # Crear registros
                                    for op_data in lista_operadores:
                                        row = {
                                            "pais": destino['nameCountry'],
                                            "destino": destino['name'],
                                            "actividad": title,
                                            "url_actividad": url_actividad,
                                            "operador": op_data["operador"],
                                            "email": op_data["email"],
                                            "telefono": op_data["telefono"],
                                            "direccion": op_data["direccion"],
                                            # Datos limpios unificados
                                            "precio_real": self._clean_data(precio_txt, 'float'),
                                            "opiniones": self._clean_data(opiniones_txt, 'int'),
                                            "viajeros": self._clean_data(viajeros_txt, 'int'),
                                            "rating": self._clean_rating(rating_txt),
                                            "moneda": currency_code,
                                            "fecha_scan": datetime.now().strftime("%Y-%m-%d")
                                        }
                                        items_data_batch.append(row)

                            except Exception as e:
                                continue
                        
                        # Guardar lote
                        if items_data_batch:
                            self._save_incremental(items_data_batch, output_file)

                        # Paginación
                        next_btn = await page_lista.query_selector(self.SELECTORS["next_btn"])
                        if next_btn and await next_btn.is_visible():
                            await page_lista.evaluate("(el) => el.click()", next_btn)
                            await page_lista.wait_for_load_state("networkidle")
                            await asyncio.sleep(1)
                        else:
                            break

                except Exception as e:
                    print(f"❌ Error en destino {destino['name']}: {e}")
                    continue

        finally:
            await self.close_browser()

    async def _scrape_details_in_new_tab(self, url):
        detail_page = await self.context.new_page()
        lista_operadores = [] 
        description_text = "N/A"
        
        try:
            # Reutilizamos el bloqueador seguro para la pestaña
            await detail_page.route("**/*", self._block_heavy_resources)
            await detail_page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # --- DESCRIPCIÓN ---
            try:
                await detail_page.evaluate(f"""() => {{
                    const btn = document.querySelector('{self.SELECTORS["view_more_trigger"]}');
                    if (btn) btn.remove();
                }}""")

                desc_el = await detail_page.query_selector(self.SELECTORS["full_description_container"])
                if desc_el:
                    raw_text = await desc_el.inner_text()
                    cleaned_text = raw_text.replace("\n", " || ").replace("\r", "")
                    description_text = " ".join(cleaned_text.split())
            except: pass

            # --- OPERADORES ---
            provider_links = await detail_page.query_selector_all(self.SELECTORS["provider_link"])
            
            if not provider_links:
                lista_operadores.append({
                    "operador": "No especificado / Único",
                    "email": "N/A", "telefono": "N/A", "direccion": "N/A"
                })
            else:
                for link in provider_links:
                    datos = {"operador": "N/A", "email": "N/A", "telefono": "N/A", "direccion": "N/A"}
                    try:
                        nombre = await link.inner_text()
                        datos["operador"] = nombre.strip()
                        target_id = await link.get_attribute("data-dropdow-target")
                        
                        if target_id:
                            if await link.is_visible():
                                await link.click()
                                await asyncio.sleep(0.5) # Pausa ligera para animación JS
                            
                            container = await detail_page.query_selector(f"#{target_id}")
                            if container:
                                contact_btn = await container.query_selector("a:has-text('Información de contacto')")
                                if contact_btn and await contact_btn.is_visible():
                                    await contact_btn.click()
                                    await asyncio.sleep(0.5)
                                
                                info_lines = await container.query_selector_all(self.SELECTORS["info_lines"])
                                for linea in info_lines:
                                    txt = (await linea.inner_text()).strip()
                                    low = txt.lower()
                                    if "correo electrónico:" in low: datos["email"] = txt.split(":", 1)[1].strip()
                                    elif "teléfono:" in low: datos["telefono"] = txt.split(":", 1)[1].strip()
                                    elif "domicilio" in low or "razón social" in low:
                                        info_limpia = txt.replace("Domicilio Social:", "").replace("Razón social:", "").strip()
                                        datos["direccion"] = info_limpia if datos["direccion"] == "N/A" else f'{datos["direccion"]} | {info_limpia}'
                        
                        lista_operadores.append(datos)
                    except: continue

        except Exception as e:
            pass
        finally:
            await detail_page.close()
            
        return lista_operadores, description_text 

    def _save_incremental(self, data, filename):
        if not data: return
        df = pd.DataFrame(data)
        
        cols_order = [
            'pais', 'destino', 'actividad', 'url_actividad', 'operador', 
            'email', 'telefono', 'direccion', 'descripcion', 
            'precio_real', 'opiniones', 'viajeros', 'rating', 
            'moneda', 'fecha_scan'
        ]
        df = df.reindex(columns=cols_order)
        
        file_exists = os.path.isfile(filename)
        df.to_csv(
            filename, 
            mode='a', 
            index=False, 
            header=not file_exists, 
            encoding='utf-8-sig', 
            sep=';',           
            quoting=csv.QUOTE_ALL 
        )

    async def _change_currency(self, page, currency_code):
        try:
            await page.wait_for_selector(self.SELECTORS["currency_nav"], timeout=5000)
            await page.click(self.SELECTORS["currency_nav"])
            target = self.SELECTORS["currency_option"].format(code=currency_code)
            await page.click(target)
            await page.wait_for_load_state("networkidle")
        except: pass

    async def _handle_overlays(self, page):
        try:
            await page.evaluate('() => { document.querySelectorAll(".lottie-reveal-overlay, #lottie-modal, ._cookies-banner").forEach(el => el.remove()); }')
            cookie = await page.query_selector(self.SELECTORS["cookie_btn"])
            if cookie and await cookie.is_visible(): await cookie.click()
        except: pass

    async def _scroll_to_bottom(self, page):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)