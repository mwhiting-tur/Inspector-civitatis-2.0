import asyncio
import pandas as pd
import os
from datetime import datetime
from .base_driver import BaseScraper # Asegúrate de que este archivo exista en la carpeta drivers
from urllib.parse import urljoin

class CivitatisScraperSemanal(BaseScraper):
    SELECTORS = {
        "currency_nav": "#page-nav__currency",
        # Selector dinámico para la moneda
        "currency_option": ".o-page-nav__dropdown__body span[data-value='{code}']",
        "container": ".o-search-list__item",
        "city": ".comfort-card__near-city",
        "title": ".comfort-card__title",
        "price": ".comfort-card__price__text",
        "price_old": ".comfort-card__price__old-text",
        "rating": ".text--rating-total",
        "viajeros": "span._full",
        "next_btn": "a.next-element",
        "view_all_btn": "a.button-list-footer",
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button"
    }

    def __init__(self):
        super().__init__()
        self.seen_items = set()

    async def extract_list(self, lista_destinos, output_file, currency_code="CLP"):
        # Iniciamos browser (headless para servidor, false para debug visual local)
        await self.init_browser(headless=True) 
        page = await self.context.new_page()
        
        try:
            # 1. Configuración inicial de moneda
            # Navegamos al home para setear la cookie de moneda
            await page.goto("https://www.civitatis.com/es/", wait_until="domcontentloaded")
            await self._handle_overlays(page)
            await self._change_currency(page, currency_code)

            for destino in lista_destinos:
                url_destino = f"https://www.civitatis.com/es/{destino['url']}/"
                print(f"📍 Procesando Destino: {destino['name']} ({destino['nameCountry']})")
                
                try:
                    await page.goto(url_destino, wait_until="networkidle", timeout=60000)
                    await self._handle_overlays(page)

                    # Click en "Ver todas las actividades" si existe
                    view_all = await page.query_selector(self.SELECTORS["view_all_btn"])
                    if view_all and await view_all.is_visible():
                        print(f"➕ Botón 'Ver todas' detectado en {destino['name']}. Expandiendo...")
                        await page.evaluate("(el) => el.click()", view_all)
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(1)

                    while True:
                        await self._handle_overlays(page)
                        await self._scroll_to_bottom(page)
                        
                        try:
                            # Esperamos a que carguen las tarjetas
                            await page.wait_for_selector(self.SELECTORS["container"], timeout=8000)
                        except:
                            print(f"⚠️ Sin actividades en {destino['name']}")
                            break

                        items = await page.query_selector_all(self.SELECTORS["container"])
                        pagina_data = []
                        
                        for item in items:
                            actividad = await self.get_safe_text(item, self.SELECTORS["title"])
                            precio_real = await self.get_safe_text(item, self.SELECTORS["price"])
                            precio_desde_original = await self.get_safe_text(item, self.SELECTORS["price_old"])
                            
                            if actividad:
                                # Lógica para encontrar URL
                                link_element = await item.query_selector(".comfort-card__title a")
                                if not link_element:
                                    link_element = await item.query_selector("a:not([href='#'])")

                                url_actividad = None
                                if link_element:
                                    href = await link_element.get_attribute("href")
                                    if href:
                                        url_actividad = urljoin(page.url, href)

                                if not url_actividad:
                                    continue
                                
                                # Identificador único para evitar duplicados en la misma ejecución
                                identifier = f"{destino['name']}-{actividad}-{precio_real}".lower().strip()
                                
                                if identifier not in self.seen_items:
                                    viajeros = await self.get_safe_text(item, self.SELECTORS["viajeros"])
                                    
                                    pagina_data.append({
                                        "moneda": currency_code,
                                        "pais": destino['nameCountry'],
                                        "destino": destino['name'],
                                        "actividad": actividad,
                                        "precio_desde_original": precio_desde_original,
                                        "precio_real": precio_real,
                                        "opiniones": await self.get_safe_text(item, self.SELECTORS["rating"]),
                                        "viajeros": viajeros,
                                        "url_fuente": url_actividad
                                    })
                                    self.seen_items.add(identifier)

                        # Guardamos datos de esta página antes de pasar a la siguiente
                        if pagina_data:
                            self._save_incremental(pagina_data, output_file)

                        # Paginación
                        next_btn = await page.query_selector(self.SELECTORS["next_btn"])
                        if next_btn and await next_btn.is_visible():
                            await page.evaluate("(el) => el.click()", next_btn)
                            await asyncio.sleep(2)
                            await page.wait_for_load_state("networkidle")
                        else:
                            break
                            
                except Exception as e:
                    print(f"⚠️ Error procesando URL {url_destino}: {e}")
                    continue

        finally:
            await self.close_browser()

    def _save_incremental(self, data, filename):
        if not data: return
        df = pd.DataFrame(data)
        df['fuente'] = "Civitatis"
        df['fecha_scan'] = datetime.now().strftime("%Y-%m-%d %H:%M")
        
        # Verificar si existe para escribir header o no
        file_exists = os.path.isfile(filename)
        
        # Escribimos en modo 'append'
        df.to_csv(filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')

    async def _change_currency(self, page, currency_code):
        try:
            print(f"💱 Intentando cambiar moneda a: {currency_code}")
            await page.wait_for_selector(self.SELECTORS["currency_nav"], timeout=5000)
            await page.click(self.SELECTORS["currency_nav"])
            
            target = self.SELECTORS["currency_option"].format(code=currency_code)
            await page.wait_for_selector(target, state="visible")
            await page.click(target)
            await page.wait_for_load_state("networkidle")
            print("✅ Cambio de moneda exitoso")
        except Exception as e: 
            print(f"⚠️ No se pudo cambiar la moneda: {e}")

    async def _handle_overlays(self, page):
        try:
            # Eliminamos banners molestos con JS directo
            await page.evaluate('() => { document.querySelectorAll(".lottie-reveal-overlay, #lottie-modal, ._cookies-banner").forEach(el => el.remove()); }')
            
            cookie_btn = await page.query_selector(self.SELECTORS["cookie_btn"])
            if cookie_btn and await cookie_btn.is_visible():
                await cookie_btn.click(timeout=2000)
        except: pass

    async def _scroll_to_bottom(self, page):
        current_pos = 0
        while True:
            await page.evaluate("window.scrollBy(0, 1000)")
            await asyncio.sleep(0.5)
            new_pos = await page.evaluate("window.pageYOffset")
            if new_pos == current_pos: break
            current_pos = new_pos