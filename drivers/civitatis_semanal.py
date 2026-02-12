import asyncio
import pandas as pd
import os
import re
from datetime import datetime
from .base_driver import BaseScraper 
from urllib.parse import urljoin

class CivitatisScraperSemanal(BaseScraper):
    SELECTORS = {
        "currency_nav": "#page-nav__currency",
        "currency_option": ".o-page-nav__dropdown__body span[data-value='{code}']",
        "container": ".o-search-list__item",
        "title": ".comfort-card__title",
        "price": ".comfort-card__price__text",
        "price_old": ".comfort-card__price__old-text",
        
        # Selector para la cantidad de opiniones (ej: "1.200 opiniones")
        "rating_opiniones": ".text--rating-total", 
        
        # NUEVO SELECTOR CORREGIDO para el puntaje (ej: "9,1 / 10")
        "rating_val": ".m-rating--text", 
        
        "viajeros": "span._full",
        "next_btn": "a.next-element",
        "view_all_btn": "a.button-list-footer",
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button"
    }

    def __init__(self):
        super().__init__()
        self.seen_items = set()

    def _clean_data(self, text, data_type='float'):
        """
        Limpia números generales (precios, viajeros, cantidad de opiniones).
        """
        if not text:
            return 0 if data_type == 'int' else 0.0
            
        # Eliminar todo lo que NO sea dígito o coma
        clean_text = re.sub(r'[^\d,]', '', text)
        clean_text = clean_text.replace(',', '.')
        
        try:
            val = float(clean_text)
            if data_type == 'int':
                return int(val)
            return val
        except ValueError:
            return 0 if data_type == 'int' else 0.0

    def _clean_rating(self, text):
        """
        Limpia específicamente el rating formato '9,1 / 10' o '9,1'
        """
        if not text:
            return 0.0
        
        # 1. Si viene con "/ 10" o "/", cortamos ahí y tomamos la primera parte
        if '/' in text:
            text = text.split('/')[0]
            
        # 2. Reemplazar coma por punto
        text = text.replace(',', '.')
        
        # 3. Limpiar espacios y extraer solo números y punto (seguridad extra)
        text = re.sub(r'[^\d.]', '', text)
        
        try:
            return float(text)
        except ValueError:
            return 0.0

    async def extract_list(self, lista_destinos, output_file, currency_code="CLP"):
        await self.init_browser(headless=True) 
        page = await self.context.new_page()
        
        try:
            await page.goto("https://www.civitatis.com/es/", wait_until="domcontentloaded")
            await self._handle_overlays(page)
            await self._change_currency(page, currency_code)

            for destino in lista_destinos:
                url_destino = f"https://www.civitatis.com/es/{destino['url']}/"
                print(f"📍 Procesando Destino: {destino['name']} ({destino['nameCountry']})")
                
                try:
                    await page.goto(url_destino, wait_until="networkidle", timeout=60000)
                    await self._handle_overlays(page)

                    view_all = await page.query_selector(self.SELECTORS["view_all_btn"])
                    if view_all and await view_all.is_visible():
                        await page.evaluate("(el) => el.click()", view_all)
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(1)

                    while True:
                        await self._handle_overlays(page)
                        await self._scroll_to_bottom(page)
                        
                        try:
                            await page.wait_for_selector(self.SELECTORS["container"], timeout=8000)
                        except:
                            break

                        items = await page.query_selector_all(self.SELECTORS["container"])
                        pagina_data = []
                        
                        for item in items:
                            actividad = await self.get_safe_text(item, self.SELECTORS["title"])
                            
                            # Obtención de URL
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

                            # Validación de URL (slug del destino debe estar en la url de la actividad)
                            if destino['url'] not in url_actividad:
                                continue
                            
                            precio_real_txt = await self.get_safe_text(item, self.SELECTORS["price"])
                            
                            identifier = f"{destino['name']}-{actividad}-{precio_real_txt}".lower().strip()
                            
                            if identifier not in self.seen_items:
                                precio_old_txt = await self.get_safe_text(item, self.SELECTORS["price_old"])
                                opiniones_txt = await self.get_safe_text(item, self.SELECTORS["rating_opiniones"])
                                viajeros_txt = await self.get_safe_text(item, self.SELECTORS["viajeros"])
                                # Extraer Rating (Puntaje)
                                rating_txt = await self.get_safe_text(item, self.SELECTORS["rating_val"])

                                pagina_data.append({
                                    "moneda": currency_code,
                                    "pais": destino['nameCountry'],
                                    "destino": destino['name'],
                                    "actividad": actividad,
                                    "url_fuente": url_actividad,
                                    "fuente": "Civitatis",
                                    "fecha_scan": datetime.now().strftime("%Y-%m-%d"),
                                    
                                    # Métricas limpias
                                    "precio_desde_original": self._clean_data(precio_old_txt, 'float'),
                                    "precio_real": self._clean_data(precio_real_txt, 'float'),
                                    "opiniones": self._clean_data(opiniones_txt, 'int'), # Cantidad de opiniones
                                    "viajeros": self._clean_data(viajeros_txt, 'int'),
                                    "rating": self._clean_rating(rating_txt) # Puntaje (ej: 9.1)
                                })
                                self.seen_items.add(identifier)

                        if pagina_data:
                            self._save_incremental(pagina_data, output_file)

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
        
        # Orden solicitado EXPLICITAMENTE
        cols_order = [
            'moneda', 
            'pais', 
            'destino', 
            'actividad', 
            'url_fuente', 
            'fuente', 
            'fecha_scan', 
            'precio_desde_original', 
            'precio_real', 
            'opiniones', 
            'viajeros',
            'rating' # Columna final solicitada
        ]
        
        # Reordenamos y rellenamos con 0 o vacío si falta alguna (seguridad)
        # Usamos reindex para forzar el orden y crear columnas si no existen
        df = df.reindex(columns=cols_order)

        file_exists = os.path.isfile(filename)
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
        except Exception: 
            pass 

    async def _handle_overlays(self, page):
        try:
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