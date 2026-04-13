import asyncio
import pandas as pd
import os
import re
import csv
from datetime import datetime
from urllib.parse import urljoin
from .base_driver import BaseScraper

class CivitatisCutoffScraper(BaseScraper):
    SELECTORS = {
        "currency_nav": "#page-nav__currency",
        "currency_option": ".o-page-nav__dropdown__body span[data-value='{code}']",
        "container": ".o-search-list__item",
        "title": ".comfort-card__title",
        "price": ".comfort-card__price__text",
        "rating_opiniones": ".text--rating-total",
        "rating_val": ".m-rating--text",
        "viajeros": "span._full",
        "next_btn": "a.next-element",
        "view_all_btn": "a.button-list-footer",
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button",
        "cutoff": ".m-activity-detail--advance b"
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
        if route.request.resource_type in ["image", "media", "font", "other"]:
            await route.abort()
        else:
            await route.continue_()

    async def _get_cutoff(self, url):
        detail_page = await self.context.new_page()
        try:
            await detail_page.route("**/*", self._block_heavy_resources)
            await detail_page.goto(url, wait_until="domcontentloaded", timeout=30000)
            el = await detail_page.query_selector(self.SELECTORS["cutoff"])
            if el:
                match = re.search(r'(\d+)', await el.inner_text())
                if match:
                    return int(match.group(1))
            return None
        except:
            return None
        finally:
            await detail_page.close()

    async def extract_list(self, lista_destinos, output_file, currency_code="USD"):
        await self.init_browser(headless=True)
        page_lista = await self.context.new_page()
        await page_lista.route("**/*", self._block_heavy_resources)

        try:
            await page_lista.goto("https://www.civitatis.com/es/", wait_until="domcontentloaded")
            await self._handle_overlays(page_lista)
            await self._change_currency(page_lista, currency_code)

            # Checkpoint: skip already-completed destinations
            destinos_completados = set()
            if os.path.exists(output_file):
                try:
                    df_check = pd.read_csv(output_file, sep=';', usecols=['destino'])
                    destinos_completados = set(df_check['destino'].unique())
                    print(f"🔄 {len(destinos_completados)} destinos ya completados.")
                except: pass

            destinos_pendientes = [d for d in lista_destinos if d['name'] not in destinos_completados]
            print(f"📋 Totales: {len(lista_destinos)} | Pendientes: {len(destinos_pendientes)}")

            for destino in destinos_pendientes:
                slug_destino = destino['url']
                url_destino = f"https://www.civitatis.com/es/{slug_destino}/"
                print(f"\n🌍 Procesando: {destino['name']}")

                try:
                    await page_lista.goto(url_destino, wait_until="networkidle", timeout=60000)
                    await self._handle_overlays(page_lista)

                    try:
                        view_all = await page_lista.query_selector(self.SELECTORS["view_all_btn"])
                        if view_all and await view_all.is_visible():
                            await page_lista.evaluate("(el) => el.click()", view_all)
                            await page_lista.wait_for_load_state("networkidle")
                            await asyncio.sleep(1)
                    except: pass

                    try:
                        await page_lista.wait_for_selector(self.SELECTORS["container"], state="attached", timeout=15000)
                    except:
                        print(f"     ⚠️ Sin actividades para {destino['name']}. Saltando...")
                        continue

                    while True:
                        await self._scroll_to_bottom(page_lista)
                        items = await page_lista.query_selector_all(self.SELECTORS["container"])
                        if not items: break

                        batch = []

                        for item in items:
                            try:
                                title = await self.get_safe_text(item, self.SELECTORS["title"])

                                link_element = await item.query_selector(".comfort-card__title a")
                                if not link_element:
                                    link_element = await item.query_selector("a:not([href='#'])")

                                url_actividad = None
                                if link_element:
                                    href = await link_element.get_attribute("href")
                                    if href:
                                        url_actividad = urljoin(page_lista.url, href)

                                if not url_actividad: continue
                                if slug_destino.lower() not in url_actividad.lower(): continue
                                if url_actividad == url_destino: continue

                                identifier = f"{destino['name']}-{title}".lower()
                                if identifier in self.seen_items: continue
                                self.seen_items.add(identifier)

                                precio_txt = await self.get_safe_text(item, self.SELECTORS["price"])
                                opiniones_txt = await self.get_safe_text(item, self.SELECTORS["rating_opiniones"])
                                viajeros_txt = await self.get_safe_text(item, self.SELECTORS["viajeros"])
                                rating_txt = await self.get_safe_text(item, self.SELECTORS["rating_val"])

                                print(f"   ↳ {title}")
                                cutoff = await self._get_cutoff(url_actividad)

                                batch.append({
                                    "pais": destino['nameCountry'],
                                    "destino": destino['name'],
                                    "actividad": title,
                                    "url_actividad": url_actividad,
                                    "precio_real": self._clean_data(precio_txt, 'float'),
                                    "opiniones": self._clean_data(opiniones_txt, 'int'),
                                    "viajeros": self._clean_data(viajeros_txt, 'int'),
                                    "rating": self._clean_rating(rating_txt),
                                    "moneda": currency_code,
                                    "fecha_scan": datetime.now().strftime("%Y-%m-%d"),
                                    "cutoff": cutoff
                                })

                            except: continue

                        if batch:
                            self._save_incremental(batch, output_file)

                        next_btn = await page_lista.query_selector(self.SELECTORS["next_btn"])
                        if next_btn and await next_btn.is_visible():
                            await page_lista.evaluate("(el) => el.click()", next_btn)
                            await page_lista.wait_for_load_state("networkidle")
                            await asyncio.sleep(1)
                        else:
                            break

                except Exception as e:
                    print(f"❌ Error en {destino['name']}: {e}")
                    continue

        finally:
            await self.close_browser()

    def _save_incremental(self, data, filename):
        if not data: return
        df = pd.DataFrame(data)
        cols_order = [
            'pais', 'destino', 'actividad', 'url_actividad',
            'precio_real', 'opiniones', 'viajeros', 'rating',
            'moneda', 'fecha_scan', 'cutoff'
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
