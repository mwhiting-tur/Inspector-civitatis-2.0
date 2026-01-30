import asyncio
import pandas as pd
import os
from datetime import datetime
from .base_driver import BaseScraper

class NomadesScraper(BaseScraper):
    SELECTORS = {
        "container": "div.transition-all.rounded-md, div.rounded-lg",
        "nombre": "h3",
        "resenas": "span.text-gray-500.text-xs",
        "valor": ".w-fit > span:nth-of-type(2)",
        "moneda_label": ".w-fit > span.text-sm"
    }

    def __init__(self):
        super().__init__()
        self.seen_items = set()

    async def extract_list(self, lista_tareas, output_file):
        """lista_tareas: lista de dicts [{'pais': 'CHILE', 'url': '...'}]"""
        await self.init_browser(headless=False)
        page = await self.context.new_page()

        try:
            for tarea in lista_tareas:
                print(f"🚀 Procesando Nomades: {tarea['url']} ({tarea['pais']})")
                try:
                    await page.goto(tarea['url'], wait_until="networkidle", timeout=60000)
                    
                    # Nomades suele ser carga dinámica, aseguramos scroll
                    await self._scroll_to_bottom(page)
                    
                    try:
                        await page.wait_for_selector(self.SELECTORS["container"], timeout=10000)
                    except:
                        print(f"⚠️ No se detectaron elementos en {tarea['url']}")
                        continue

                    items = await page.query_selector_all(self.SELECTORS["container"])
                    pagina_data = []

                    for item in items:
                        nombre = await self.get_safe_text(item, self.SELECTORS["nombre"])
                        valor = await self.get_safe_text(item, self.SELECTORS["valor"])
                        
                        if nombre and valor:
                            # Huella única para no repetir en el CSV
                            identifier = f"{tarea['pais']}-{nombre}-{valor}".lower().strip()
                            
                            if identifier not in self.seen_items:
                                moneda = await self.get_safe_text(item, self.SELECTORS["moneda_label"])
                                resenas = await self.get_safe_text(item, self.SELECTORS["resenas"])
                                
                                pagina_data.append({
                                    "pais": tarea['pais'],
                                    "url_destino": tarea['url'],
                                    "actividad": nombre,
                                    "precio": valor,
                                    "moneda": moneda,
                                    "resenas": resenas,
                                    "fuente": "Nomades",
                                    "fecha_scan": datetime.now().strftime("%Y-%m-%d %H:%M")
                                })
                                self.seen_items.add(identifier)

                    if pagina_data:
                        self._save_incremental(pagina_data, output_file)
                        print(f"💾 {len(pagina_data)} tours guardados.")

                except Exception as e:
                    print(f"⚠️ Error en {tarea['url']}: {e}")
                    continue
        finally:
            await self.close_browser()

    def _save_incremental(self, data, filename):
        df = pd.DataFrame(data)
        file_exists = os.path.isfile(filename)
        df.to_csv(filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')

    async def _scroll_to_bottom(self, page):
        for _ in range(3): # Nomades no suele ser tan largo como Civitatis
            await page.evaluate("window.scrollBy(0, 1000)")
            await asyncio.sleep(0.5)