import asyncio
import pandas as pd
import os
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
        "rating": ".text--rating-total",
        "viajeros": "span._full",
        "link": "a", 
        "next_btn": "a.next-element",
        "view_all_btn": "a.button-list-footer",
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button",
        
        # --- SELECTORES NUEVOS PARA DESCRIPCIÓN ---
        "full_description_container": "#descripcion", # Contenedor principal de la descripción
        "view_more_trigger": "#view-more-trigger",    # Botón "Ver más" (si existe)

        # --- SELECTORES PARA DETALLE MULTI-OPERADOR ---
        "provider_link": "a.o-answers-provider__name",
        "contact_btn_sub": "a.o-answers-provider__link", 
        "info_lines": ".o-answers-provider__info"
    }

    def __init__(self):
        super().__init__()
        self.seen_items = set()

    async def extract_list(self, lista_destinos, output_file, currency_code="COP"):
        await self.init_browser(headless=True) 
        page_lista = await self.context.new_page()
        
        try:
            # 1. Configuración inicial
            await page_lista.goto("https://www.civitatis.com/es/", wait_until="domcontentloaded")
            await self._handle_overlays(page_lista)
            await self._change_currency(page_lista, currency_code)

            # --- NUEVA LÓGICA: DETECTAR LO YA PROCESADO ---
            destinos_completados = set()
            if os.path.exists(output_file):
                try:
                    # Leemos solo la columna 'destino' para saber qué ciudades ya están en el CSV
                    # IMPORTANTE: Asegúrate de que 'sep' sea el mismo que usas al guardar (';' o ',')
                    df_check = pd.read_csv(output_file, sep=';', usecols=['destino'])
                    
                    # Convertimos a un SET para búsqueda rápida
                    destinos_completados = set(df_check['destino'].unique())
                    print(f"🔄 RESUMIENDO: Se encontraron {len(destinos_completados)} destinos ya listos en el archivo.")
                except Exception as e:
                    print(f"⚠️ No se pudo leer el archivo para resumir (se empezará de cero): {e}")

            # Filtramos la lista original para dejar solo lo que FALTA
            # Comparamos d['name'] (del input) con lo que hay en el CSV
            destinos_pendientes = [d for d in lista_destinos if d['name'] not in destinos_completados]
            
            print(f"📋 Destinos totales: {len(lista_destinos)} | Pendientes: {len(destinos_pendientes)}")
            # -----------------------------------------------

            # 2. Bucle de Destinos
            for destino in destinos_pendientes:
                url_destino = f"https://www.civitatis.com/es/{destino['url']}/"
                print(f"🌍 Procesando Destino: {destino['name']}")
                
                try:
                    await page_lista.goto(url_destino, wait_until="networkidle", timeout=60000)
                    await self._handle_overlays(page_lista)

                    # Expandir lista si es necesario
                    view_all = await page_lista.query_selector(self.SELECTORS["view_all_btn"])
                    if view_all and await view_all.is_visible():
                        await page_lista.evaluate("(el) => el.click()", view_all)
                        await page_lista.wait_for_load_state("networkidle")
                        await asyncio.sleep(1)

                    # 3. Bucle de Paginación
                    while True:
                        await self._scroll_to_bottom(page_lista)
                        
                        items = await page_lista.query_selector_all(self.SELECTORS["container"])
                        if not items:
                            print("⚠️ No se encontraron actividades.")
                            break

                        items_data_batch = []

                        # 4. Bucle de Actividades
                        for item in items:
                            try:
                                title = await self.get_safe_text(item, self.SELECTORS["title"])
                                price = await self.get_safe_text(item, self.SELECTORS["price"])
                                # Ya no sacamos descripción aquí, la sacaremos del detalle
                                
                                # --- CORRECCIÓN DE URL ---
                                link_element = await item.query_selector(".comfort-card__title a")
                                if not link_element:
                                    link_element = await item.query_selector("a:not([href='#'])")

                                url_actividad = None
                                if link_element:
                                    href = await link_element.get_attribute("href")
                                    if href:
                                        url_actividad = urljoin(page_lista.url, href)

                                if not url_actividad:
                                    continue

                                identifier = f"{destino['name']}-{title}".lower()
                                
                                if identifier not in self.seen_items:
                                    self.seen_items.add(identifier)
                                    print(f"   ↳ Scrapeando: {title}")

                                    viajeros = await self.get_safe_text(item, self.SELECTORS["viajeros"])
                                    rating = await self.get_safe_text(item, self.SELECTORS["rating"])

                                    # --- CAMBIO PRINCIPAL: OBTENER TUPLA (OPERADORES, DESCRIPCIÓN) ---
                                    lista_operadores_encontrados, descripcion_full = await self._scrape_details_in_new_tab(url_actividad)

                                    # Creamos una fila por cada operador, usando la misma descripción detallada
                                    for op_data in lista_operadores_encontrados:
                                        row = {
                                            "destino": destino['name'],
                                            "pais": destino['nameCountry'],
                                            "actividad": title,
                                            "precio": price,
                                            "moneda": currency_code,
                                            "rating": rating,
                                            "viajeros": viajeros,
                                            "descripcion": descripcion_full, # <--- AQUI VA LA DESCRIPCION DETALLADA
                                            # Datos variables del operador
                                            "operador": op_data["operador"],
                                            "email": op_data["email"],
                                            "telefono": op_data["telefono"],
                                            "direccion": op_data["direccion"],
                                            # Metadatos
                                            "url_actividad": url_actividad,
                                            "fecha_scan": datetime.now().strftime("%Y-%m-%d")
                                        }
                                        items_data_batch.append(row)

                            except Exception as e:
                                print(f"⚠️ Error procesando item individual: {e}")
                                continue
                        
                        # Guardamos el lote
                        if items_data_batch:
                            self._save_incremental(items_data_batch, output_file)

                        # Siguiente página
                        next_btn = await page_lista.query_selector(self.SELECTORS["next_btn"])
                        if next_btn and await next_btn.is_visible():
                            print("➡️ Pasando a siguiente página...")
                            await page_lista.evaluate("(el) => el.click()", next_btn)
                            await page_lista.wait_for_load_state("networkidle")
                            await asyncio.sleep(2)
                        else:
                            print(f"✅ Fin de lista para {destino['name']}")
                            break

                except Exception as e:
                    print(f"❌ Error crítico en destino {destino['name']}: {e}")
                    continue

        finally:
            await self.close_browser()

    async def _scrape_details_in_new_tab(self, url):
        """
        Entra a la actividad.
        Retorna:
            1. Una lista de diccionarios (operadores).
            2. Un string con la descripción completa.
        """
        detail_page = await self.context.new_page()
        lista_operadores = [] 
        description_text = "N/A" # Valor por defecto
        
        try:
            # Bloqueamos recursos multimedia (imágenes, fuentes, css) para velocidad extrema
            await detail_page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2}", lambda route: route.abort())
            await detail_page.goto(url, wait_until="domcontentloaded", timeout=20000)
            
            # --- 1. EXTRACCIÓN DE LA DESCRIPCIÓN ---
            try:
                # Borramos botón "Ver más"
                await detail_page.evaluate(f"""() => {{
                    const btn = document.querySelector('{self.SELECTORS["view_more_trigger"]}');
                    if (btn) btn.remove();
                }}""")

                # Seleccionamos el contenedor
                desc_el = await detail_page.query_selector(self.SELECTORS["full_description_container"])
                if desc_el:
                    # Obtenemos el texto
                    raw_text = await desc_el.inner_text()
                    
                    # --- LIMPIEZA PROFUNDA ---
                    # 1. Reemplazamos los saltos de línea por un separador visual (ej: " || ")
                    cleaned_text = raw_text.replace("\n", " || ").replace("\r", "")
                    
                    # 2. Reemplazamos las comas del texto por puntos (opcional, para seguridad extra)
                    # cleaned_text = cleaned_text.replace(",", ".") 
                    
                    # 3. Quitamos espacios dobles
                    description_text = " ".join(cleaned_text.split())
                    
            except Exception as e_desc:
                print(f"⚠️ No se pudo extraer descripción: {e_desc}")

            # --- 2. EXTRACCIÓN DE OPERADORES (Tu lógica original) ---
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
                            # Click JS forzado por si el elemento visual está tapado
                            if await link.is_visible():
                                await link.click()
                                await asyncio.sleep(0.3)
                            
                            container_selector = f"#{target_id}"
                            container = await detail_page.query_selector(container_selector)
                            
                            if container:
                                contact_btn = await container.query_selector("a:has-text('Información de contacto')")
                                if contact_btn and await contact_btn.is_visible():
                                    await contact_btn.click()
                                    await asyncio.sleep(0.3)
                                
                                info_lines = await container.query_selector_all(self.SELECTORS["info_lines"])
                                for linea in info_lines:
                                    txt = (await linea.inner_text()).strip()
                                    low = txt.lower()
                                    
                                    if "correo electrónico:" in low:
                                        datos["email"] = txt.split(":", 1)[1].strip()
                                    elif "teléfono:" in low:
                                        datos["telefono"] = txt.split(":", 1)[1].strip()
                                    elif "domicilio" in low or "razón social" in low:
                                        info_limpia = txt.replace("Domicilio Social:", "").replace("Razón social:", "").strip()
                                        if datos["direccion"] == "N/A":
                                            datos["direccion"] = info_limpia
                                        else:
                                            datos["direccion"] += f" | {info_limpia}"
                        
                        lista_operadores.append(datos)
                    except Exception as e:
                        print(f"⚠️ Error extrayendo un operador: {e}")
                        continue

        except Exception as e:
            print(f"⚠️ Error general en detalle {url}: {e}")
            
        finally:
            await detail_page.close()
            
        return lista_operadores, description_text # <--- RETORNA LA TUPLA AHORA

    def _save_incremental(self, data, filename):
        if not data: return
        df = pd.DataFrame(data)
        file_exists = os.path.isfile(filename)
        
        # CAMBIOS AQUÍ:
        # 1. sep=';' -> Usa punto y coma como separador (mejor para español)
        # 2. quoting=1 -> (csv.QUOTE_ALL) Pone comillas a TODO para proteger el texto
        # 3. escapechar='\\' -> Escapa caracteres raros
        
        import csv # Asegúrate de importar csv arriba si no lo has hecho
        
        df.to_csv(
            filename, 
            mode='a', 
            index=False, 
            header=not file_exists, 
            encoding='utf-8-sig', 
            sep=';',           # <--- CAMBIO CLAVE
            quoting=csv.QUOTE_ALL # <--- FUERZA COMILLAS EN TODO
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