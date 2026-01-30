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
        "link": "a", # Selector genérico para encontrar el link dentro de la tarjeta
        "next_btn": "a.next-element",
        "view_all_btn": "a.button-list-footer",
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button",
        
        # --- SELECTORES PARA DETALLE MULTI-OPERADOR ---
        # El enlace que tiene el nombre del operador
        "provider_link": "a.o-answers-provider__name",
        # El selector para encontrar el botón de contacto DENTRO del contenedor del operador
        "contact_btn_sub": "a.o-answers-provider__link", 
        # Las líneas de texto dentro del contenedor
        "info_lines": ".o-answers-provider__info"
    }

    def __init__(self):
        super().__init__()
        self.seen_items = set()

    async def extract_list(self, lista_destinos, output_file, currency_code="CLP"):
        await self.init_browser(headless=True) 
        # Esta es tu página PRINCIPAL (donde vive la lista)
        page_lista = await self.context.new_page()
        
        try:
            # 1. Configuración inicial
            await page_lista.goto("https://www.civitatis.com/es/", wait_until="domcontentloaded")
            await self._handle_overlays(page_lista)
            await self._change_currency(page_lista, currency_code)

            # 2. Bucle de Destinos
            for destino in lista_destinos:
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

                    # 3. Bucle de Paginación (Next page)
                    while True:
                        await self._scroll_to_bottom(page_lista)
                        
                        # Capturamos todas las tarjetas de la página actual
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
                                
                                # --- CORRECCIÓN DE URL ---
                                # 1. Buscamos el enlace 'a' dentro del título (es el más fiable)
                                link_element = await item.query_selector(".comfort-card__title a")
                                
                                # 2. Si no está en el título, buscamos cualquier 'a' en la tarjeta que no sea vacío
                                if not link_element:
                                    link_element = await item.query_selector("a:not([href='#'])")

                                url_actividad = None
                                if link_element:
                                    href = await link_element.get_attribute("href")
                                    if href:
                                        # urljoin maneja la magia:
                                        # Si href es "/es/roma/tour", lo pega al dominio.
                                        # Si href es "tour-coliseo/", lo pega a la carpeta actual.
                                        url_actividad = urljoin(page_lista.url, href)

                                # Si falló todo, saltamos (no inventamos URLs porque fallan)
                                if not url_actividad:
                                    print(f"⚠️ No se encontró URL para: {title}")
                                    continue

                                identifier = f"{destino['name']}-{title}".lower()
                                
                                if identifier not in self.seen_items:
                                    self.seen_items.add(identifier)
                                    print(f"   ↳ Scrapeando: {title}")

                                    viajeros = await self.get_safe_text(item, self.SELECTORS["viajeros"])
                                    rating = await self.get_safe_text(item, self.SELECTORS["rating"])

                                    # --- CAMBIO PRINCIPAL AQUÍ ---
                                    # Obtenemos la LISTA de operadores
                                    lista_operadores_encontrados = await self._scrape_details_in_new_tab(url_actividad)

                                    # Creamos UNA FILA por cada operador encontrado
                                    # Si la actividad tiene 2 operadores, se guardarán 2 filas en el CSV
                                    # con la misma información de actividad pero distinto operador.
                                    for op_data in lista_operadores_encontrados:
                                        row = {
                                            "destino": destino['name'],
                                            "pais": destino['nameCountry'],
                                            "actividad": title,
                                            "precio": price,
                                            "moneda": currency_code,
                                            "rating": rating,
                                            "viajeros": viajeros,
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
                        
                        # Guardamos el lote de esta página
                        if items_data_batch:
                            self._save_incremental(items_data_batch, output_file)

                        # Intentar ir a la siguiente página de la lista
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
        Entra a la actividad, busca TODOS los operadores listados y extrae su info.
        Retorna una lista de diccionarios.
        """
        detail_page = await self.context.new_page()
        lista_operadores = [] # Aquí guardaremos todos los encontrados
        
        try:
            # Bloqueamos recursos multimedia para velocidad
            await detail_page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2}", lambda route: route.abort())
            await detail_page.goto(url, wait_until="domcontentloaded", timeout=20000)
            
            # 1. Encontrar todos los enlaces de nombres de operadores
            provider_links = await detail_page.query_selector_all(self.SELECTORS["provider_link"])
            
            if not provider_links:
                # Si no hay lista explicita, intentamos buscar el texto genérico (fallback)
                # ... lógica de fallback o retornar un operador vacío ...
                lista_operadores.append({
                    "operador": "No especificado / Único",
                    "email": "N/A", "telefono": "N/A", "direccion": "N/A"
                })
            else:
                # 2. Iterar sobre cada operador encontrado
                for link in provider_links:
                    datos = {"operador": "N/A", "email": "N/A", "telefono": "N/A", "direccion": "N/A"}
                    
                    try:
                        # Extraer nombre
                        nombre = await link.inner_text()
                        datos["operador"] = nombre.strip()
                        
                        # Obtener el ID del contenedor de detalles (data-dropdow-target)
                        target_id = await link.get_attribute("data-dropdow-target")
                        
                        if target_id:
                            # Hacemos clic en el nombre para desplegar (por si acaso no está visible)
                            if await link.is_visible():
                                await link.click()
                                await asyncio.sleep(0.3) # Pequeña pausa UI
                            
                            # Seleccionamos el contenedor específico usando el ID
                            container_selector = f"#{target_id}"
                            container = await detail_page.query_selector(container_selector)
                            
                            if container:
                                # Buscar botón de contacto DENTRO de este contenedor
                                contact_btn = await container.query_selector("a:has-text('Información de contacto')")
                                
                                if contact_btn and await contact_btn.is_visible():
                                    await contact_btn.click()
                                    await asyncio.sleep(0.3)
                                
                                # Extraer líneas de información SOLO de este contenedor
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
                        
                        # Guardamos este operador en la lista
                        lista_operadores.append(datos)
                        
                    except Exception as e:
                        print(f"⚠️ Error extrayendo un operador: {e}")
                        continue

        except Exception as e:
            print(f"⚠️ Error general en detalle {url}: {e}")
            
        finally:
            await detail_page.close()
            
        return lista_operadores

    def _save_incremental(self, data, filename):
        if not data: return
        df = pd.DataFrame(data)
        file_exists = os.path.isfile(filename)
        # Modo 'a' (append) agrega al final sin borrar lo anterior
        df.to_csv(filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')

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