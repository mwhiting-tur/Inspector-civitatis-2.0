"""
Driver de operadores de Civitatis preparado para recorrer el sitio completo.

Diferencias con drivers/civitatis_operadores.py (que se mantiene intacto):
  - Lanza Chromium descargado por Playwright (sin channel="chrome"), que es lo
    que existe en los runners de GitHub Actions.
  - Procesa varios destinos en paralelo y, dentro de cada destino, varias
    fichas de actividad en paralelo.
  - No escribe CSV: entrega las filas al orquestador por callback para que
    éste las suba a BigQuery en lotes incrementales.
  - Soporta deadline (para cortar antes del timeout de GitHub Actions) y una
    lista de urls ya scrapeadas (para reanudar).
"""

import asyncio
import json
import os
import re
import time
import uuid
from collections import Counter
from urllib.parse import urljoin

from playwright.async_api import async_playwright


def proxy_apify(session_id=None):
    """
    Config de proxy para Playwright usando Apify Proxy (mismo esquema que ya
    usa gyg/gyg_precios_github.py). Devuelve None si no hay secret cargado.

    Civitatis limita por IP de forma agresiva (429 y luego 406). Las IPs de los
    runners de GitHub Actions son de datacenter y se bloquean rápido, así que
    para el recorrido completo conviene salir por proxy residencial.
    """
    password = os.environ.get("APIFY_PROXY_PASSWORD")
    if not password:
        return None
    groups = os.environ.get("APIFY_PROXY_GROUPS", "RESIDENTIAL")
    pais = os.environ.get("APIFY_PROXY_COUNTRY")
    session_id = session_id or uuid.uuid4().hex[:16]
    usuario = f"groups-{groups},session-{session_id}"
    if pais:
        usuario += f",country-{pais}"
    return {
        "server": os.environ.get("APIFY_PROXY_SERVER", "http://proxy.apify.com:8000"),
        "username": usuario,
        "password": password,
    }

COLUMNAS = [
    "pais", "destino", "actividad", "url_actividad", "operador",
    "email", "telefono", "direccion", "descripcion",
    "precio_real", "opiniones", "viajeros", "rating",
    "moneda", "fecha_scan",
]

# datatur.supply.operators_civitatis no tiene un solo NULL en sus 20.832 filas:
# los datos de operador ausentes se guardan como 'N/A' y la descripción vacía
# como ''. Se respeta esa convención para no mezclar formatos en la tabla.
SIN_DATO = "N/A"
OPERADOR_GENERICO = "No especificado / Único"


class DeadlineAlcanzado(Exception):
    """Se agotó el tiempo máximo de ejecución asignado al scraper."""


# ---------------------------------------------------------------------- #
# Extracción de operadores del payload de Next.js
#
# Civitatis está migrando las fichas a Next.js. En esa variante el bloque de
# operadores ya NO se renderiza en el HTML (ni con scroll ni expandiendo el
# FAQ): viaja como JSON dentro del payload de hidratación, bajo la clave
# "providers". Conviene además porque trae los campos ya separados.
# La variante vieja (server-rendered, con a.o-answers-provider__name) sigue
# viva en parte del sitio, así que se soportan las dos.
# ---------------------------------------------------------------------- #

RE_PROVIDERS = re.compile(r'\\?"providers\\?"\s*:\s*\[')


def _desescapar(s):
    """Deshace el escapado de string de JS para recuperar el JSON original."""
    out, i, n = [], 0, len(s)
    while i < n:
        c = s[i]
        if c == "\\" and i + 1 < n:
            sig = s[i + 1]
            if sig == "\\":
                out.append("\\"); i += 2; continue
            if sig == '"':
                out.append('"'); i += 2; continue
            if sig == "n":
                out.append("\n"); i += 2; continue
            if sig == "t":
                out.append("\t"); i += 2; continue
            if sig == "u" and i + 6 <= n:
                try:
                    out.append(chr(int(s[i + 2:i + 6], 16))); i += 6; continue
                except ValueError:
                    pass
        out.append(c); i += 1
    return "".join(out)


def _recortar_array(texto, inicio):
    """Devuelve el array JSON completo que empieza en `inicio`, balanceando []."""
    prof, en_str, i, n = 0, False, inicio, len(texto)
    while i < n:
        c = texto[i]
        if c == "\\":
            i += 2
            continue
        if c == '"':
            en_str = not en_str
            i += 1
            continue
        if not en_str:
            if c == "[":
                prof += 1
            elif c == "]":
                prof -= 1
                if prof == 0:
                    return texto[inicio:i + 1]
        i += 1
    return None


def extraer_providers_json(html):
    """Lista de dicts de operadores embebidos en el HTML, o [] si no hay."""
    for m in RE_PROVIDERS.finditer(html):
        inicio = html.find("[", m.end() - 1)
        if inicio == -1:
            continue
        frag = _recortar_array(html, inicio)
        if not frag:
            continue
        for candidato in (_desescapar(frag), frag):
            try:
                datos = json.loads(candidato)
            except Exception:
                continue
            if (isinstance(datos, list) and datos and isinstance(datos[0], dict)
                    and ("display_name" in datos[0] or "legal_name" in datos[0])):
                return datos
    return []


def _mapear_provider(p):
    """Pasa un provider del JSON a las columnas de la tabla."""
    legal = (p.get("legal_name") or "").strip()
    direccion = " | ".join(x for x in (legal, (p.get("address") or "").strip()) if x)
    return {
        "operador": (p.get("display_name") or legal).strip() or SIN_DATO,
        "email": (p.get("email") or "").strip() or SIN_DATO,
        "telefono": (p.get("phone") or "").strip() or SIN_DATO,
        "direccion": direccion or SIN_DATO,
    }


class CivitatisOperadoresScraper:
    SELECTORS = {
        "currency_nav": "#page-nav__currency",
        "currency_option": ".o-page-nav__dropdown__body span[data-value='{code}']",
        "container": ".o-search-list__item",
        "title": ".comfort-card__title",
        # El enlace real de la actividad envuelve la tarjeta entera. El <a> que
        # hay dentro de .comfort-card__title es el botón de favoritos (href="#").
        "activity_link": "a._activity-link",
        "price": ".comfort-card__price__text",
        "rating_opiniones": ".text--rating-total",
        "rating_val": ".m-rating--text",
        "viajeros": "span._full",
        # Cuando NO hay página siguiente el sitio renderiza
        # <span class="next-element --deactivated">, por eso se exige el <a>.
        "next_link": "a.next-element",
        "total_label": ".o-pagination__showing",
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button",
        "full_description_container": "#descripcion",
        "view_more_trigger": "#view-more-trigger",
        "provider_link": "a.o-answers-provider__name",
        "info_lines": ".o-answers-provider__info",
    }

    def __init__(
        self,
        currency_code="USD",
        fecha_scan=None,
        concurrencia_destinos=3,
        concurrencia_detalle=6,
        incluir_descripcion=True,
        max_desc_chars=5000,
        headless=True,
        timeout_lista_ms=60000,
        timeout_detalle_ms=45000,
        espera_contenedor_ms=15000,
        max_paginas=200,
        backoff_base=20,
        reintentos_http=4,
        pausa_entre_fichas=0.0,
        proxy=None,
    ):
        self.currency_code = currency_code
        self.fecha_scan = fecha_scan
        self.concurrencia_destinos = max(1, concurrencia_destinos)
        self.concurrencia_detalle = max(1, concurrencia_detalle)
        self.incluir_descripcion = incluir_descripcion
        self.max_desc_chars = max_desc_chars
        self.headless = headless
        self.timeout_lista_ms = timeout_lista_ms
        self.timeout_detalle_ms = timeout_detalle_ms
        self.espera_contenedor_ms = espera_contenedor_ms
        self.max_paginas = max_paginas
        self.backoff_base = backoff_base
        self.reintentos_http = reintentos_http
        self.pausa_entre_fichas = pausa_entre_fichas
        self.proxy = proxy

        self.playwright = None
        self.browser = None
        self.context = None

        self._deadline = None
        self.detenido_por_tiempo = False

        self._urls_vistas = set()
        self._lock_vistas = asyncio.Lock()
        # slug canónico -> nº de veces que se descartó una tarjeta suya. Sirve
        # para detectar actividades que ningún destino de la lista reclama.
        self.slugs_descartados = Counter()

        self.stats = {
            "destinos_ok": 0,
            "destinos_error": 0,
            "destinos_vacios": 0,
            "paginas": 0,
            "actividades": 0,
            "actividades_omitidas": 0,
            "actividades_sin_operador": 0,
            "tarjetas_otro_destino": 0,
            "fichas_via_json": 0,
            "fichas_via_dom": 0,
            "filas": 0,
            "http_bloqueos": 0,
        }

    # ------------------------------------------------------------------ #
    # Infraestructura del navegador
    # ------------------------------------------------------------------ #

    async def init_browser(self):
        self.playwright = await async_playwright().start()
        lanzamiento = {
            "headless": self.headless,
            "args": ["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
        }
        if self.proxy:
            # En Chromium el proxy se define al lanzar el navegador.
            lanzamiento["proxy"] = self.proxy
            print(f"🛡️  Saliendo por proxy {self.proxy['server']} ({self.proxy['username'].split(',')[0]})")

        self.browser = await self.playwright.chromium.launch(**lanzamiento)
        self.context = await self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="es-ES",
            viewport={"width": 1366, "height": 900},
        )
        # El bloqueo se registra en el contexto: aplica a todas las pestañas.
        await self.context.route("**/*", self._block_heavy_resources)
        print("✅ Navegador iniciado")

    async def close_browser(self):
        for cerrar in (self.context, self.browser):
            try:
                if cerrar:
                    await cerrar.close()
            except Exception:
                pass
        try:
            if self.playwright:
                await self.playwright.stop()
        except Exception:
            pass
        print("🔒 Navegador cerrado")

    async def _block_heavy_resources(self, route):
        # Se deja pasar el CSS: sin estilos los dropdowns de operadores no son
        # "visibles" para Playwright y los clics fallan.
        try:
            if route.request.resource_type in ("image", "media", "font"):
                await route.abort()
            else:
                await route.continue_()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    async def _texto_seguro(elemento, selector):
        try:
            target = await elemento.query_selector(selector)
            if target:
                return (await target.inner_text()).strip()
        except Exception:
            pass
        return None

    @staticmethod
    def _limpiar_numero(texto, tipo="float"):
        if not texto:
            return 0 if tipo == "int" else 0.0
        limpio = re.sub(r"[^\d,]", "", texto).replace(",", ".")
        try:
            valor = float(limpio)
            return int(valor) if tipo == "int" else valor
        except ValueError:
            return 0 if tipo == "int" else 0.0

    @staticmethod
    def _limpiar_rating(texto):
        if not texto:
            return 0.0
        if "/" in texto:
            texto = texto.split("/")[0]
        texto = re.sub(r"[^\d.]", "", texto.replace(",", "."))
        try:
            return float(texto)
        except ValueError:
            return 0.0

    def _chequear_deadline(self):
        if self._deadline is not None and time.monotonic() > self._deadline:
            self.detenido_por_tiempo = True
            raise DeadlineAlcanzado()

    async def _ir_a(self, page, url, timeout_ms):
        """
        Navega reintentando ante 429/5xx. Civitatis limita por IP de forma
        agresiva, así que sin este backoff el scraping completo se cae.
        Devuelve el status HTTP (None si no hubo respuesta) o lanza si no
        logra cargar la página.
        """
        ultimo = None
        for intento in range(self.reintentos_http):
            self._chequear_deadline()
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                ultimo = resp.status if resp else None
                if ultimo is None or ultimo < 400:
                    return ultimo
                if ultimo in (429, 500, 502, 503, 504):
                    self.stats["http_bloqueos"] += 1
                    if intento == self.reintentos_http - 1:
                        break
                    espera = self.backoff_base * (2 ** intento)
                    print(f"   ⏳ HTTP {ultimo} en {url} — esperando {espera}s", flush=True)
                    await asyncio.sleep(espera)
                    continue
                return ultimo  # 404/410/...: no tiene sentido reintentar
            except DeadlineAlcanzado:
                raise
            except Exception:
                if intento == self.reintentos_http - 1:
                    raise
                await asyncio.sleep(self.backoff_base * (2 ** intento))
        return ultimo

    async def _handle_overlays(self, page):
        try:
            await page.evaluate(
                "() => { document.querySelectorAll("
                "'.lottie-reveal-overlay, #lottie-modal, ._cookies-banner'"
                ").forEach(el => el.remove()); }"
            )
            cookie = await page.query_selector(self.SELECTORS["cookie_btn"])
            if cookie and await cookie.is_visible():
                await cookie.click()
        except Exception:
            pass

    async def _scroll_to_bottom(self, page):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
        except Exception:
            pass

    MONEDAS_VALIDAS = {"ARS", "BRL", "CLP", "COP", "EUR", "GBP", "MXN", "PEN", "USD"}

    @staticmethod
    async def _moneda_actual(page):
        try:
            return await page.evaluate(
                "() => document.querySelector('#currencySelectorButton')?.dataset.value || null"
            )
        except Exception:
            return None

    async def preparar_sesion(self):
        """
        Acepta cookies y fija la moneda una sola vez para todo el contexto.
        Si la moneda no queda aplicada se aborta: el sitio seguiría devolviendo
        precios en EUR y los guardaríamos etiquetados con otra moneda.
        """
        if self.currency_code not in self.MONEDAS_VALIDAS:
            raise RuntimeError(
                f"Moneda '{self.currency_code}' no ofrecida por Civitatis. "
                f"Válidas: {sorted(self.MONEDAS_VALIDAS)}"
            )

        page = await self.context.new_page()
        try:
            status = await self._ir_a(page, "https://www.civitatis.com/es/", 60000)
            if status is not None and status >= 400:
                raise RuntimeError(
                    f"Civitatis respondió HTTP {status} en la home. "
                    "Suele ser bloqueo por IP: usá Apify Proxy (APIFY_PROXY_PASSWORD)."
                )
            await self._handle_overlays(page)

            if await self._moneda_actual(page) == self.currency_code:
                print(f"💱 Moneda ya estaba en {self.currency_code}")
                return

            await page.wait_for_selector(self.SELECTORS["currency_nav"], timeout=15000)
            await page.click(self.SELECTORS["currency_nav"])
            await page.click(self.SELECTORS["currency_option"].format(code=self.currency_code))
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass

            actual = await self._moneda_actual(page)
            if actual != self.currency_code:
                raise RuntimeError(
                    f"No se pudo fijar la moneda {self.currency_code} (quedó en {actual}). "
                    "Se aborta para no guardar precios con la moneda equivocada."
                )
            print(f"💱 Moneda fijada en {self.currency_code}")
        finally:
            await page.close()

    # ------------------------------------------------------------------ #
    # Recorrido principal
    # ------------------------------------------------------------------ #

    async def run(self, destinos, on_rows, urls_omitidas=None, deadline=None):
        """
        destinos      : lista de dicts del destinos_civitatis.json
        on_rows       : coroutine `async def on_rows(filas, destino)` que recibe cada lote
        urls_omitidas : set de url_actividad ya cargadas (para reanudar)
        deadline      : time.monotonic() límite; al superarlo corta de forma ordenada
        """
        self._deadline = deadline
        self._urls_vistas = set(urls_omitidas or ())
        omitidas_iniciales = len(self._urls_vistas)

        sem_destinos = asyncio.Semaphore(self.concurrencia_destinos)
        sem_detalle = asyncio.Semaphore(self.concurrencia_detalle)
        total = len(destinos)
        contador = {"hechos": 0}

        async def procesar(indice, destino):
            async with sem_destinos:
                if self.detenido_por_tiempo:
                    return
                try:
                    await self._procesar_destino(destino, on_rows, sem_detalle)
                    self.stats["destinos_ok"] += 1
                except DeadlineAlcanzado:
                    return
                except Exception as e:
                    self.stats["destinos_error"] += 1
                    print(f"❌ Error en destino {destino.get('name')}: {type(e).__name__}: {e}")
                finally:
                    contador["hechos"] += 1
                    print(
                        f"📈 Progreso destinos: {contador['hechos']}/{total} "
                        f"| filas emitidas: {self.stats['filas']} "
                        f"| actividades: {self.stats['actividades']}",
                        flush=True,
                    )

        await asyncio.gather(*(procesar(i, d) for i, d in enumerate(destinos)))

        self.stats["actividades_omitidas"] = omitidas_iniciales
        return self.stats

    async def _procesar_destino(self, destino, on_rows, sem_detalle):
        self._chequear_deadline()

        slug = destino["url"]
        url_destino = f"https://www.civitatis.com/es/{slug}/"
        page = await self.context.new_page()
        print(f"🌍 Destino: {destino['name']} ({destino['nameCountry']}) -> {url_destino}", flush=True)

        try:
            url_actual = url_destino
            pagina_num = 0
            tarjetas_vistas = 0
            descartadas = 0

            # El listado del destino está paginado de a 20 actividades y la
            # página siguiente se obtiene del href de a.next-element
            # (https://www.civitatis.com/es/madrid/2/, /3/, ...).
            while url_actual and pagina_num < self.max_paginas:
                self._chequear_deadline()
                pagina_num += 1

                status = await self._ir_a(page, url_actual, self.timeout_lista_ms)
                if status is not None and status >= 400:
                    print(f"   ⚠️ {destino['name']} pág. {pagina_num}: HTTP {status}, se corta el destino")
                    break

                await self._handle_overlays(page)
                self.stats["paginas"] += 1

                try:
                    await page.wait_for_selector(
                        self.SELECTORS["container"], state="attached", timeout=self.espera_contenedor_ms
                    )
                except Exception:
                    if pagina_num == 1:
                        self.stats["destinos_vacios"] += 1
                        print(f"   ⚠️ Sin actividades visibles en {destino['name']}")
                    break

                if pagina_num == 1:
                    total_txt = await self._texto_seguro(page, self.SELECTORS["total_label"])
                    if total_txt:
                        print(f"   ℹ️ {destino['name']}: {' '.join(total_txt.split())}")

                await self._scroll_to_bottom(page)
                items = await page.query_selector_all(self.SELECTORS["container"])
                if not items:
                    break

                tarjetas = []
                for item in items:
                    tarjeta = await self._leer_tarjeta(item, page, slug, url_destino)
                    if tarjeta:
                        tarjetas.append(tarjeta)
                tarjetas_vistas += len(items)
                descartadas += len(items) - len(tarjetas)

                # Dedup global: una actividad puede repetirse entre páginas o
                # ya venir cargada de una corrida anterior (urls_omitidas).
                nuevas = []
                async with self._lock_vistas:
                    for t in tarjetas:
                        if t["url_actividad"] in self._urls_vistas:
                            continue
                        self._urls_vistas.add(t["url_actividad"])
                        nuevas.append(t)

                if nuevas:
                    filas = await self._scrapear_fichas(nuevas, destino, sem_detalle)
                    if filas:
                        self.stats["filas"] += len(filas)
                        await on_rows(filas, destino)

                # Siguiente página: si es un <span --deactivated> no matchea y
                # el bucle termina.
                siguiente = await page.query_selector(self.SELECTORS["next_link"])
                href = await siguiente.get_attribute("href") if siguiente else None
                url_actual = urljoin(page.url, href) if href else None

            # `descartadas` son tarjetas cuya URL canónica cuelga de OTRO
            # destino (Civitatis lista actividades de pueblos vecinos). No se
            # pierden: se scrapean cuando le toca el turno a su destino propio.
            self.stats["tarjetas_otro_destino"] += descartadas
            extra = f" ({descartadas} de otro destino)" if descartadas else ""
            print(
                f"   ✓ {destino['name']}: {tarjetas_vistas - descartadas} actividades propias "
                f"de {tarjetas_vistas} tarjetas en {pagina_num} página(s){extra}",
                flush=True,
            )

        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _leer_tarjeta(self, item, page, slug, url_destino):
        try:
            titulo = await self._texto_seguro(item, self.SELECTORS["title"])

            link = await item.query_selector(self.SELECTORS["activity_link"])
            if not link:
                link = await item.query_selector("a[href]:not([href='#'])")
            if not link:
                return None

            href = await link.get_attribute("href")
            if not href:
                return None

            url_actividad = urljoin(page.url, href)
            # Un listado puede incluir actividades de localidades vecinas. Se
            # atribuye cada actividad SOLO a su destino canónico (el del slug
            # de su propia URL) para no duplicarla entre shards; la recoge el
            # destino que le corresponde.
            if slug.lower() not in url_actividad.lower():
                # Se registra de qué destino es realmente para poder auditar
                # después si quedó algún slug sin cubrir.
                partes = url_actividad.split("/es/", 1)
                if len(partes) == 2:
                    canonico = partes[1].split("/")[0].lower()
                    if canonico:
                        self.slugs_descartados[canonico] += 1
                return None
            if url_actividad.rstrip("/") == url_destino.rstrip("/"):
                return None

            return {
                "actividad": titulo,
                "url_actividad": url_actividad,
                "precio_txt": await self._texto_seguro(item, self.SELECTORS["price"]),
                "viajeros_txt": await self._texto_seguro(item, self.SELECTORS["viajeros"]),
                "opiniones_txt": await self._texto_seguro(item, self.SELECTORS["rating_opiniones"]),
                "rating_txt": await self._texto_seguro(item, self.SELECTORS["rating_val"]),
            }
        except Exception:
            return None

    async def _scrapear_fichas(self, tarjetas, destino, sem_detalle):
        async def una(tarjeta):
            async with sem_detalle:
                if self.detenido_por_tiempo:
                    return []
                operadores, descripcion = await self._scrape_detalle(tarjeta["url_actividad"])
                if self.pausa_entre_fichas:
                    await asyncio.sleep(self.pausa_entre_fichas)
                self.stats["actividades"] += 1
                if not operadores:
                    # La ficha no cargó: no se emite nada y la próxima corrida
                    # la reintenta (no quedó en BigQuery).
                    self.stats["actividades_sin_operador"] += 1
                    return []
                base = {
                    "pais": destino.get("nameCountry"),
                    "destino": destino.get("name"),
                    "actividad": tarjeta["actividad"],
                    "url_actividad": tarjeta["url_actividad"],
                    "descripcion": descripcion,
                    "precio_real": self._limpiar_numero(tarjeta["precio_txt"], "float"),
                    "opiniones": self._limpiar_numero(tarjeta["opiniones_txt"], "int"),
                    "viajeros": self._limpiar_numero(tarjeta["viajeros_txt"], "int"),
                    "rating": self._limpiar_rating(tarjeta["rating_txt"]),
                    "moneda": self.currency_code,
                    "fecha_scan": self.fecha_scan,
                }
                return [
                    {
                        **base,
                        "operador": op["operador"],
                        "email": op["email"],
                        "telefono": op["telefono"],
                        "direccion": op["direccion"],
                    }
                    for op in operadores
                ]

        lotes = await asyncio.gather(*(una(t) for t in tarjetas), return_exceptions=True)
        filas = []
        for lote in lotes:
            if isinstance(lote, Exception):
                continue
            filas.extend(lote)
        return filas

    async def _buscar_proveedores(self, page):
        """
        El bloque de operadores vive en el FAQ, abajo del todo de la ficha.
        Primero se prueba directo; si no está, se baja la página para forzar
        el render diferido y se reintenta antes de darlo por inexistente.
        """
        sel = self.SELECTORS["provider_link"]
        links = await page.query_selector_all(sel)
        if links:
            return links

        try:
            await page.wait_for_selector(sel, state="attached", timeout=2500)
            return await page.query_selector_all(sel)
        except Exception:
            pass

        try:
            for i in range(1, 5):
                await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {i} / 4)")
                await asyncio.sleep(0.4)
            await page.wait_for_selector(sel, state="attached", timeout=4000)
        except Exception:
            return []

        return await page.query_selector_all(sel)

    async def _scrape_detalle(self, url):
        page = await self.context.new_page()
        operadores = []
        descripcion = ""

        try:
            status = await self._ir_a(page, url, self.timeout_detalle_ms)
            if status is not None and status >= 400:
                return [], ""

            if self.incluir_descripcion:
                try:
                    await page.evaluate(
                        "(sel) => { const b = document.querySelector(sel); if (b) b.remove(); }",
                        self.SELECTORS["view_more_trigger"],
                    )
                    desc_el = await page.query_selector(self.SELECTORS["full_description_container"])
                    if desc_el:
                        crudo = await desc_el.inner_text()
                        limpio = crudo.replace("\n", " || ").replace("\r", "")
                        descripcion = " ".join(limpio.split())[: self.max_desc_chars]
                except Exception:
                    pass

            # --- Variante Next.js: operadores en el payload de hidratación ---
            try:
                provs = extraer_providers_json(await page.content())
            except Exception:
                provs = []
            if provs:
                self.stats["fichas_via_json"] += 1
                return [_mapear_provider(p) for p in provs], descripcion

            # --- Variante legacy: operadores renderizados en el HTML ---
            links = await self._buscar_proveedores(page)

            if not links:
                operadores.append(
                    {
                        "operador": OPERADOR_GENERICO,
                        "email": SIN_DATO,
                        "telefono": SIN_DATO,
                        "direccion": SIN_DATO,
                    }
                )
            else:
                self.stats["fichas_via_dom"] += 1
                # Los datos del operador ya están en el DOM dentro de spans con
                # clase js-hide (el sitio solo los muestra/oculta por CSS). Se
                # leen con textContent: no hace falta ningún clic, que era lo
                # más lento y frágil de la versión anterior.
                for link in links:
                    datos = {k: SIN_DATO for k in ("operador", "email", "telefono", "direccion")}
                    try:
                        datos["operador"] = (await link.evaluate("e => e.textContent")).strip() or SIN_DATO
                        target_id = await link.get_attribute("data-dropdow-target")
                        if target_id:
                            textos = await page.evaluate(
                                """(id) => {
                                    const c = document.getElementById(id);
                                    if (!c) return [];
                                    return [...c.querySelectorAll('.o-answers-provider__info')]
                                        .map(e => e.textContent.replace(/\\s+/g, ' ').trim());
                                }""",
                                target_id,
                            )
                            for txt in textos:
                                low = txt.lower()
                                if low.startswith("correo electrónico:"):
                                    datos["email"] = txt.split(":", 1)[1].strip()
                                elif low.startswith("teléfono:"):
                                    datos["telefono"] = txt.split(":", 1)[1].strip()
                                elif "domicilio" in low or "razón social" in low:
                                    info = (
                                        txt.replace("Domicilio Social:", "")
                                        .replace("Razón social:", "")
                                        .strip()
                                    )
                                    actual = datos["direccion"]
                                    datos["direccion"] = (
                                        info if actual == SIN_DATO else f"{actual} | {info}"
                                    )
                        operadores.append(datos)
                    except Exception:
                        continue

            if not operadores:
                operadores.append(
                    {
                        "operador": OPERADOR_GENERICO,
                        "email": SIN_DATO,
                        "telefono": SIN_DATO,
                        "direccion": SIN_DATO,
                    }
                )

        except Exception:
            # Una ficha caída no debe tumbar el destino: se devuelve vacío.
            pass
        finally:
            try:
                await page.close()
            except Exception:
                pass

        return operadores, descripcion
