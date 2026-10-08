"""
Driver de opiniones (reviews) de Civitatis.

Reemplaza la lógica que vivía dentro de reviews_destino.py. Cambios de fondo
respecto de la versión anterior, todos verificados contra el sitio en vivo:

  - El enlace de la actividad NO está en `.comfort-card__title a` (ese <a> es el
    botón de favoritos, href="#"). El enlace real es `a._activity-link`.
  - El listado del destino se pagina por URL (`/es/<destino>/2/`), no haciendo
    click: en la página 1 ni siquiera existe el elemento "siguiente".
  - Las opiniones también se paginan por URL
    (`/es/<destino>/<actividad>/opiniones/2/`). El botón "siguiente" es un
    <span class="next-element js-link"> con la URL en base64 en `data-loc`, así
    que clickear era frágil. Además `.last-element[data-page]` dice de entrada
    cuántas páginas hay, lo que permite informar progreso real.
  - Las opiniones vienen ordenadas de más nueva a más vieja: en cuanto aparece
    una anterior a `fecha_desde` se corta la actividad.
  - Se extraen campos que antes se perdían: rating, nombre, comentario, tipo de
    viajero y código ISO de país (de la clase `b-flag_xx`).
  - No escribe CSV: entrega las filas al orquestador por callback para que las
    suba a BigQuery en lotes incrementales.
  - Soporta deadline (cortar antes del timeout de GitHub Actions) y lista de
    actividades ya cargadas (para reanudar).
"""

import asyncio
import hashlib
import re
import time
from datetime import date, datetime
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright

COLUMNAS = [
    "review_hash", "pais", "destino", "actividad", "url_actividad",
    "rating", "fecha_review", "nombre_usuario", "pais_usuario",
    "cod_pais_usuario", "tipo_viajero", "comentario", "fecha_scan",
]

MESES_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12,
}

RE_FLAG = re.compile(r"b-flag_([a-z]{2,3})\b")
RE_RATING = re.compile(r"(\d+(?:[.,]\d+)?)")


class DeadlineAlcanzado(Exception):
    """Se agotó el tiempo máximo de ejecución asignado al scraper."""


def parsear_fecha(texto):
    """'30 / Ago / 2026' -> date(2026, 8, 30). None si no se puede."""
    if not texto:
        return None
    partes = [p.strip() for p in str(texto).split("/")]
    if len(partes) != 3:
        return None
    try:
        dia = int(re.sub(r"\D", "", partes[0]))
        mes = MESES_ES.get(partes[1][:3].lower())
        anio = int(re.sub(r"\D", "", partes[2]))
        if not mes:
            return None
        return date(anio, mes, dia)
    except (ValueError, TypeError):
        return None


class CivitatisReviewsScraper:
    SELECTORS = {
        # --- listado de actividades del destino ---
        "container": ".o-search-list__item",
        "title": ".comfort-card__title",
        "activity_link": "a._activity-link",
        "card_opiniones": ".text--rating-total",
        "ver_todas": "a.button-list-footer",
        "total_label": ".o-pagination__showing",
        # --- ficha de opiniones ---
        "review": ".o-container-opiniones-small",
        "review_date": ".a-opiniones-date",
        "review_rating": ".m-rating-stars",
        "review_rating_alt": ".m-rating__stars__full .hide",
        "review_name": ".opi-name",
        "review_location": ".opi-location",
        "review_flag": "[class*='b-flag_']",
        "review_type": ".a-opiniones-type div",
        "review_text": ".container-opinion-txt",
        "last_page": ".last-element",
        "cookie_btn": "#btn-accept-cookies, ._accept, .accept-button",
    }

    def __init__(
        self,
        fecha_desde,
        fecha_hasta=None,
        fecha_scan=None,
        concurrencia_actividades=5,
        limite_actividades=None,
        solo_con_opiniones=True,
        headless=True,
        timeout_lista_ms=60000,
        timeout_reviews_ms=45000,
        espera_contenedor_ms=15000,
        max_paginas_listado=60,
        max_paginas_reviews=5000,
        backoff_base=20,
        reintentos_http=5,
    ):
        self.fecha_desde = fecha_desde
        self.fecha_hasta = fecha_hasta
        self.fecha_scan = fecha_scan
        self.concurrencia_actividades = max(1, concurrencia_actividades)
        self.limite_actividades = limite_actividades
        self.solo_con_opiniones = solo_con_opiniones
        self.headless = headless
        self.timeout_lista_ms = timeout_lista_ms
        self.timeout_reviews_ms = timeout_reviews_ms
        self.espera_contenedor_ms = espera_contenedor_ms
        self.max_paginas_listado = max_paginas_listado
        self.max_paginas_reviews = max_paginas_reviews
        self.backoff_base = backoff_base
        self.reintentos_http = reintentos_http

        self.playwright = None
        self.browser = None
        self.context = None

        self._deadline = None
        self.detenido_por_tiempo = False
        self._urls_omitidas = set()

        self.stats = {
            "actividades_listadas": 0,
            "actividades_scrapeadas": 0,
            "actividades_omitidas": 0,
            "actividades_sin_opiniones": 0,
            "actividades_error": 0,
            "tarjetas_descartadas": 0,
            "paginas_listado": 0,
            "paginas_reviews": 0,
            "reviews_vistas": 0,
            "reviews_en_rango": 0,
            "http_bloqueos": 0,
        }

    # ------------------------------------------------------------------ #
    # Infraestructura del navegador
    # ------------------------------------------------------------------ #

    async def init_browser(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
        )
        self.context = await self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="es-ES",
            viewport={"width": 1366, "height": 900},
        )
        await self.context.route("**/*", self._block_heavy_resources)
        print("✅ Navegador iniciado", flush=True)

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
        print("🔒 Navegador cerrado", flush=True)

    async def _block_heavy_resources(self, route):
        # Las opiniones son HTML servido por el servidor: no hace falta ni CSS.
        try:
            if route.request.resource_type in ("image", "media", "font", "stylesheet"):
                await route.abort()
            else:
                await route.continue_()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _chequear_deadline(self):
        if self._deadline is not None and time.monotonic() > self._deadline:
            self.detenido_por_tiempo = True
            raise DeadlineAlcanzado()

    def tiempo_restante(self):
        if self._deadline is None:
            return None
        return max(0, int(self._deadline - time.monotonic()))

    @staticmethod
    async def _texto(elemento, selector):
        try:
            target = await elemento.query_selector(selector)
            if target:
                return " ".join((await target.inner_text()).split())
        except Exception:
            pass
        return None

    async def _ir_a(self, page, url, timeout_ms):
        """
        Navega reintentando ante 429/5xx. Civitatis limita por IP de forma
        agresiva: sin este backoff el scraping se cae a los pocos minutos.
        Devuelve el status HTTP (None si no hubo respuesta).
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
                "'.lottie-reveal-overlay, #lottie-modal, ._cookies-banner, #didomi-host'"
                ").forEach(el => el.remove()); }"
            )
            cookie = await page.query_selector(self.SELECTORS["cookie_btn"])
            if cookie and await cookie.is_visible():
                await cookie.click()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Recorrido principal
    # ------------------------------------------------------------------ #

    async def run(self, destino, on_rows, urls_omitidas=None, deadline=None):
        """
        destino       : dict del destinos_civitatis.json
        on_rows       : coroutine `async def on_rows(filas)` que recibe cada lote
        urls_omitidas : set de url_actividad ya cargadas (para reanudar)
        deadline      : time.monotonic() límite; al superarlo corta ordenado
        """
        self._deadline = deadline
        self._urls_omitidas = set(urls_omitidas or ())

        nombre = destino["name"]
        pais = destino.get("nameCountry", "")
        slug = destino["url"]

        print(f"\n🌍 Destino: {nombre} ({pais}) — opiniones desde {self.fecha_desde}", flush=True)

        try:
            actividades = await self._listar_actividades(slug, nombre)
        except DeadlineAlcanzado:
            print("⏱️ Se agotó el tiempo durante el listado de actividades.", flush=True)
            return self.stats

        self.stats["actividades_listadas"] = len(actividades)
        if not actividades:
            print(f"⚠️ {nombre}: no se listó ninguna actividad.", flush=True)
            return self.stats

        pendientes = [a for a in actividades if a["url_actividad"] not in self._urls_omitidas]
        self.stats["actividades_omitidas"] = len(actividades) - len(pendientes)
        if self.limite_actividades:
            pendientes = pendientes[: self.limite_actividades]
            print(f"   ⚠️ Modo prueba: solo las primeras {len(pendientes)} actividades", flush=True)

        print(
            f"   ↳ {len(actividades)} actividades listadas | "
            f"{self.stats['actividades_omitidas']} ya cargadas (se saltan) | "
            f"{len(pendientes)} pendientes",
            flush=True,
        )

        sem = asyncio.Semaphore(self.concurrencia_actividades)
        total = len(pendientes)
        hechas = {"n": 0}

        async def procesar(act):
            async with sem:
                if self.detenido_por_tiempo:
                    return
                try:
                    await self._scrapear_actividad(act, pais, nombre, on_rows)
                    self.stats["actividades_scrapeadas"] += 1
                except DeadlineAlcanzado:
                    return
                except Exception as e:
                    self.stats["actividades_error"] += 1
                    print(f"   ❌ {act['actividad'][:60]}: {type(e).__name__}: {e}", flush=True)
                finally:
                    hechas["n"] += 1
                    restante = self.tiempo_restante()
                    print(
                        f"📈 [{nombre}] {hechas['n']}/{total} actividades | "
                        f"{self.stats['reviews_en_rango']} reviews en rango | "
                        f"{self.stats['paginas_reviews']} páginas"
                        + (f" | quedan {restante // 60}min" if restante is not None else ""),
                        flush=True,
                    )

        await asyncio.gather(*(procesar(a) for a in pendientes))
        return self.stats

    # ------------------------------------------------------------------ #
    # Listado de actividades del destino
    # ------------------------------------------------------------------ #

    async def _listar_actividades(self, slug, nombre_destino):
        """
        Junta las actividades del destino recorriendo dos raíces paginables y
        deduplicando por URL:

          1. la home del destino (`/es/<slug>/`, `/es/<slug>/2/`, …)
          2. el listado "Ver todas las actividades" (`/es/<slug>/buscar/…`)

        Se usan las dos porque la home no muestra el control de paginación en
        la página 1 (aunque `/2/` existe igual) y porque no todos los destinos
        exponen el botón "Ver todas". Pedir una página fuera de rango devuelve
        la última, así que el corte real es "esta página no aportó URLs nuevas".
        """
        page = await self.context.new_page()
        actividades = []
        vistas = set()

        try:
            raices = await self._raices_listado(page, slug)
            for raiz in raices:
                antes = len(actividades)
                await self._paginar_listado(page, raiz, nombre_destino, actividades, vistas)
                print(f"   ↳ {raiz}: +{len(actividades) - antes} actividades nuevas", flush=True)
        finally:
            try:
                await page.close()
            except Exception:
                pass

        return actividades

    async def _paginar_listado(self, page, raiz, nombre_destino, actividades, vistas):
        urls_previas = None
        for num in range(1, self.max_paginas_listado + 1):
            self._chequear_deadline()
            url = raiz if num == 1 else f"{raiz}{num}/"

            status = await self._ir_a(page, url, self.timeout_lista_ms)
            if status is not None and status >= 400:
                if num == 1:
                    print(f"   ⚠️ {url}: HTTP {status}", flush=True)
                break

            await self._handle_overlays(page)
            self.stats["paginas_listado"] += 1

            try:
                await page.wait_for_selector(
                    self.SELECTORS["container"], state="attached",
                    timeout=self.espera_contenedor_ms,
                )
            except Exception:
                break

            if num == 1:
                total_txt = await self._texto(page, self.SELECTORS["total_label"])
                if total_txt:
                    print(f"   ℹ️ {nombre_destino}: {total_txt}", flush=True)

            items = await page.query_selector_all(self.SELECTORS["container"])
            if not items:
                break

            urls_pagina = []
            for item in items:
                tarjeta = await self._leer_tarjeta(item, page)
                if not tarjeta:
                    # Tarjetas del listado que no son fichas de actividad
                    # (promos, bloques editoriales). Se cuentan para que no
                    # desaparezcan en silencio del total del destino.
                    self.stats["tarjetas_descartadas"] += 1
                    continue
                urls_pagina.append(tarjeta["url_actividad"])
                if tarjeta["url_actividad"] in vistas:
                    continue
                vistas.add(tarjeta["url_actividad"])
                actividades.append(tarjeta)

            if not urls_pagina:
                break
            # Pedir una página fuera de rango devuelve la última: el corte es
            # que la página repita exactamente el contenido de la anterior. No
            # alcanza con "no trajo nada nuevo": la segunda raíz arranca con
            # actividades que ya vimos en la primera y cortaría en la página 1.
            if urls_pagina == urls_previas:
                break
            urls_previas = urls_pagina

    async def _raices_listado(self, page, slug):
        """
        Home del destino + listado "Ver todas las actividades" (si existe y es
        distinto). Ambas aceptan paginación numérica agregando `<n>/`.
        """
        base = f"https://www.civitatis.com/es/{slug}/"
        raices = [base]
        try:
            status = await self._ir_a(page, base, self.timeout_lista_ms)
            if status is not None and status >= 400:
                return raices
            await self._handle_overlays(page)
            link = await page.query_selector(self.SELECTORS["ver_todas"])
            href = await link.get_attribute("href") if link else None
            if href:
                raiz = urljoin(base, href)
                if not raiz.endswith("/"):
                    raiz += "/"
                if raiz != base:
                    print(f"   ℹ️ Listado completo: {raiz}", flush=True)
                    raices.append(raiz)
        except DeadlineAlcanzado:
            raise
        except Exception:
            pass
        return raices

    async def _leer_tarjeta(self, item, page):
        try:
            link = await item.query_selector(self.SELECTORS["activity_link"])
            if not link:
                link = await item.query_selector("a[href]:not([href='#'])")
            if not link:
                return None

            href = await link.get_attribute("href")
            if not href:
                return None

            url_actividad = urljoin(page.url, href)
            if not self._es_url_de_actividad(url_actividad):
                return None

            titulo = await self._texto(item, self.SELECTORS["title"]) or ""
            opiniones = self._a_entero(await self._texto(item, self.SELECTORS["card_opiniones"]))

            return {
                "actividad": titulo,
                "url_actividad": url_actividad if url_actividad.endswith("/") else url_actividad + "/",
                "opiniones_card": opiniones,
            }
        except Exception:
            return None

    @staticmethod
    def _es_url_de_actividad(url):
        """
        Una ficha de actividad es `/<idioma>/<destino>/<actividad>/`. Con dos
        segmentos es la home de un destino y con más es otra sección. No se
        filtra por el slug del destino a propósito: hay actividades listadas en
        un destino cuya URL cuelga de otro (excursiones de un día, por ejemplo)
        y filtrar por slug las perdía.
        """
        partes = [p for p in urlparse(url).path.split("/") if p]
        if len(partes) != 3:
            return False
        return partes[2] not in ("buscar", "opiniones")

    @staticmethod
    def _a_entero(texto):
        """'1.234 opiniones' -> 1234 ; None si no hay número."""
        if not texto:
            return None
        limpio = re.sub(r"[^\d]", "", texto.split("opinion")[0])
        return int(limpio) if limpio else None

    # ------------------------------------------------------------------ #
    # Opiniones de una actividad
    # ------------------------------------------------------------------ #

    async def _scrapear_actividad(self, act, pais, destino, on_rows):
        if self.solo_con_opiniones and act["opiniones_card"] == 0:
            self.stats["actividades_sin_opiniones"] += 1
            return

        base = act["url_actividad"] + "opiniones/"
        page = await self.context.new_page()
        filas = []
        total_paginas = None

        try:
            num = 1
            firma_previa = None

            while num <= self.max_paginas_reviews:
                self._chequear_deadline()
                url = base if num == 1 else f"{base}{num}/"

                status = await self._ir_a(page, url, self.timeout_reviews_ms)
                if status is not None and status >= 400:
                    break
                if "opiniones" not in page.url:
                    break  # redirigió fuera de la sección de opiniones

                if num == 1:
                    await self._handle_overlays(page)

                elementos = await page.query_selector_all(self.SELECTORS["review"])
                if not elementos:
                    break

                self.stats["paginas_reviews"] += 1

                if total_paginas is None:
                    total_paginas = await self._total_paginas(page)

                pagina = []
                for el in elementos:
                    review = await self._leer_review(el)
                    if review:
                        pagina.append(review)

                # Pedir una página fuera de rango devuelve la última, así que
                # una actividad de una sola página respondería lo mismo en la 2.
                # La comparación va ANTES de acumular: si no, esas opiniones
                # entrarían dos veces.
                firma = [
                    (r["fecha_review"], r["nombre_usuario"], r["rating"],
                     (r["comentario"] or "")[:60])
                    for r in pagina
                ]
                if firma and firma == firma_previa:
                    break
                firma_previa = firma

                self.stats["reviews_vistas"] += len(pagina)

                corta_por_fecha = False
                for review in pagina:
                    fecha = review["fecha_review"]
                    if fecha is None:
                        continue
                    if fecha < self.fecha_desde:
                        corta_por_fecha = True
                        continue
                    if self.fecha_hasta and fecha > self.fecha_hasta:
                        continue

                    filas.append({
                        **review,
                        "pais": pais,
                        "destino": destino,
                        "actividad": act["actividad"],
                        "url_actividad": act["url_actividad"],
                        "fecha_scan": self.fecha_scan,
                    })

                if corta_por_fecha:
                    break
                if total_paginas and num >= total_paginas:
                    break
                num += 1

        finally:
            try:
                await page.close()
            except Exception:
                pass

        if filas:
            self._asignar_hashes(filas)
            self.stats["reviews_en_rango"] += len(filas)
            await on_rows(filas)

    async def _total_paginas(self, page):
        try:
            last = await page.query_selector(self.SELECTORS["last_page"])
            if last:
                valor = await last.get_attribute("data-page")
                if valor and valor.isdigit():
                    return int(valor)
        except Exception:
            pass
        return None

    async def _leer_review(self, el):
        try:
            fecha = parsear_fecha(await self._texto(el, self.SELECTORS["review_date"]))

            rating = None
            try:
                nodo = await el.query_selector(self.SELECTORS["review_rating"])
                crudo = await nodo.get_attribute("title") if nodo else None
                if not crudo:
                    crudo = await self._texto(el, self.SELECTORS["review_rating_alt"])
                if crudo:
                    m = RE_RATING.search(crudo)
                    if m:
                        rating = float(m.group(1).replace(",", "."))
            except Exception:
                pass

            ubicacion = await self._texto(el, self.SELECTORS["review_location"]) or ""
            pais_usuario = ubicacion.split(",")[-1].strip() if ubicacion else None

            cod_pais = None
            try:
                flag = await el.query_selector(self.SELECTORS["review_flag"])
                if flag:
                    m = RE_FLAG.search(await flag.get_attribute("class") or "")
                    if m:
                        cod_pais = m.group(1).upper()
            except Exception:
                pass

            comentario = await self._texto(el, self.SELECTORS["review_text"])

            return {
                "fecha_review": fecha,
                "rating": rating,
                "nombre_usuario": await self._texto(el, self.SELECTORS["review_name"]),
                "pais_usuario": pais_usuario or None,
                "cod_pais_usuario": cod_pais,
                "tipo_viajero": await self._texto(el, self.SELECTORS["review_type"]),
                "comentario": comentario or None,
            }
        except Exception:
            return None

    @classmethod
    def _asignar_hashes(cls, filas):
        """
        Civitatis no expone un id de opinión, así que se arma uno con los
        campos que la identifican. Sirve para deduplicar en BigQuery entre
        corridas: la misma opinión siempre produce el mismo review_hash.

        Dos opiniones de la misma actividad pueden ser idénticas en todos los
        campos visibles (típico: "Anónimo", mismo día, misma nota, sin texto).
        Para que el hash siga siendo único se les agrega su número de repetición
        dentro de la actividad, que es estable mientras esas opiniones existan.
        """
        repeticiones = {}
        for fila in filas:
            semilla = cls._semilla_review(fila)
            n = repeticiones.get(semilla, 0)
            repeticiones[semilla] = n + 1
            final = semilla if n == 0 else f"{semilla}|#{n}"
            fila["review_hash"] = hashlib.sha1(final.encode("utf-8")).hexdigest()

    @staticmethod
    def _semilla_review(fila):
        fecha = fila.get("fecha_review")
        return "|".join([
            str(fila.get("url_actividad") or ""),
            fecha.isoformat() if isinstance(fecha, (date, datetime)) else str(fecha or ""),
            str(fila.get("nombre_usuario") or ""),
            str(fila.get("rating") or ""),
            str(fila.get("pais_usuario") or ""),
            str(fila.get("tipo_viajero") or ""),
            (str(fila.get("comentario") or ""))[:300],
        ])
