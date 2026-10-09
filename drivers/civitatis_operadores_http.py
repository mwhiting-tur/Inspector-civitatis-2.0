"""
Motor HTTP del scraper de operadores de Civitatis. Sin navegador y sin proxy.

Por qué existe
--------------
La versión con Playwright abría Chromium por ficha, y cada carga disparaba
decenas de peticiones (HTML + CSS + JS + XHR). En la corrida del 2026-10-08 eso
produjo 4.311 respuestas 429 y 13 respuestas 406, y obligaba a salir por Apify
Proxy.

Verificado contra el sitio: pidiendo la ficha con un GET pelado, Civitatis
devuelve el HTML server-rendered con TODO lo que necesitamos ya embebido —
incluidos los operadores con razón social, domicilio (con ciudad y país),
email y teléfono. Una petición por actividad en vez de decenas.

Dos detalles que condicionan el diseño:
  - Los LISTADOS (/es/madrid/) devuelven 406 a clientes sin navegador, así que
    las actividades se enumeran por sitemap, no crawleando listados.
  - La moneda se fija con la cookie `currency`; no hace falta tocar el selector.
"""

import asyncio
import gzip
import random
import re
import time
import xml.etree.ElementTree as ET
from collections import deque

import httpx
from bs4 import BeautifulSoup

from .civitatis_comun import (  # noqa: F401
    COLUMNAS,
    MONEDAS_VALIDAS,
    OPERADOR_GENERICO,
    SIN_DATO,
    limpiar_numero,
    limpiar_rating,
    slug_de_url,
)

BASE = "https://www.civitatis.com"
SITEMAP_INDEX = f"{BASE}/sitemap.xml"

# Canario de reserva: actividad grande y estable, para cuando no hay fichas
# previas de las que tomar una. Sin canario no se puede distinguir "catálogo
# dado de baja" de "nos bloquearon", y confundirlos marca miles de actividades
# vivas como inexistentes.
CANARIO_POR_DEFECTO = f"{BASE}/es/madrid/visita-guiada-palacio-real/"

CABECERAS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com/",
}

# Los sitemaps NO toleran el set completo de cabeceras de navegador: con los
# Sec-Fetch-* puestos devuelven 406. Para XML va un juego mínimo.
CABECERAS_XML = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml,*/*;q=0.8",
    "Referer": "https://www.google.com/",
}

RE_ACTIVIDAD = re.compile(r"^https?://(?:www\.)?civitatis\.com/es/[^/?#]+/[^/?#]+/?$")
RE_VIAJEROS = re.compile(r"([\d.,]+)\s*viajeros")
RE_OPINIONES = re.compile(r"([\d.,]+)\s*opiniones")

# Páginas que no son actividades aunque calcen con el patrón de URL.
SLUGS_NO_ACTIVIDAD = {
    "excursiones", "visitas-guiadas", "free-tours", "entradas", "traslados",
    "actividades", "tours", "que-ver", "opiniones", "seguro-viaje",
}


class DeadlineAlcanzado(Exception):
    """Se agotó el tiempo máximo asignado al scraper."""


class BloqueoDetectado(Exception):
    """El canario falla: Civitatis dejó de respondernos, no son fichas de baja."""


# ====================================================================== #
# Enumeración de actividades
# ====================================================================== #

def _descargar(cliente, url, intentos=4, espera_base=15):
    for i in range(intentos):
        try:
            r = cliente.get(url, timeout=45)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 403, 500, 502, 503, 504):
                if i == intentos - 1:
                    return r
                espera = espera_base * (2 ** i)
                print(f"   ⏳ HTTP {r.status_code} en {url} — esperando {espera}s", flush=True)
                time.sleep(espera)
                continue
            return r
        except Exception as e:
            if i == intentos - 1:
                print(f"   ❌ {type(e).__name__} en {url}")
                return None
            time.sleep(espera_base * (2 ** i))
    return None


def _urls_de_xml(texto):
    urls = []
    try:
        raiz = ET.fromstring(texto)
    except ET.ParseError:
        return urls
    for el in raiz.iter():
        if el.tag.endswith("}loc") or el.tag == "loc":
            if el.text:
                urls.append(el.text.strip())
    return urls


def urls_desde_sitemap(limite_sitemaps=None):
    """Recorre sitemap.xml (y sus sub-sitemaps) y devuelve las URLs de /es/."""
    encontradas = set()
    with httpx.Client(headers=CABECERAS_XML, follow_redirects=True) as c:
        r = _descargar(c, SITEMAP_INDEX)
        if r is None or r.status_code != 200:
            estado = r.status_code if r is not None else "sin respuesta"
            print(f"⚠️ No se pudo leer {SITEMAP_INDEX} (HTTP {estado})")
            return []

        hijos = _urls_de_xml(r.text)
        # Del índice sólo interesa el de /es/: los sitemap_images_* traen 75k
        # urls de fotos cada uno y los de otros idiomas son el mismo catálogo.
        sub = [u for u in hijos if ".xml" in u and "images" not in u]
        solo_es = [u for u in sub if re.search(r"sitemap_es\b", u)]
        if solo_es:
            sub = solo_es
        if limite_sitemaps:
            sub = sub[:limite_sitemaps]
        print(f"🗺️  sitemap index: {len(sub)} sub-sitemaps útiles")

        if not sub:  # el index ya traía URLs directas
            encontradas.update(hijos)
        for i, s in enumerate(sub, 1):
            rs = _descargar(c, s)
            if rs is None or rs.status_code != 200:
                continue
            texto = rs.text
            if s.endswith(".gz"):
                try:
                    texto = gzip.decompress(rs.content).decode("utf-8", "replace")
                except Exception:
                    pass
            nuevas = _urls_de_xml(texto)
            encontradas.update(nuevas)
            print(f"   [{i}/{len(sub)}] {s.split('/')[-1]}: {len(nuevas)} urls", flush=True)
            time.sleep(1)

    return sorted(u for u in encontradas if "/es/" in u)


def urls_desde_archivo(ruta):
    """Respaldo local: civitatis_baseline.txt generado por sitemap_civitatis.py."""
    urls = set()
    try:
        with open(ruta, encoding="utf-8", errors="replace") as f:
            for linea in f:
                linea = linea.strip()
                if linea.startswith("http"):
                    urls.add(linea)
    except OSError as e:
        print(f"⚠️ No se pudo leer {ruta}: {e}")
    return sorted(urls)


def filtrar_actividades(urls):
    """Deja sólo /es/<destino>/<actividad>/ descartando páginas de categoría."""
    salida = []
    for u in urls:
        if not RE_ACTIVIDAD.match(u):
            continue
        dest, act = slug_de_url(u)
        # Se descartan por los dos lados: /es/madrid/excursiones/ (categoría)
        # y /es/seguro-viaje/loquesea/ (producto que no es una actividad).
        if not act or act in SLUGS_NO_ACTIVIDAD or not dest or dest in SLUGS_NO_ACTIVIDAD:
            continue
        salida.append(u.rstrip("/") + "/")
    return sorted(set(salida))


# ====================================================================== #
# Parseo de la ficha
# ====================================================================== #

def _texto(sopa, selector):
    el = sopa.select_one(selector)
    return el.get_text(" ", strip=True) if el else None


def parsear_operadores(sopa):
    """
    Bloque de operadores del FAQ. El contenido está en spans con clase js-hide
    (sólo ocultos por CSS), así que se lee directo del DOM sin interactuar.
    """
    operadores = []
    for enlace in sopa.select("a.o-answers-provider__name"):
        datos = {k: SIN_DATO for k in ("operador", "email", "telefono", "direccion")}
        datos["operador"] = enlace.get_text(" ", strip=True) or SIN_DATO

        destino_id = enlace.get("data-dropdow-target")
        if destino_id:
            cont = sopa.find(id=destino_id)
            if cont:
                for linea in cont.select(".o-answers-provider__info"):
                    txt = " ".join(linea.get_text(" ", strip=True).split())
                    low = txt.lower()
                    if low.startswith("correo electrónico:"):
                        datos["email"] = txt.split(":", 1)[1].strip() or SIN_DATO
                    elif low.startswith("teléfono:"):
                        datos["telefono"] = txt.split(":", 1)[1].strip() or SIN_DATO
                    elif "domicilio" in low or "razón social" in low:
                        info = (txt.replace("Domicilio Social:", "")
                                   .replace("Razón social:", "").strip())
                        actual = datos["direccion"]
                        datos["direccion"] = info if actual == SIN_DATO else f"{actual} | {info}"
        operadores.append(datos)

    if not operadores:
        operadores.append({
            "operador": OPERADOR_GENERICO, "email": SIN_DATO,
            "telefono": SIN_DATO, "direccion": SIN_DATO,
        })
    return operadores


def parsear_ficha(html, url, pais, destino, moneda, fecha_scan,
                  incluir_descripcion=True, max_desc_chars=5000):
    """HTML de una ficha -> lista de filas listas para BigQuery (una por operador)."""
    sopa = BeautifulSoup(html, "html.parser")

    h1 = sopa.find("h1")
    actividad = h1.get_text(" ", strip=True) if h1 else None
    if not actividad:
        return []

    # '7.977 opiniones | 193.083 viajeros' viene todo en el mismo elemento.
    resumen = _texto(sopa, ".a-text--rating-total") or ""
    m_op = RE_OPINIONES.search(resumen)
    m_vi = RE_VIAJEROS.search(resumen)

    descripcion = ""
    if incluir_descripcion:
        bruto = _texto(sopa, "#descripcion") or ""
        descripcion = " ".join(bruto.replace("\n", " || ").split())[:max_desc_chars]

    base = {
        "pais": pais,
        "destino": destino,
        "actividad": actividad,
        "url_actividad": url,
        "descripcion": descripcion,
        "precio_real": limpiar_numero(_texto(sopa, ".m-activity-price__text"), "float"),
        "opiniones": limpiar_numero(m_op.group(1) if m_op else None, "int"),
        "viajeros": limpiar_numero(m_vi.group(1) if m_vi else None, "int"),
        "rating": limpiar_rating(_texto(sopa, "#rating-activity-view")),
        "moneda": moneda,
        "fecha_scan": fecha_scan,
    }
    return [{**base, **op} for op in parsear_operadores(sopa)]


# ====================================================================== #
# Scraper
# ====================================================================== #

class CivitatisOperadoresHTTP:
    def __init__(self, moneda="USD", fecha_scan=None, concurrencia=2,
                 pausa=1.5, incluir_descripcion=True, reintentos=4,
                 backoff_base=20, timeout=45, canario=CANARIO_POR_DEFECTO,
                 ventana=40, umbral_406=0.7):
        if moneda not in MONEDAS_VALIDAS:
            raise RuntimeError(f"Moneda '{moneda}' no ofrecida por Civitatis. "
                               f"Válidas: {sorted(MONEDAS_VALIDAS)}")
        self.moneda = moneda
        self.fecha_scan = fecha_scan
        self.concurrencia = max(1, concurrencia)
        self.pausa = pausa
        self.incluir_descripcion = incluir_descripcion
        self.reintentos = reintentos
        self.backoff_base = backoff_base
        self.timeout = timeout
        # Canario: una URL que ya scrapeamos bien. Si ella tambien devuelve 406
        # es que nos bloquearon; si responde 200, los 406 son fichas de baja.
        self.canario = canario or CANARIO_POR_DEFECTO
        self.ventana = ventana
        self.umbral_406 = umbral_406
        self._ultimos = deque(maxlen=ventana)
        self.bloqueado = False
        self._lock_canario = asyncio.Lock()

        self._deadline = None
        self.detenido_por_tiempo = False
        self.stats = {
            "fichas_ok": 0, "fichas_error": 0, "fichas_404": 0,
            "no_disponibles": 0, "sin_operador": 0, "filas": 0,
            "http_bloqueos": 0, "canario_ok": 0, "canario_fallo": 0,
        }

    def _chequear_deadline(self):
        if self._deadline is not None and time.monotonic() > self._deadline:
            self.detenido_por_tiempo = True
            raise DeadlineAlcanzado()

    async def _canario_responde(self, cliente):
        """True si una URL que sabemos buena sigue devolviendo 200."""
        if not self.canario:
            # Nunca se asume salud: sin canario no hay forma de verificar.
            return False
        try:
            r = await cliente.get(self.canario, timeout=self.timeout)
            ok = r.status_code == 200
        except Exception:
            ok = False
        self.stats["canario_ok" if ok else "canario_fallo"] += 1
        return ok

    async def _registrar_resultado(self, cliente, fue_406):
        """
        Lleva la ventana móvil de 406. Si se dispara el umbral, consulta el
        canario: si él también falla no son fichas de baja sino bloqueo, y se
        aborta el shard en vez de marcar miles de actividades como inexistentes.
        """
        self._ultimos.append(bool(fue_406))
        if len(self._ultimos) < self._ultimos.maxlen:
            return
        if sum(self._ultimos) / len(self._ultimos) < self.umbral_406:
            return

        async with self._lock_canario:
            if self.bloqueado:
                raise BloqueoDetectado()
            ratio = sum(self._ultimos) / len(self._ultimos)
            print(f"   🐤 {ratio:.0%} de 406 en las últimas {len(self._ultimos)} "
                  f"fichas; probando el canario…", flush=True)
            if await self._canario_responde(cliente):
                print("   🐤 canario OK: son fichas dadas de baja, se sigue.", flush=True)
                self._ultimos.clear()
                return
            self.bloqueado = True
            print("   🛑 canario en 406: estamos bloqueados, no son bajas. "
                  "Se corta el shard y se sube lo pendiente.", flush=True)
            raise BloqueoDetectado()

    async def _pedir(self, cliente, url, saltos=0):
        """
        GET con backoff. Devuelve (status, texto) o (status, None).

        Una actividad dada de baja responde 301 hacia la página de su destino,
        y esa página devuelve 406 porque los listados rechazan clientes sin
        navegador. Si se siguiera el redirect se vería un 406 indistinguible de
        un bloqueo y se gastarían ~140s de backoff por URL muerta. Por eso se
        resuelven los redirects a mano: a destino => la actividad ya no existe.
        """
        for i in range(self.reintentos):
            self._chequear_deadline()
            try:
                r = await cliente.get(url, timeout=self.timeout)
            except Exception:
                if i == self.reintentos - 1:
                    return None, None
                await asyncio.sleep(self.backoff_base * (2 ** i))
                continue

            if r.status_code == 200:
                return 200, r.text
            if r.status_code == 406:
                # 406 NO es un bloqueo contra el scraper: un Chromium real
                # recibe lo mismo en esas fichas. Es "actividad no disponible".
                # Reintentarlo con backoff (20+40+80s) era puro desperdicio y
                # es lo que dejó un run entero 117 min sin producir una fila.
                # Si en realidad estamos bloqueados lo detecta el canario.
                return 406, None
            if r.status_code in (301, 302, 307, 308):
                destino = r.headers.get("location") or ""
                if destino.startswith("/"):
                    destino = BASE + destino
                _, act = slug_de_url(destino)
                if act and saltos < 2:
                    # Redirige a otra actividad (renombrada): se sigue un salto.
                    return await self._pedir(cliente, destino, saltos + 1)
                return 410, None  # redirige al destino => actividad dada de baja
            if r.status_code in (404, 410):
                return r.status_code, None
            if r.status_code in (429, 403, 500, 502, 503, 504):
                self.stats["http_bloqueos"] += 1
                if i == self.reintentos - 1:
                    return r.status_code, None
                espera = self.backoff_base * (2 ** i) + random.uniform(0, 5)
                print(f"   ⏳ HTTP {r.status_code} en {url} — esperando {espera:.0f}s", flush=True)
                await asyncio.sleep(espera)
                continue
            return r.status_code, None
        return None, None

    async def run(self, trabajos, on_rows, urls_omitidas=None, deadline=None):
        """
        trabajos      : lista de dicts {url, pais, destino}
        on_rows       : coroutine on_rows(filas) que recibe cada lote
        urls_omitidas : set de url ya cargadas (reanudación)
        """
        self._deadline = deadline
        omitidas = set(urls_omitidas or ())
        pendientes = [t for t in trabajos if t["url"] not in omitidas]
        total = len(pendientes)
        print(f"📋 Fichas asignadas: {len(trabajos)} | ya cargadas: "
              f"{len(trabajos) - total} | a scrapear: {total}", flush=True)

        sem = asyncio.Semaphore(self.concurrencia)
        hechas = {"n": 0}
        limites = httpx.Limits(max_connections=self.concurrencia,
                               max_keepalive_connections=self.concurrencia)

        async with httpx.AsyncClient(
            headers=CABECERAS,
            cookies={"currency": self.moneda, "civ_lang": "es"},
            follow_redirects=False, limits=limites,
        ) as cliente:

            async def una(trabajo):
                async with sem:
                    if self.detenido_por_tiempo or self.bloqueado:
                        return
                    try:
                        status, html = await self._pedir(cliente, trabajo["url"])
                    except DeadlineAlcanzado:
                        return

                    if status == 406:
                        # Ficha no disponible (un navegador real ve lo mismo).
                        self.stats["no_disponibles"] += 1
                        try:
                            await self._registrar_resultado(cliente, True)
                        except BloqueoDetectado:
                            return
                    else:
                        try:
                            await self._registrar_resultado(cliente, False)
                        except BloqueoDetectado:
                            return

                    if status == 200 and html:
                        filas = parsear_ficha(
                            html, trabajo["url"], trabajo["pais"], trabajo["destino"],
                            self.moneda, self.fecha_scan, self.incluir_descripcion,
                        )
                        if filas:
                            self.stats["fichas_ok"] += 1
                            if filas[0]["operador"] == OPERADOR_GENERICO:
                                self.stats["sin_operador"] += 1
                            self.stats["filas"] += len(filas)
                            await on_rows(filas)
                        else:
                            self.stats["fichas_error"] += 1
                    elif status in (404, 410):
                        self.stats["fichas_404"] += 1
                    elif status == 406:
                        pass  # ya contabilizada arriba
                    else:
                        # No se emite nada: la próxima corrida la reintenta.
                        self.stats["fichas_error"] += 1

                    if self.pausa:
                        await asyncio.sleep(self.pausa)

                    hechas["n"] += 1
                    if hechas["n"] % 50 == 0 or hechas["n"] == total:
                        print(f"📈 {hechas['n']}/{total} fichas | filas={self.stats['filas']} "
                              f"| ok={self.stats['fichas_ok']} err={self.stats['fichas_error']} "
                              f"404={self.stats['fichas_404']} no_disp={self.stats['no_disponibles']} "
                              f"429={self.stats['http_bloqueos']}", flush=True)

            await asyncio.gather(*(una(t) for t in pendientes), return_exceptions=True)

        return self.stats
