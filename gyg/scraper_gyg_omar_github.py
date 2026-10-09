"""
Scraper de reviews de GetYourGuide -> BigQuery (datatur.supply.reviews_gyg_omar).

Reescrito tras la corrida del 2026-10-08, que dejó 7 de los 20 destinos sin
cargar. Qué cambió y por qué:

  1. SIN APIFY. El proxy ahora es opcional: sólo se usa si hay
     APIFY_PROXY_PASSWORD en el entorno. Por defecto sale por la IP de la
     máquina. Si se agotan los créditos, el scraper sigue funcionando.

  2. CONTRATO NUEVO DE LA API. GYG migró activity-details-page a un formato
     server-driven UI: ya no existen nodos `{"type": "review", "author": …}`.
     El parser viejo devolvía 0 reviews para TODA actividad. Ahora los datos
     salen de los trackingEvent (`target == "activity-review-card"`), que traen
     review_id, review_rating, review_date, countryCode y traveler_type. Se
     mantiene el parser viejo como respaldo por si GYG revierte.

  3. CARGA INCREMENTAL + REANUDACIÓN. Antes se acumulaba todo en memoria y se
     subía una sola vez al final: si el job moría, se perdía el destino entero.
     Ahora sube por lotes y, al relanzar con el mismo --fecha-scraper, saltea
     los tour_id que ya están en BigQuery.

  4. REINTENTOS DE VERDAD. Antes un 429/5xx se trataba como "terminé": truncaba
     las reviews de esa actividad en silencio y la daba por completa. Ahora
     reintenta con backoff y, si no lo logra, deja la actividad SIN marcar para
     que la próxima corrida la rehaga.

Uso:
    python gyg/scraper_gyg_omar_github.py --destino roma-l33
    python gyg/scraper_gyg_omar_github.py --listar-faltantes
    python gyg/scraper_gyg_omar_github.py --destino roma-l33 --fecha-scraper 2026-10-08
"""

import argparse
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pandas as pd
import requests

TABLA_DESTINO = "datatur.supply.reviews_gyg_omar"
FECHA_DESDE_DEFAULT = "2025-05-01"

URL_API = ("https://travelers-api.getyourguide.com/user-interface/"
           "activity-details-page/blocks?ranking_uuid=8db3d7f9-ae97-4e8e-9782-086c43dd5f1b")

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://www.getyourguide.com",
    "Referer": "https://www.getyourguide.com/",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"),
    "accept-currency": "EUR",
    "accept-language": "es-ES",
    "geo-ip-country": "ES",
    "partner-id": "CD951",
    "visitor-id": "F97X0158YNG8999WZEDF645USNKBJAZD",
    "visitor-platform": "desktop",
    "x-gyg-app-type": "Web",
}

COLUMNAS = [
    "review_id", "tour_id", "pais", "destino", "actividad",
    "rating", "fecha_review", "pais_usuario", "url_actividad", "fecha_scraper",
]

# La API corta alrededor del offset 320 por combinación de filtros. Cuando se
# llega a ese tope sin alcanzar fecha_desde, se parte la consulta por estrellas.
TOPE_OFFSET_API = 320

# Nombres de país que ya existen en la tabla y no coinciden con los que da
# babel para ese ISO. Se respeta la grafía que ya está cargada para que la
# columna quede homogénea entre los 13 destinos viejos y los 7 nuevos.
OVERRIDE_PAIS = {
    "BA": "Bosnia Herzegovina",
    "BD": "Bangladesh",
    "BM": "Bermuda",
    "BQ": "Bonaire",
    "CG": "Brazzaville",
    "CI": "Costa de Marfil",
    "CZ": "República Checa",
    "GG": "Guernsey",
    "HK": "Hong Kong",
    "IQ": "Iraq",
    "MO": "Macao",
    "MR": "Mauritanie",
    "SX": "Sint Marteen",
    "SZ": "Suazilandia",
    "VC": "San Vicente y Granadinas",
    "VI": "Islas Vírgenes de Los Estados Unidos",
}

_iso_a_nombre = None


def nombre_pais(iso):
    """ISO-3166 alfa-2 -> nombre en español, con la grafía que usa la tabla."""
    global _iso_a_nombre
    if not iso:
        return "Desconocido"
    if _iso_a_nombre is None:
        try:
            from babel import Locale
            _iso_a_nombre = {k: v for k, v in Locale("es").territories.items() if len(k) == 2}
        except ImportError:
            print("⚠️ Sin babel instalado: pais_usuario quedará con el código ISO.")
            _iso_a_nombre = {}
    iso = iso.upper()
    return OVERRIDE_PAIS.get(iso) or _iso_a_nombre.get(iso) or iso


# ====================================================================== #
# HTTP
# ====================================================================== #

def construir_proxies():
    """Apify sólo si está configurado. Sin la variable, se sale directo."""
    password = os.environ.get("APIFY_PROXY_PASSWORD")
    if not password:
        return None
    grupos = os.environ.get("APIFY_PROXY_GROUPS", "RESIDENTIAL")
    sesion = os.urandom(8).hex()
    url = f"http://groups-{grupos},session-{sesion}:{password}@proxy.apify.com:8000"
    return {"http": url, "https": url}


class ApiGYG:
    def __init__(self, pausa_min=0.8, pausa_max=1.5, reintentos=5, timeout=25):
        self.pausa_min = pausa_min
        self.pausa_max = pausa_max
        self.reintentos = reintentos
        self.timeout = timeout
        self.usa_proxy = bool(os.environ.get("APIFY_PROXY_PASSWORD"))
        self.sesion = requests.Session()
        self.bloqueos = 0
        self.fallos = 0
        self._lock = threading.Lock()

    def pedir(self, tour_id, offset, orden, filtros):
        cuerpo = json.dumps({"payload": {
            "activityId": tour_id,
            "templateName": "ActivityDetails",
            "contentIdentifier": "next-reviews-page",
            "additionalDetailsSelectedLanguage": "es-ES",
            "reviewsOffset": offset,
            "reviewsLimit": 20,
            "selectedReviewsSortingOrder": orden,
            "selectedReviewsFilters": filtros,
            "participantsLanguage": "es-ES",
            "reviewsExperiments": [
                {"key": "rvw-display-traveler-type-in-reviews", "isEnabled": False}
            ],
        }}, separators=(",", ":"))

        for intento in range(1, self.reintentos + 1):
            try:
                resp = self.sesion.post(
                    URL_API, headers=HEADERS, data=cuerpo,
                    timeout=self.timeout, proxies=construir_proxies() if self.usa_proxy else None,
                )
            except requests.RequestException as e:
                if intento == self.reintentos:
                    with self._lock:
                        self.fallos += 1
                    raise
                time.sleep(min(60, 4 * (2 ** (intento - 1))))
                continue

            if resp.status_code == 200:
                time.sleep(random.uniform(self.pausa_min, self.pausa_max))
                return resp.json()

            if resp.status_code == 404:
                return None  # actividad dada de baja

            if resp.status_code in (403, 429, 500, 502, 503, 504):
                with self._lock:
                    self.bloqueos += 1
                if intento == self.reintentos:
                    raise RuntimeError(f"HTTP {resp.status_code} tras {self.reintentos} intentos")
                espera = min(90, 5 * (2 ** (intento - 1)))
                print(f"  ⏳ HTTP {resp.status_code} en tour {tour_id} (offset {offset}) "
                      f"— esperando {espera}s", flush=True)
                time.sleep(espera)
                continue

            raise RuntimeError(f"HTTP {resp.status_code} inesperado en tour {tour_id}")

        raise RuntimeError("sin respuesta")


# ====================================================================== #
# Parsers: contrato nuevo (SDUI) con respaldo al viejo
# ====================================================================== #

def _parsear_contrato_nuevo(datos):
    """
    Desde 2026-10 las reviews vienen como bloques de UI. Los únicos campos
    estructurados están en los trackingEvent de cada tarjeta de review.
    """
    por_id = {}

    def recorrer(x):
        if isinstance(x, dict):
            props = x.get("properties")
            if (isinstance(props, dict)
                    and props.get("target") == "activity-review-card"
                    and props.get("review_id")):
                rid = props["review_id"]
                # Una misma review aparece en varios eventos; gana el más completo.
                if rid not in por_id or len(props) > len(por_id[rid]):
                    por_id[rid] = props
            for v in x.values():
                recorrer(v)
        elif isinstance(x, list):
            for v in x:
                recorrer(v)

    recorrer(datos)

    reviews = []
    for p in por_id.values():
        fecha = _fecha(p.get("review_date"))
        if fecha is None:
            continue
        reviews.append({
            "review_id": p["review_id"],
            "rating": p.get("review_rating"),
            "fecha": fecha,
            "pais_usuario": nombre_pais(p.get("countryCode")),
        })
    return reviews


def _parsear_contrato_viejo(datos):
    """Formato anterior a octubre de 2026. Se deja como respaldo."""
    crudas = []

    def recorrer(x):
        if isinstance(x, dict):
            if x.get("type") == "review" and "author" in x:
                crudas.append(x)
            else:
                for v in x.values():
                    recorrer(v)
        elif isinstance(x, list):
            for v in x:
                recorrer(v)

    recorrer(datos)

    reviews = []
    for r in crudas:
        props = r.get("onImpressionTrackingEvent", {}).get("properties", {})
        fecha = _fecha(props.get("review_date"))
        if fecha is None:
            continue
        autor = r.get("author", {}).get("title", {}).get("text", "")
        autor = autor.replace(" – ", " - ").replace(" — ", " - ")
        pais = autor.split(" - ")[-1].strip() if " - " in autor else "Desconocido"
        reviews.append({
            "review_id": r.get("reviewId"),
            "rating": r.get("rating"),
            "fecha": fecha,
            "pais_usuario": pais or "Desconocido",
        })
    return reviews


def parsear_reviews(datos):
    if not datos:
        return []
    reviews = _parsear_contrato_nuevo(datos)
    if reviews:
        return reviews
    return _parsear_contrato_viejo(datos)


def _fecha(texto):
    if not texto:
        return None
    try:
        return datetime.strptime(str(texto).split("T")[0], "%Y-%m-%d").date()
    except ValueError:
        return None


# ====================================================================== #
# Recorrido de una actividad
# ====================================================================== #

def paginar(api, tour_id, orden, filtros, vistas, fecha_desde):
    """
    Pagina en una dirección con los filtros dados.
    Devuelve (reviews, tope_alcanzado). tope_alcanzado=True significa que la
    API cortó cerca del offset 320 sin llegar al límite de fechas: puede haber
    reviews sin capturar y hay que escalar a filtros por estrella.
    """
    resultados = []
    offset = 0
    while True:
        datos = api.pedir(tour_id, offset, orden, filtros)
        if datos is None:
            return resultados, False

        reviews = parsear_reviews(datos)
        if not reviews:
            # Fin real de la lista: el tope de la API se detecta más abajo, al
            # cruzar TOPE_OFFSET_API. La versión anterior marcaba tope aquí y
            # escalaba a los 5 filtros por estrella en CADA actividad chica.
            return resultados, False

        detener = False
        for rev in reviews:
            rid = rev["review_id"]
            if orden == "date_desc":
                if rev["fecha"] < fecha_desde:
                    detener = True
                    break
                if rid not in vistas:
                    vistas.add(rid)
                    resultados.append(rev)
            else:  # date_asc
                if rid in vistas:
                    detener = True  # reconectamos con territorio ya cubierto
                    break
                if rev["fecha"] >= fecha_desde:
                    vistas.add(rid)
                    resultados.append(rev)

        if detener:
            return resultados, False

        offset += 20
        if offset >= TOPE_OFFSET_API:
            return resultados, True


def reviews_de_actividad(api, tour_id, fecha_desde):
    """
    Todas las reviews en rango de una actividad. Si choca con el tope de la API
    antes de llegar a fecha_desde, parte por calificación (1 a 5 estrellas) y,
    si una partición también se topa, agrega un pase inverso.
    """
    vistas = set()
    resultados, tope = paginar(api, tour_id, "date_desc", {}, vistas, fecha_desde)
    if not tope:
        return resultados, False, False

    hueco = False
    for estrella in (1, 2, 3, 4, 5):
        sub, tope_e = paginar(api, tour_id, "date_desc", {"ratings": [estrella]}, vistas, fecha_desde)
        resultados.extend(sub)
        if tope_e:
            sub_asc, tope_asc = paginar(api, tour_id, "date_asc", {"ratings": [estrella]},
                                        vistas, fecha_desde)
            resultados.extend(sub_asc)
            if tope_asc:
                hueco = True
    return resultados, True, hueco


def limpiar_destino(ciudad_id):
    """'nueva-york-l59' -> 'Nueva York'. Igual que la corrida anterior."""
    return re.sub(r"-l\d+$", "", str(ciudad_id)).replace("-", " ").title()


# ====================================================================== #
# BigQuery
# ====================================================================== #

class CargadorBQ:
    def __init__(self, table_id, dry_run=False):
        from google.cloud import bigquery

        self.dry_run = dry_run

        self.bigquery = bigquery
        self.table_id = table_id
        self.client = bigquery.Client(project=table_id.split(".")[0])
        self.tabla = self.client.get_table(table_id)
        self.schema = list(self.tabla.schema)
        self.nombres = [f.name for f in self.schema]
        self.filas_subidas = 0
        self.lotes_subidos = 0
        self._lock = threading.Lock()

    def alinear(self, filas):
        df = pd.DataFrame(filas)
        for f in self.schema:
            if f.name not in df.columns:
                df[f.name] = None
            tipo = f.field_type.upper()
            if tipo in ("INTEGER", "INT64"):
                df[f.name] = pd.to_numeric(df[f.name], errors="coerce").astype("Int64")
            elif tipo in ("FLOAT", "FLOAT64"):
                df[f.name] = pd.to_numeric(df[f.name], errors="coerce").astype("float64")
            else:
                df[f.name] = df[f.name].astype("string")
        return df[self.nombres]

    def subir(self, filas, reintentos=5):
        from google.api_core.exceptions import (
            InternalServerError, ServiceUnavailable, TooManyRequests,
        )

        if not filas:
            return 0
        df = self.alinear(filas)

        if self.dry_run:
            with self._lock:
                self.filas_subidas += len(df)
                self.lotes_subidos += 1
                n, acum = self.lotes_subidos, self.filas_subidas
            print(f"🧪 [dry-run] Lote #{n}: {len(df):,} reviews NO subidas (acumulado: {acum:,})",
                  flush=True)
            return len(df)

        cfg = self.bigquery.LoadJobConfig(schema=self.schema, write_disposition="WRITE_APPEND")
        for intento in range(1, reintentos + 1):
            try:
                self.client.load_table_from_dataframe(df, self.table_id, job_config=cfg).result()
                with self._lock:
                    self.filas_subidas += len(df)
                    self.lotes_subidos += 1
                    n, acum = self.lotes_subidos, self.filas_subidas
                print(f"⬆️  Lote #{n}: {len(df):,} reviews a BigQuery (acumulado: {acum:,})", flush=True)
                return len(df)
            except (TooManyRequests, ServiceUnavailable, InternalServerError) as e:
                if intento == reintentos:
                    raise
                espera = 5 * (2 ** (intento - 1))
                print(f"⚠️ BigQuery ocupado ({type(e).__name__}). Reintento {intento} en {espera}s…")
                time.sleep(espera)
        return 0

    def tours_ya_cargados(self, fecha_scraper, destino):
        sql = (f"SELECT DISTINCT tour_id FROM `{self.table_id}` "
               "WHERE fecha_scraper = @fecha AND destino = @destino AND tour_id IS NOT NULL")
        cfg = self.bigquery.QueryJobConfig(query_parameters=[
            self.bigquery.ScalarQueryParameter("fecha", "STRING", fecha_scraper),
            self.bigquery.ScalarQueryParameter("destino", "STRING", destino),
        ])
        try:
            return {r["tour_id"] for r in self.client.query(sql, job_config=cfg).result()}
        except Exception as e:
            print(f"⚠️ No se pudo leer el progreso previo ({type(e).__name__}): se parte de cero.")
            return set()

    def destinos_con_datos(self, fecha_scraper):
        sql = (f"SELECT destino, COUNT(*) filas, COUNT(DISTINCT tour_id) tours "
               f"FROM `{self.table_id}` WHERE fecha_scraper = @fecha GROUP BY 1")
        cfg = self.bigquery.QueryJobConfig(query_parameters=[
            self.bigquery.ScalarQueryParameter("fecha", "STRING", fecha_scraper)])
        return {r["destino"]: (r["filas"], r["tours"]) for r in self.client.query(sql, job_config=cfg).result()}


# ====================================================================== #
# Orquestación de un destino
# ====================================================================== #

def correr_destino(args):
    slug = args.destino
    destino = limpiar_destino(slug)
    fecha_desde = datetime.strptime(args.fecha_desde, "%Y-%m-%d").date()
    fecha_scraper = args.fecha_scraper or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    archivo = args.csv_tours or f"tours_urls_{slug}.csv"
    if not os.path.exists(archivo):
        print(f"❌ No se encontró {archivo}. Corré primero gyg/gyg_sitemap_omar_github.py")
        return 1

    df_tours = pd.read_csv(archivo, sep=";")
    if df_tours.empty:
        print(f"⚠️ {archivo} está vacío. Nada que hacer.")
        return 0

    cargador = CargadorBQ(args.tabla, dry_run=args.dry_run)
    ya = set() if args.no_reanudar else cargador.tours_ya_cargados(fecha_scraper, destino)

    pendientes = [row for _, row in df_tours.iterrows() if int(row["tour_id"]) not in ya]

    api = ApiGYG(pausa_min=args.pausa_min, pausa_max=args.pausa_max)
    print(f"\n🚀 {destino} ({slug}) | reviews desde {fecha_desde} | fecha_scraper={fecha_scraper}")
    print(f"   {len(df_tours)} actividades | {len(ya)} ya cargadas | {len(pendientes)} pendientes")
    print(f"   proxy: {'Apify' if api.usa_proxy else 'NINGUNO (IP directa)'}{' | DRY-RUN' if args.dry_run else ''} | "
          f"concurrencia={args.concurrencia} | lote={args.batch_size} | máx {args.max_minutos} min\n")

    if not pendientes:
        print("✅ Nada pendiente para este destino.")
        return 0

    deadline = time.monotonic() + args.max_minutos * 60
    buffer = []
    lock = threading.Lock()
    estado = {"hechas": 0, "reviews": 0, "escalados": 0, "huecos": 0, "errores": 0, "sin_tiempo": False}
    total = len(pendientes)

    def flush(forzar=False):
        with lock:
            if not buffer or (not forzar and len(buffer) < args.batch_size):
                return
            lote, buffer[:] = list(buffer), []
        cargador.subir(lote)

    def procesar(row):
        if time.monotonic() > deadline:
            estado["sin_tiempo"] = True
            return
        tour_id = int(row["tour_id"])
        try:
            revs, escalo, hueco = reviews_de_actividad(api, tour_id, fecha_desde)
        except Exception as e:
            with lock:
                estado["errores"] += 1
            print(f"  ❌ tour {tour_id}: {type(e).__name__}: {e} — queda pendiente", flush=True)
            return

        filas = [{
            "review_id": r["review_id"],
            "tour_id": tour_id,
            "pais": str(row["pais"]),
            "destino": destino,
            "actividad": str(row["titulo_referencia"]),
            "rating": r["rating"],
            "fecha_review": r["fecha"].strftime("%Y-%m-%d"),
            "pais_usuario": r["pais_usuario"],
            "url_actividad": str(row["url"]),
            "fecha_scraper": fecha_scraper,
        } for r in revs]

        with lock:
            buffer.extend(filas)
            estado["hechas"] += 1
            estado["reviews"] += len(filas)
            if escalo:
                estado["escalados"] += 1
            if hueco:
                estado["huecos"] += 1
            hechas, acum = estado["hechas"], estado["reviews"]
            listo = len(buffer) >= args.batch_size

        if listo:
            flush()
        if hechas % 25 == 0 or hechas == total:
            restante = max(0, int(deadline - time.monotonic()))
            print(f"📈 {hechas}/{total} actividades | {acum:,} reviews | "
                  f"bloqueos={api.bloqueos} errores={estado['errores']} | "
                  f"quedan {restante // 60}min", flush=True)

    try:
        with ThreadPoolExecutor(max_workers=args.concurrencia) as pool:
            list(pool.map(procesar, pendientes))
    finally:
        try:
            flush(forzar=True)
        except Exception as e:
            print(f"❌ No se pudo subir el último lote: {type(e).__name__}: {e}")

    sin_procesar = total - estado["hechas"]
    icono = "✅" if sin_procesar == 0 and estado["errores"] == 0 else "⏱️"
    print(f"\n{icono} {destino}: {cargador.filas_subidas:,} reviews subidas en "
          f"{cargador.lotes_subidos} lotes | {estado['hechas']}/{total} actividades")
    if estado["escalados"]:
        print(f"   ℹ️ {estado['escalados']} actividades necesitaron partir por estrellas "
              f"(tope de ~{TOPE_OFFSET_API} de la API); {estado['huecos']} con hueco residual.")
    if sin_procesar or estado["errores"]:
        print(f"   ⏱️ Quedaron {sin_procesar + estado['errores']} actividades sin cargar. "
              f"Relanzá con --fecha-scraper {fecha_scraper} y sigue donde quedó.")

    resumen = os.environ.get("GITHUB_STEP_SUMMARY")
    if resumen:
        with open(resumen, "a", encoding="utf-8") as f:
            f.write(f"## {icono} {destino}\n\n| Métrica | Valor |\n|---|---|\n")
            f.write(f"| fecha_scraper | `{fecha_scraper}` |\n")
            f.write(f"| Actividades | {estado['hechas']}/{total} |\n")
            f.write(f"| Reviews subidas | {cargador.filas_subidas:,} |\n")
            f.write(f"| Lotes | {cargador.lotes_subidos} |\n")
            f.write(f"| Bloqueos HTTP | {api.bloqueos} |\n")
            f.write(f"| Errores | {estado['errores']} |\n")
            if sin_procesar or estado["errores"]:
                f.write(f"\n> ⏱️ Relanzá con `fecha_scraper = {fecha_scraper}` para continuar.\n")

    return 0


def listar_faltantes(args):
    from gyg.gyg_sitemap_omar_github import DESTINOS

    fecha = args.fecha_scraper or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cargador = CargadorBQ(args.tabla)
    con_datos = cargador.destinos_con_datos(fecha)

    print(f"\n📊 {args.tabla} — fecha_scraper = {fecha}\n")
    print(f"{'Destino':<16} {'Slug':<18} {'Reviews':>10} {'Tours':>7}  Estado")
    print("-" * 68)
    faltan = []
    for slug in DESTINOS:
        destino = limpiar_destino(slug)
        filas, tours = con_datos.get(destino, (0, 0))
        if filas == 0:
            faltan.append(slug)
        print(f"{destino:<16} {slug:<18} {filas:>10,} {tours:>7,}  "
              f"{'❌ FALTA' if filas == 0 else '✅'}")
    print(f"\n{len(DESTINOS) - len(faltan)}/{len(DESTINOS)} destinos con datos.")
    if faltan:
        print(f"\nFaltan {len(faltan)}: {' '.join(faltan)}")
        print("\nPara completarlos:")
        print(f"  for d in {' '.join(faltan)}; do \\")
        print(f"    python gyg/scraper_gyg_omar_github.py --destino $d --fecha-scraper {fecha}; done")
    return 0


def parsear_args(argv):
    p = argparse.ArgumentParser(description="Reviews de GetYourGuide -> BigQuery (sin Apify)")
    p.add_argument("destino_posicional", nargs="?", default=None,
                   help="Slug del destino (compatibilidad con la invocación vieja).")
    p.add_argument("--destino", default=None, help="Slug del destino, ej: roma-l33")
    p.add_argument("--fecha-desde", default=FECHA_DESDE_DEFAULT, help="YYYY-MM-DD")
    p.add_argument("--fecha-scraper", default=None,
                   help="YYYY-MM-DD de la corrida. Repetir para REANUDAR.")
    p.add_argument("--tabla", default=TABLA_DESTINO)
    p.add_argument("--csv-tours", default=None, help="CSV de actividades (default tours_urls_<slug>.csv)")
    p.add_argument("--batch-size", type=int, default=2000, help="Reviews por lote a BigQuery.")
    p.add_argument("--concurrencia", type=int, default=4, help="Actividades en paralelo.")
    p.add_argument("--max-minutos", type=int, default=320, help="Corte ordenado antes del timeout.")
    p.add_argument("--pausa-min", type=float, default=0.8)
    p.add_argument("--pausa-max", type=float, default=1.5)
    p.add_argument("--no-reanudar", action="store_true")
    p.add_argument("--dry-run", action="store_true",
                   help="Scrapear sin escribir en BigQuery (prueba).")
    p.add_argument("--listar-faltantes", action="store_true",
                   help="Mostrar qué destinos no tienen datos para --fecha-scraper y salir.")
    args = p.parse_args(argv)
    if args.destino is None:
        args.destino = args.destino_posicional
    return args


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = parsear_args(sys.argv[1:])

    if args.listar_faltantes:
        sys.exit(listar_faltantes(args))

    if not args.destino:
        print("❌ Falta --destino (ej: roma-l33). Usá --listar-faltantes para ver cuáles faltan.")
        sys.exit(2)

    sys.exit(correr_destino(args))
