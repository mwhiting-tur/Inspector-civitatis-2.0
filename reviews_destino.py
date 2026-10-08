"""
Scraper de opiniones (reviews) de Civitatis -> BigQuery.

Recorre un destino por ejecución (un runner por destino en la matriz de GitHub
Actions), lista todas sus actividades, baja las opiniones dentro del rango de
fechas pedido y las sube a BigQuery en LOTES INCREMENTALES (WRITE_APPEND). Si
el job se cae o se queda sin tiempo, lo ya subido queda en la tabla y una nueva
ejecución con el MISMO --fecha-scan reanuda por las actividades que falten.

Uso típico (GitHub Actions, un destino por runner):
    python reviews_destino.py --destino "Madrid, España" --fecha-scan 2026-10-08

Otros modos:
    python reviews_destino.py --listar-destinos      # valida la lista contra el JSON
    python reviews_destino.py --inspeccionar-tabla   # schema de BigQuery
    python reviews_destino.py --resumen-bq --fecha-scan 2026-10-08
"""

import argparse
import asyncio
import json
import os
import sys
import time
import unicodedata
from datetime import date, datetime, timezone

# pandas, google-cloud-bigquery y el driver (playwright) se importan dentro de
# las funciones que los usan: así `--matriz` y `--listar-destinos` corren en el
# job `preparar` del workflow, que no instala esas dependencias.

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TABLA_DESTINO = "datatur.supply.reviews_civitatis_omar"
RUTA_DESTINOS = "destinos_civitatis.json"

# Rango por defecto: 1 de mayo de 2025 -> hoy.
FECHA_DESDE_DEFAULT = "2025-05-01"

# Los 20 destinos del encargo. El nombre debe resolver contra
# destinos_civitatis.json (se valida con --listar-destinos).
DESTINOS_OMAR = [
    # España
    "Madrid, España",
    "Barcelona, España",
    "Sevilla, España",
    "Granada, España",
    "Córdoba, España",
    # Italia
    "Roma, Italia",
    "Florencia, Italia",
    "Venecia, Italia",
    # Portugal
    "Lisboa, Portugal",
    "Oporto, Portugal",
    # Resto de Europa
    "París, Francia",
    "Londres, Reino Unido",
    "Ámsterdam, Países Bajos",
    # USA
    "Nueva York, Estados Unidos",
    "Orlando, Estados Unidos",
    "Los Ángeles, Estados Unidos",
    # APAC
    "Tokio, Japón",
    "Sídney, Australia",
    "Bangkok, Tailandia",
    "Singapur, Singapur",
]

# Schema que se usa SOLO si la tabla todavía no existe. Si ya existe manda
# siempre el schema real de BigQuery (ver BigQueryCargador.alinear).
SCHEMA_POR_DEFECTO = [
    ("review_hash", "STRING"),
    ("pais", "STRING"),
    ("destino", "STRING"),
    ("actividad", "STRING"),
    ("url_actividad", "STRING"),
    ("rating", "FLOAT"),
    ("fecha_review", "DATE"),
    ("nombre_usuario", "STRING"),
    ("pais_usuario", "STRING"),
    ("cod_pais_usuario", "STRING"),
    ("tipo_viajero", "STRING"),
    ("comentario", "STRING"),
    ("fecha_scan", "DATE"),
]

# Columnas que produce el scraper (el driver expone la misma lista; ejecutar()
# avisa si alguna vez dejan de coincidir).
COLUMNAS = [nombre for nombre, _ in SCHEMA_POR_DEFECTO]


# ====================================================================== #
# Destinos
# ====================================================================== #

def _normalizar(texto):
    texto = unicodedata.normalize("NFKD", str(texto or "").lower().strip())
    return texto.encode("ascii", "ignore").decode("ascii")


def cargar_catalogo():
    if not os.path.exists(RUTA_DESTINOS):
        print(f"❌ No se encontró {RUTA_DESTINOS}")
        sys.exit(1)
    with open(RUTA_DESTINOS, "r", encoding="utf-8") as f:
        return json.load(f)


def resolver_destino(entrada, catalogo=None):
    """'Madrid, España' -> dict del catálogo. None si no existe."""
    catalogo = catalogo if catalogo is not None else cargar_catalogo()
    partes = [p.strip() for p in str(entrada).split(",")]
    ciudad = _normalizar(partes[0])
    pais = _normalizar(partes[1]) if len(partes) > 1 else ""

    candidatos = [d for d in catalogo if d.get("url") and d.get("name")]
    for d in candidatos:  # match exacto ciudad + país
        if _normalizar(d["name"]) == ciudad and (not pais or _normalizar(d.get("nameCountry")) == pais):
            return d
    for d in candidatos:  # match laxo
        if ciudad in _normalizar(d["name"]) and (not pais or pais in _normalizar(d.get("nameCountry"))):
            return d
    return None


# ====================================================================== #
# BigQuery
# ====================================================================== #

class BigQueryCargador:
    def __init__(self, table_id, crear_si_no_existe=True):
        from google.cloud import bigquery

        self.bigquery = bigquery
        self.table_id = table_id
        self.client = bigquery.Client(project=table_id.split(".")[0])
        self.tabla = self._obtener_o_crear(crear_si_no_existe)
        self.schema = list(self.tabla.schema)
        self.nombres = [f.name for f in self.schema]
        self.tipos = {f.name: f.field_type.upper() for f in self.schema}
        self.filas_subidas = 0
        self.lotes_subidos = 0

    def _obtener_o_crear(self, crear):
        from google.api_core.exceptions import NotFound

        try:
            return self.client.get_table(self.table_id)
        except NotFound:
            if not crear:
                raise
            print(f"ℹ️ La tabla {self.table_id} no existe. Creándola con el schema por defecto…")
            schema = [self.bigquery.SchemaField(n, t) for n, t in SCHEMA_POR_DEFECTO]
            tabla = self.bigquery.Table(self.table_id, schema=schema)
            tabla.time_partitioning = self.bigquery.TimePartitioning(
                type_=self.bigquery.TimePartitioningType.DAY, field="fecha_scan"
            )
            tabla.clustering_fields = ["destino", "url_actividad"]
            return self.client.create_table(tabla)

    def describir(self):
        print(f"\n📐 Schema de {self.table_id} ({self.tabla.num_rows} filas actuales):")
        for f in self.schema:
            print(f"   - {f.name:<18} {f.field_type:<10} {f.mode}")
        faltan = [c for c in COLUMNAS if c not in self.nombres]
        sobran = [c for c in self.nombres if c not in COLUMNAS]
        if faltan:
            print(f"   ⚠️ El scraper genera columnas que la tabla NO tiene (se descartan): {faltan}")
        if sobran:
            print(f"   ⚠️ La tabla tiene columnas que el scraper NO genera (irán NULL): {sobran}")

    def validar_compatibilidad(self):
        requeridos = [f.name for f in self.schema if f.mode == "REQUIRED" and f.name not in COLUMNAS]
        if requeridos:
            raise SystemExit(
                f"❌ {self.table_id} tiene campos REQUIRED que este scraper no genera: {requeridos}"
            )
        if "fecha_scan" not in self.nombres:
            print("⚠️ La tabla no tiene columna fecha_scan: no se podrá reanudar por fecha.")

    def valor_fecha_scan(self, fecha_iso):
        import pandas as pd

        tipo = self.tipos.get("fecha_scan", "DATE")
        if tipo == "STRING":
            return fecha_iso
        if tipo in ("TIMESTAMP", "DATETIME"):
            return pd.Timestamp(fecha_iso)
        return date.fromisoformat(fecha_iso)

    def alinear(self, filas):
        """Lleva el DataFrame al schema real de la tabla (nombres, orden y tipos)."""
        import pandas as pd

        df = pd.DataFrame(filas)
        for f in self.schema:
            tipo = f.field_type.upper()
            if f.name not in df.columns:
                df[f.name] = None
            col = df[f.name]

            if tipo in ("STRING", "BYTES"):
                df[f.name] = col.astype("object").where(col.notna(), None)
                df[f.name] = df[f.name].map(lambda v: None if v is None else str(v))
            elif tipo in ("INTEGER", "INT64"):
                df[f.name] = pd.to_numeric(col, errors="coerce").astype("Int64")
            elif tipo in ("FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
                df[f.name] = pd.to_numeric(col, errors="coerce").astype("float64")
            elif tipo in ("BOOLEAN", "BOOL"):
                df[f.name] = col.astype("boolean")
            elif tipo == "DATE":
                conv = pd.to_datetime(col, errors="coerce")
                df[f.name] = [None if pd.isna(v) else v.date() for v in conv]
            elif tipo in ("TIMESTAMP", "DATETIME"):
                df[f.name] = pd.to_datetime(col, errors="coerce")

        return df[self.nombres]

    def subir(self, filas, reintentos=5):
        from google.api_core.exceptions import (
            InternalServerError, ServiceUnavailable, TooManyRequests,
        )

        if not filas:
            return 0

        df = self.alinear(filas)
        job_config = self.bigquery.LoadJobConfig(
            schema=self.schema,
            write_disposition="WRITE_APPEND",
        )

        for intento in range(1, reintentos + 1):
            try:
                job = self.client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
                job.result()
                self.filas_subidas += len(df)
                self.lotes_subidos += 1
                print(
                    f"⬆️  Lote #{self.lotes_subidos}: {len(df)} reviews a BigQuery "
                    f"(acumulado en esta corrida: {self.filas_subidas})",
                    flush=True,
                )
                return len(df)
            except (TooManyRequests, ServiceUnavailable, InternalServerError) as e:
                if intento == reintentos:
                    raise
                espera = 5 * (2 ** (intento - 1))
                print(f"⚠️ BigQuery ocupado ({type(e).__name__}). Reintento {intento} en {espera}s…")
                time.sleep(espera)
        return 0

    # --- reanudación / resumen -------------------------------------- #

    def _cond_fecha_scan(self, fecha_iso):
        tipo = self.tipos.get("fecha_scan", "DATE")
        if tipo == "STRING":
            return "fecha_scan = @fecha", self.bigquery.ScalarQueryParameter("fecha", "STRING", fecha_iso)
        if tipo in ("TIMESTAMP", "DATETIME"):
            return "DATE(fecha_scan) = @fecha", self.bigquery.ScalarQueryParameter("fecha", "DATE", fecha_iso)
        return "fecha_scan = @fecha", self.bigquery.ScalarQueryParameter("fecha", "DATE", fecha_iso)

    def urls_ya_cargadas(self, fecha_iso, destino):
        """url_actividad ya cargadas para este destino y fecha_scan."""
        if "url_actividad" not in self.nombres or "fecha_scan" not in self.nombres:
            print("⚠️ Sin url_actividad/fecha_scan en la tabla: reanudación deshabilitada.")
            return set()

        cond, param = self._cond_fecha_scan(fecha_iso)
        params = [param]
        filtro = ""
        if "destino" in self.nombres and destino:
            filtro = " AND destino = @destino"
            params.append(self.bigquery.ScalarQueryParameter("destino", "STRING", destino))

        sql = (
            f"SELECT DISTINCT url_actividad FROM `{self.table_id}` "
            f"WHERE {cond}{filtro} AND url_actividad IS NOT NULL"
        )
        try:
            job = self.client.query(
                sql, job_config=self.bigquery.QueryJobConfig(query_parameters=params)
            )
            urls = {r["url_actividad"] for r in job.result()}
            print(f"🔄 Reanudación: {len(urls)} actividades ya cargadas para "
                  f"destino={destino}, fecha_scan={fecha_iso}")
            return urls
        except Exception as e:
            print(f"⚠️ No se pudo consultar el progreso previo ({type(e).__name__}: {e}). Se parte de cero.")
            return set()

    def resumen(self, fecha_iso):
        cond, param = self._cond_fecha_scan(fecha_iso)
        sql = f"""
            SELECT
              COUNT(*)                        AS reviews,
              COUNT(DISTINCT review_hash)     AS reviews_unicas,
              COUNT(DISTINCT url_actividad)   AS actividades,
              COUNT(DISTINCT destino)         AS destinos,
              MIN(fecha_review)               AS review_mas_vieja,
              MAX(fecha_review)               AS review_mas_nueva,
              ROUND(AVG(rating), 2)           AS rating_promedio
            FROM `{self.table_id}`
            WHERE {cond}
        """
        job = self.client.query(
            sql, job_config=self.bigquery.QueryJobConfig(query_parameters=[param])
        )
        return dict(next(iter(job.result())))

    def resumen_por_destino(self, fecha_iso):
        cond, param = self._cond_fecha_scan(fecha_iso)
        sql = f"""
            SELECT destino,
                   COUNT(*)                      AS reviews,
                   COUNT(DISTINCT url_actividad) AS actividades,
                   MIN(fecha_review)             AS desde,
                   MAX(fecha_review)             AS hasta
            FROM `{self.table_id}`
            WHERE {cond}
            GROUP BY destino
            ORDER BY reviews DESC
        """
        job = self.client.query(
            sql, job_config=self.bigquery.QueryJobConfig(query_parameters=[param])
        )
        return [dict(r) for r in job.result()]


# ====================================================================== #
# Progreso
# ====================================================================== #

class Progreso:
    def __init__(self, ruta, destino, fecha_scan, fecha_desde, fecha_hasta, omitidas_inicio):
        self.ruta = ruta
        self.datos = {
            "destino": destino,
            "fecha_scan": fecha_scan,
            "rango": f"{fecha_desde} → {fecha_hasta}",
            "actividades_ya_cargadas_al_inicio": omitidas_inicio,
            "filas_subidas": 0,
            "lotes_subidos": 0,
            "estado": "EN_CURSO",
            "inicio_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "ultima_actualizacion_utc": None,
        }
        os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
        self.guardar()

    def actualizar(self, **kwargs):
        self.datos.update(kwargs)
        self.guardar()

    def guardar(self):
        self.datos["ultima_actualizacion_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with open(self.ruta, "w", encoding="utf-8") as f:
            json.dump(self.datos, f, ensure_ascii=False, indent=2)

    def a_resumen_github(self, stats):
        ruta = os.environ.get("GITHUB_STEP_SUMMARY")
        if not ruta:
            return
        d = self.datos
        icono = {"COMPLETO": "✅", "INCOMPLETO_POR_TIEMPO": "⏱️", "ERROR": "❌"}.get(d["estado"], "🔄")
        with open(ruta, "a", encoding="utf-8") as f:
            f.write(f"## {icono} {d['destino']} — {d['estado']}\n\n")
            f.write("| Métrica | Valor |\n|---|---|\n")
            f.write(f"| fecha_scan | `{d['fecha_scan']}` |\n")
            f.write(f"| Rango de reviews | {d['rango']} |\n")
            f.write(f"| Actividades listadas | {stats.get('actividades_listadas', 0)} |\n")
            f.write(f"| Tarjetas descartadas (no eran actividades) | {stats.get('tarjetas_descartadas', 0)} |\n")
            f.write(f"| Actividades scrapeadas | {stats.get('actividades_scrapeadas', 0)} |\n")
            f.write(f"| Actividades omitidas (ya cargadas) | {d['actividades_ya_cargadas_al_inicio']} |\n")
            f.write(f"| Actividades sin opiniones | {stats.get('actividades_sin_opiniones', 0)} |\n")
            f.write(f"| Actividades con error | {stats.get('actividades_error', 0)} |\n")
            f.write(f"| Páginas de opiniones leídas | {stats.get('paginas_reviews', 0)} |\n")
            f.write(f"| Reviews en rango | {stats.get('reviews_en_rango', 0)} |\n")
            f.write(f"| Filas subidas a BigQuery | {d['filas_subidas']} |\n")
            f.write(f"| Lotes subidos | {d['lotes_subidos']} |\n")
            f.write(f"| Bloqueos HTTP (429/5xx) | {stats.get('http_bloqueos', 0)} |\n")
            if d["estado"] == "INCOMPLETO_POR_TIEMPO":
                pend = max(0, stats.get("actividades_listadas", 0)
                           - stats.get("actividades_scrapeadas", 0)
                           - stats.get("actividades_omitidas", 0)
                           - stats.get("actividades_sin_opiniones", 0))
                f.write(
                    f"\n> ⏱️ Se cortó por tiempo con ~{pend} actividades sin procesar. "
                    f"Volvé a lanzar el workflow con `fecha_scan = {d['fecha_scan']}` "
                    f"y este destino continúa donde quedó.\n"
                )


# ====================================================================== #
# Orquestación
# ====================================================================== #

async def ejecutar(args):
    from drivers.civitatis_reviews_full import COLUMNAS as COLUMNAS_DRIVER
    from drivers.civitatis_reviews_full import CivitatisReviewsScraper

    if COLUMNAS_DRIVER != COLUMNAS:
        print(f"⚠️ El driver y SCHEMA_POR_DEFECTO ya no coinciden.\n"
              f"   driver: {COLUMNAS_DRIVER}\n   acá:    {COLUMNAS}")

    fecha_iso = args.fecha_scan or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fecha_desde = date.fromisoformat(args.fecha_desde)
    fecha_hasta = date.fromisoformat(args.fecha_hasta) if args.fecha_hasta else date.today()

    if fecha_desde > fecha_hasta:
        print(f"❌ --fecha-desde ({fecha_desde}) es posterior a --fecha-hasta ({fecha_hasta}).")
        return 1

    destino_obj = resolver_destino(args.destino)
    if not destino_obj:
        print(f"❌ No se encontró el destino '{args.destino}' en {RUTA_DESTINOS}.")
        return 1

    nombre = destino_obj["name"]
    pais = destino_obj.get("nameCountry", "")

    print(f"\n🚀 {nombre} ({pais}) | reviews {fecha_desde} → {fecha_hasta} | fecha_scan={fecha_iso}")
    print(f"   Tabla destino: {args.tabla} | lote={args.batch_size} filas "
          f"| concurrencia actividades={args.concurrencia} | máx {args.max_minutos} min")

    cargador = BigQueryCargador(args.tabla, crear_si_no_existe=not args.no_crear_tabla)
    cargador.describir()
    cargador.validar_compatibilidad()

    urls_omitidas = set()
    if not args.no_reanudar:
        urls_omitidas = cargador.urls_ya_cargadas(fecha_iso, nombre)

    ruta_progreso = args.progreso or f"progreso/reviews_{_normalizar(nombre).replace(' ', '_')}.json"
    progreso = Progreso(
        ruta_progreso, nombre, fecha_iso, fecha_desde, fecha_hasta, len(urls_omitidas)
    )

    scraper = CivitatisReviewsScraper(
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        fecha_scan=cargador.valor_fecha_scan(fecha_iso),
        concurrencia_actividades=args.concurrencia,
        limite_actividades=args.limite_actividades,
        headless=not args.ver_navegador,
    )

    buffer = []
    lock = asyncio.Lock()

    async def flush(forzar=False):
        import pandas as pd

        nonlocal buffer
        async with lock:
            if not buffer or (not forzar and len(buffer) < args.batch_size):
                return
            lote, buffer = buffer, []
        await asyncio.to_thread(cargador.subir, lote)
        if args.csv_respaldo:
            pd.DataFrame(lote).reindex(columns=COLUMNAS).to_csv(
                args.csv_respaldo, mode="a", index=False,
                header=not os.path.isfile(args.csv_respaldo),
                encoding="utf-8-sig", sep=";",
            )
        progreso.actualizar(
            filas_subidas=cargador.filas_subidas,
            lotes_subidos=cargador.lotes_subidos,
        )

    async def on_rows(filas):
        async with lock:
            buffer.extend(filas)
            listo = len(buffer) >= args.batch_size
        if listo:
            await flush()

    deadline = time.monotonic() + args.max_minutos * 60
    estado = "COMPLETO"
    stats = {}

    try:
        await scraper.init_browser()
        stats = await scraper.run(
            destino_obj, on_rows, urls_omitidas=urls_omitidas, deadline=deadline
        )
        if scraper.detenido_por_tiempo:
            estado = "INCOMPLETO_POR_TIEMPO"
    except Exception as e:
        estado = "ERROR"
        stats = scraper.stats
        print(f"❌ Error fatal: {type(e).__name__}: {e}")
        raise
    finally:
        try:
            await scraper.close_browser()
        except Exception:
            pass
        # Siempre se intenta subir lo que quedó en memoria.
        try:
            await flush(forzar=True)
        except Exception as e:
            print(f"❌ No se pudo subir el último lote: {type(e).__name__}: {e}")
            estado = "ERROR"
        progreso.actualizar(
            estado=estado,
            filas_subidas=cargador.filas_subidas,
            lotes_subidos=cargador.lotes_subidos,
            **{f"scraper_{k}": v for k, v in (stats or {}).items()},
        )
        progreso.a_resumen_github(stats or {})

    icono = {"COMPLETO": "✅", "INCOMPLETO_POR_TIEMPO": "⏱️"}.get(estado, "❌")
    print(
        f"\n{icono} {nombre} terminó en estado {estado}: "
        f"{cargador.filas_subidas} reviews en {cargador.lotes_subidos} lotes. "
        f"Progreso en {ruta_progreso}"
    )
    return 0


# ====================================================================== #
# CLI
# ====================================================================== #

def parsear_args(argv):
    p = argparse.ArgumentParser(description="Scraper de reviews de Civitatis -> BigQuery")
    p.add_argument("--destino", default=None,
                   help='Destino a scrapear, formato "Ciudad, País" (ej: "Madrid, España").')
    p.add_argument("--fecha-desde", default=FECHA_DESDE_DEFAULT,
                   help=f"YYYY-MM-DD. Reviews desde esta fecha (default {FECHA_DESDE_DEFAULT}).")
    p.add_argument("--fecha-hasta", default=None, help="YYYY-MM-DD. Default: hoy.")
    p.add_argument("--fecha-scan", default=None,
                   help="YYYY-MM-DD de la corrida. Repetir el mismo valor para REANUDAR.")
    p.add_argument("--tabla", default=TABLA_DESTINO, help="Tabla destino en BigQuery.")
    p.add_argument("--batch-size", type=int, default=500, help="Filas por lote subido a BigQuery.")
    p.add_argument("--concurrencia", type=int, default=5,
                   help="Actividades en paralelo dentro del destino.")
    p.add_argument("--max-minutos", type=int, default=320,
                   help="Corta de forma ordenada antes del timeout de GitHub Actions (6h).")
    p.add_argument("--no-reanudar", action="store_true",
                   help="No consultar BigQuery por lo ya cargado (vuelve a scrapear todo).")
    p.add_argument("--no-crear-tabla", action="store_true", help="Falla si la tabla no existe.")
    p.add_argument("--csv-respaldo", default=None, help="Ruta de un CSV espejo de lo subido.")
    p.add_argument("--progreso", default=None, help="Ruta del JSON de progreso.")
    p.add_argument("--limite-actividades", type=int, default=None,
                   help="Tope de actividades por destino (pruebas).")
    p.add_argument("--ver-navegador", action="store_true", help="Lanzar con interfaz (debug local).")
    p.add_argument("--listar-destinos", action="store_true",
                   help="Validar la lista de destinos contra el JSON y salir.")
    p.add_argument("--matriz", action="store_true",
                   help="Imprimir la lista de destinos como JSON (matriz de GitHub Actions) y salir.")
    p.add_argument("--destinos", default=None,
                   help="Subconjunto separado por ';' para --matriz/--listar-destinos. "
                        "Vacío = los 20 configurados.")
    p.add_argument("--inspeccionar-tabla", action="store_true",
                   help="Solo mostrar el schema de BigQuery y salir.")
    p.add_argument("--resumen-bq", action="store_true",
                   help="Solo consultar BigQuery y resumir lo cargado para --fecha-scan.")
    return p.parse_args(argv)


def seleccionar_destinos(subconjunto=None):
    """Lista de destinos a procesar. `subconjunto` es un string separado por ';'."""
    if not subconjunto or not subconjunto.strip():
        return list(DESTINOS_OMAR)
    pedidos = [d.strip() for d in subconjunto.split(";") if d.strip()]
    elegidos, sueltos = [], []
    for pedido in pedidos:
        clave = _normalizar(pedido.split(",")[0])
        match = next((d for d in DESTINOS_OMAR if _normalizar(d.split(",")[0]) == clave), None)
        (elegidos if match else sueltos).append(match or pedido)
    if sueltos:
        print(f"⚠️ Destinos fuera de la lista configurada (se usan igual): {sueltos}", file=sys.stderr)
    return elegidos + sueltos


def listar_destinos(subconjunto=None):
    catalogo = cargar_catalogo()
    destinos = seleccionar_destinos(subconjunto)
    print(f"\n📋 {len(destinos)} destinos a procesar:\n")
    print(f"{'Entrada':<32} {'Civitatis':<20} {'País':<18} {'Slug':<18} {'Act.':>5} {'Reviews':>9}")
    print("-" * 108)
    faltan = []
    for entrada in destinos:
        d = resolver_destino(entrada, catalogo)
        if not d:
            faltan.append(entrada)
            print(f"{entrada:<32} ❌ NO ENCONTRADO")
            continue
        print(f"{entrada:<32} {d['name']:<20} {d.get('nameCountry', ''):<18} "
              f"{d['url']:<18} {str(d.get('totalActivities', '')):>5} "
              f"{str(d.get('numReviews', '')):>9}")
    if faltan:
        print(f"\n❌ Sin resolver: {faltan}")
        return 1
    print(f"\n✅ Los {len(destinos)} destinos resuelven contra destinos_civitatis.json")
    return 0


if __name__ == "__main__":
    args = parsear_args(sys.argv[1:])

    if args.matriz:
        # Salida consumida por el job `preparar` del workflow.
        print(json.dumps(seleccionar_destinos(args.destinos), ensure_ascii=False))
        sys.exit(0)

    if args.listar_destinos:
        sys.exit(listar_destinos(args.destinos))

    if args.inspeccionar_tabla:
        BigQueryCargador(args.tabla, crear_si_no_existe=False).describir()
        sys.exit(0)

    if args.resumen_bq:
        fecha_iso = args.fecha_scan or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cargador = BigQueryCargador(args.tabla, crear_si_no_existe=False)
        total = cargador.resumen(fecha_iso)
        por_destino = cargador.resumen_por_destino(fecha_iso)

        print(f"\n📊 {args.tabla} — fecha_scan = {fecha_iso}")
        for k, v in total.items():
            print(f"   {k:<18} {v:,}" if isinstance(v, int) else f"   {k:<18} {v}")

        ruta = os.environ.get("GITHUB_STEP_SUMMARY")
        if ruta:
            with open(ruta, "a", encoding="utf-8") as f:
                f.write(f"## 📊 Total cargado en `{args.tabla}` — fecha_scan `{fecha_iso}`\n\n")
                f.write("| Métrica | Valor |\n|---|---|\n")
                for k, v in total.items():
                    f.write(f"| {k} | {v:,} |\n" if isinstance(v, int) else f"| {k} | {v} |\n")
                esperados = seleccionar_destinos(args.destinos)
                f.write(f"\n### Por destino ({len(por_destino)}/{len(esperados)} con datos)\n\n")
                f.write("| Destino | Reviews | Actividades | Desde | Hasta |\n|---|---:|---:|---|---|\n")
                for r in por_destino:
                    f.write(f"| {r['destino']} | {r['reviews']:,} | {r['actividades']:,} "
                            f"| {r['desde']} | {r['hasta']} |\n")

                # Se compara contra el nombre canónico de Civitatis, que es el
                # que el scraper escribe en la columna `destino`.
                catalogo = cargar_catalogo()
                con_datos = {r["destino"] for r in por_destino}
                faltantes = []
                for entrada in esperados:
                    d = resolver_destino(entrada, catalogo)
                    if not d or d["name"] not in con_datos:
                        faltantes.append(d["name"] if d else entrada)
                if faltantes:
                    f.write(f"\n> ⚠️ Destinos sin ninguna fila para este `fecha_scan`: "
                            f"{', '.join(faltantes)}\n")
        sys.exit(0)

    if not args.destino:
        print("❌ Falta --destino. Ejemplo: --destino \"Madrid, España\"")
        print(f"   Destinos configurados: {len(DESTINOS_OMAR)} (ver --listar-destinos)")
        sys.exit(2)

    sys.exit(asyncio.run(ejecutar(args)))
