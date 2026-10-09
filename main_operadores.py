"""
Scraper de operadores de Civitatis para TODO el sitio.

Recorre los destinos de destinos_civitatis.json (3.748 destinos / ~28.7k
actividades), entra a cada ficha de actividad, extrae los operadores con sus
datos de contacto y sube las filas a BigQuery en LOTES INCREMENTALES
(WRITE_APPEND). Si el job se cae o se queda sin tiempo, lo ya subido queda en
la tabla y una nueva ejecución reanuda exactamente donde quedó.

Uso típico (GitHub Actions, un shard por runner):
    python main_operadores.py --shard 0 --total-shards 24 --moneda USD

Otros modos:
    python main_operadores.py --plan --total-shards 24        # ver reparto
    python main_operadores.py --inspeccionar-tabla            # ver schema BQ
    python main_operadores.py --paises "Chile,Perú" --shard 0 --total-shards 1
"""

import argparse
import asyncio
import json
import os
import sys
import time
import unicodedata
from datetime import datetime, date, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Sólo lo común a nivel de módulo: el motor "navegador" importa Playwright y el
# motor "http" no, así que cada uno se carga bajo demanda en ejecutar().
from drivers.civitatis_comun import COLUMNAS  # noqa: E402

RUTA_BASELINE = "civitatis_baseline.txt"
RUTA_CACHE_URLS = "data/urls_actividades_civitatis.txt"

TABLA_DESTINO = "datatur.supply.operators_civitatis"
RUTA_DESTINOS = "destinos_civitatis.json"

# Schema usado SOLO si la tabla no existe. Replica el real de
# datatur.supply.operators_civitatis, que tiene las 15 columnas en STRING
# (incluidos precio_real, opiniones, viajeros, rating y fecha_scan) y sin
# particionar. Si la tabla existe manda su schema vivo (ver alinear()).
SCHEMA_POR_DEFECTO = [(c, "STRING") for c in COLUMNAS]


# ====================================================================== #
# Destinos y sharding
# ====================================================================== #

def _normalizar(texto):
    texto = unicodedata.normalize("NFKD", str(texto or "").lower().strip())
    return texto.encode("ascii", "ignore").decode("ascii")


def cargar_destinos(paises=None):
    if not os.path.exists(RUTA_DESTINOS):
        print(f"❌ No se encontró {RUTA_DESTINOS}")
        sys.exit(1)

    with open(RUTA_DESTINOS, "r", encoding="utf-8") as f:
        todos = json.load(f)

    todos = [d for d in todos if d.get("url") and d.get("name") and d.get("nameCountry")]

    if paises:
        buscados = {_normalizar(p) for p in paises}
        todos = [d for d in todos if _normalizar(d.get("nameCountry")) in buscados]
        encontrados = {_normalizar(d["nameCountry"]) for d in todos}
        for p in buscados - encontrados:
            print(f"⚠️ País sin destinos en el JSON: {p}")

    return todos


def peso(destino):
    """Costo estimado de un destino: actividades + overhead fijo de carga."""
    try:
        actividades = int(destino.get("totalActivities") or 0)
    except (TypeError, ValueError):
        actividades = 0
    return actividades + 3


def urls_historicas_bq(tabla):
    """url_actividad vistas en scans anteriores: la fuente más completa."""
    try:
        from google.cloud import bigquery
        cli = bigquery.Client(project=tabla.split(".")[0])
        sql = (f"SELECT DISTINCT url_actividad AS u FROM `{tabla}` "
               f"WHERE url_actividad IS NOT NULL AND url_actividad != ''")
        return [r["u"] for r in cli.query(sql).result()]
    except Exception as e:
        print(f"⚠️ No se pudo leer el histórico de {tabla} ({type(e).__name__}). Se sigue sin esa fuente.")
        return []


def construir_trabajos(args, paises):
    """
    Lista de {url, pais, destino} para el motor HTTP.

    Las actividades se enumeran por sitemap porque los listados (/es/madrid/)
    devuelven 406 a clientes sin navegador. Si el sitemap no responde se cae al
    civitatis_baseline.txt del repo, que ya trae ~12,6k urls de actividad.
    """
    from drivers.civitatis_operadores_http import (
        filtrar_actividades, urls_desde_archivo, urls_desde_sitemap,
    )
    from drivers.civitatis_comun import slug_de_url

    if args.urls_desde and os.path.exists(args.urls_desde):
        actividades = filtrar_actividades(urls_desde_archivo(args.urls_desde))
        print(f"🗂️  {len(actividades)} urls de actividad leídas de {args.urls_desde}")
    else:
        # Tres fuentes que se complementan. El sitemap trae ~10,6k pero deja
        # fuera más de la mitad de lo que ya conocemos: el histórico de la
        # tabla tiene ~19,9k url_actividad distintas, de las cuales ~11,7k no
        # figuran en el sitemap. Las que ya no existan devolverán 404 y cuestan
        # un request.
        fuentes = {}
        if not args.solo_baseline:
            fuentes["sitemap"] = filtrar_actividades(
                urls_desde_sitemap(limite_sitemaps=args.limite_sitemaps))
        fuentes["baseline"] = filtrar_actividades(urls_desde_archivo(RUTA_BASELINE))
        if args.semilla_bq and not args.sin_bigquery:
            fuentes["historico_bq"] = filtrar_actividades(urls_historicas_bq(args.tabla))

        actividades = sorted(set().union(*[set(v) for v in fuentes.values()]) if fuentes else [])
        for nombre, v in fuentes.items():
            print(f"   fuente {nombre:<13} {len(v):>7,} urls")
        print(f"🔎 {len(actividades):,} urls de actividad únicas")

    # Cache para que los demás shards no vuelvan a pegarle al sitemap.
    if actividades and not args.urls_desde:
        try:
            os.makedirs(os.path.dirname(RUTA_CACHE_URLS) or ".", exist_ok=True)
            with open(RUTA_CACHE_URLS, "w", encoding="utf-8") as f:
                f.write("\n".join(actividades))
        except OSError:
            pass

    # slug de destino -> (nombre, país) desde el JSON, para que destino/pais
    # queden escritos igual que en las filas que ya tiene la tabla.
    mapa = {d["url"].lower(): (d["name"], d["nameCountry"]) for d in cargar_destinos(None)}
    filtro = {_normalizar(p) for p in paises} if paises else None

    trabajos, sin_destino = [], 0
    for u in actividades:
        slug, _ = slug_de_url(u)
        nombre, pais = mapa.get(slug, (None, None))
        if nombre is None:
            sin_destino += 1
            nombre = slug.replace("-", " ").title()
            pais = "N/A"
        if filtro and _normalizar(pais) not in filtro:
            continue
        trabajos.append({"url": u, "pais": pais, "destino": nombre})

    if sin_destino:
        print(f"ℹ️ {sin_destino} actividades cuyo slug de destino no está en "
              f"{RUTA_DESTINOS} (se guardan con pais='N/A')")
    return trabajos


def repartir_en_shards(destinos, total_shards):
    """Reparto greedy por peso para que todos los runners tarden parecido."""
    ordenados = sorted(destinos, key=peso, reverse=True)
    shards = [[] for _ in range(total_shards)]
    cargas = [0] * total_shards
    for d in ordenados:
        i = cargas.index(min(cargas))
        shards[i].append(d)
        cargas[i] += peso(d)
    # Dentro de cada shard, de menor a mayor: así los destinos chicos entran
    # rápido a BigQuery y el progreso se ve desde el principio.
    for s in shards:
        s.sort(key=peso)
    return shards, cargas


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
            # Sin particionar: fecha_scan es STRING en la tabla real y BigQuery
            # no admite particionado por columna de texto.
            return self.client.create_table(self.bigquery.Table(self.table_id, schema=schema))

    def describir(self):
        print(f"\n📐 Schema de {self.table_id} ({self.tabla.num_rows} filas actuales):")
        for f in self.schema:
            print(f"   - {f.name:<16} {f.field_type:<10} {f.mode}")
        faltan = [c for c in COLUMNAS if c not in self.nombres]
        sobran = [c for c in self.nombres if c not in COLUMNAS]
        if faltan:
            print(f"   ⚠️ El scraper genera columnas que la tabla NO tiene (se descartan): {faltan}")
        if sobran:
            print(f"   ⚠️ La tabla tiene columnas que el scraper NO genera (irán NULL): {sobran}")

    def validar_compatibilidad(self):
        """Falla rápido si la tabla exige campos que el scraper no produce."""
        requeridos = [f.name for f in self.schema if f.mode == "REQUIRED" and f.name not in COLUMNAS]
        if requeridos:
            raise SystemExit(
                f"❌ {self.table_id} tiene campos REQUIRED que este scraper no genera: {requeridos}"
            )
        if "fecha_scan" not in self.nombres:
            print("⚠️ La tabla no tiene columna fecha_scan: no se podrá reanudar por fecha.")

    def valor_fecha_scan(self, fecha_iso):
        """Devuelve fecha_scan en el tipo exacto que espera la tabla."""
        tipo = self.tipos.get("fecha_scan", "DATE")
        if tipo == "STRING":
            return fecha_iso
        if tipo in ("TIMESTAMP", "DATETIME"):
            return pd.Timestamp(fecha_iso)
        return date.fromisoformat(fecha_iso)

    def alinear(self, filas):
        """Lleva el DataFrame al schema real de la tabla (nombres, orden y tipos)."""
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
        from google.api_core.exceptions import TooManyRequests, ServiceUnavailable, InternalServerError

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
                    f"⬆️  Lote #{self.lotes_subidos}: {len(df)} filas a BigQuery "
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

    def resumen(self, fecha_iso):
        """Conteos de lo cargado para una fecha_scan, para cerrar el workflow."""
        tipo = self.tipos.get("fecha_scan", "DATE")
        cond = "DATE(fecha_scan) = @fecha" if tipo in ("TIMESTAMP", "DATETIME") else "fecha_scan = @fecha"
        tipo_param = "STRING" if tipo == "STRING" else "DATE"
        sql = f"""
            SELECT
              COUNT(*)                        AS filas,
              COUNT(DISTINCT url_actividad)   AS actividades,
              COUNT(DISTINCT destino)         AS destinos,
              COUNT(DISTINCT pais)            AS paises,
              COUNT(DISTINCT operador)        AS operadores,
              COUNTIF(email IS NOT NULL AND email NOT IN ('N/A', '')) AS filas_con_email
            FROM `{self.table_id}`
            WHERE {cond}
        """
        job = self.client.query(
            sql,
            job_config=self.bigquery.QueryJobConfig(
                query_parameters=[self.bigquery.ScalarQueryParameter("fecha", tipo_param, fecha_iso)]
            ),
        )
        return dict(next(iter(job.result())))

    def urls_ya_cargadas(self, fecha_iso, paises):
        """Qué url_actividad ya están en la tabla para esta fecha_scan."""
        if "url_actividad" not in self.nombres or "fecha_scan" not in self.nombres:
            print("⚠️ Sin url_actividad/fecha_scan en la tabla: reanudación deshabilitada.")
            return set()

        tipo = self.tipos["fecha_scan"]
        if tipo == "STRING":
            cond, param = "fecha_scan = @fecha", self.bigquery.ScalarQueryParameter("fecha", "STRING", fecha_iso)
        elif tipo in ("TIMESTAMP", "DATETIME"):
            cond = "DATE(fecha_scan) = @fecha"
            param = self.bigquery.ScalarQueryParameter("fecha", "DATE", fecha_iso)
        else:
            cond, param = "fecha_scan = @fecha", self.bigquery.ScalarQueryParameter("fecha", "DATE", fecha_iso)

        params = [param]
        filtro_pais = ""
        if "pais" in self.nombres and paises:
            filtro_pais = " AND pais IN UNNEST(@paises)"
            params.append(self.bigquery.ArrayQueryParameter("paises", "STRING", sorted(paises)))

        sql = (
            f"SELECT DISTINCT url_actividad FROM `{self.table_id}` "
            f"WHERE {cond}{filtro_pais} AND url_actividad IS NOT NULL"
        )
        try:
            job = self.client.query(
                sql, job_config=self.bigquery.QueryJobConfig(query_parameters=params)
            )
            urls = {r["url_actividad"] for r in job.result()}
            print(f"🔄 Reanudación: {len(urls)} actividades ya cargadas para fecha_scan={fecha_iso}")
            return urls
        except Exception as e:
            print(f"⚠️ No se pudo consultar el progreso previo ({type(e).__name__}: {e}). Se parte de cero.")
            return set()


class CargadorCSV:
    """
    Reemplazo de BigQueryCargador para --sin-bigquery: misma interfaz, pero
    escribe a un CSV local. Sirve para probar el scraper sin credenciales y
    sin ensuciar la tabla.
    """

    def __init__(self, ruta):
        self.ruta = ruta
        self.table_id = ruta
        self.nombres = list(COLUMNAS)
        self.tipos = {c: "STRING" for c in COLUMNAS}  # igual que la tabla real
        self.filas_subidas = 0
        self.lotes_subidos = 0
        os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)

    def describir(self):
        print(f"\n🧪 Modo --sin-bigquery: las filas van a {self.ruta} (no se toca BigQuery)")

    def validar_compatibilidad(self):
        pass

    def valor_fecha_scan(self, fecha_iso):
        return fecha_iso

    def subir(self, filas, reintentos=1):
        df = pd.DataFrame(filas).reindex(columns=self.nombres)
        df.to_csv(self.ruta, mode="a", index=False, header=not os.path.isfile(self.ruta),
                  encoding="utf-8-sig", sep=";")
        self.filas_subidas += len(df)
        self.lotes_subidos += 1
        print(f"⬆️  Lote #{self.lotes_subidos}: {len(df)} filas al CSV "
              f"(acumulado: {self.filas_subidas})", flush=True)
        return len(df)

    def urls_ya_cargadas(self, fecha_iso, paises):
        if not os.path.isfile(self.ruta):
            return set()
        try:
            df = pd.read_csv(self.ruta, sep=";", usecols=["url_actividad", "fecha_scan"])
            urls = set(df.loc[df["fecha_scan"].astype(str) == fecha_iso, "url_actividad"].dropna())
            print(f"🔄 Reanudación desde CSV: {len(urls)} actividades ya guardadas")
            return urls
        except Exception:
            return set()


# ====================================================================== #
# Progreso
# ====================================================================== #

class Progreso:
    def __init__(self, ruta, shard, total_shards, fecha_scan, total_destinos, omitidas_inicio):
        self.ruta = ruta
        self.datos = {
            "shard": shard,
            "total_shards": total_shards,
            "fecha_scan": fecha_scan,
            "destinos_asignados": total_destinos,
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
        icono = {"COMPLETO": "✅", "INCOMPLETO_POR_TIEMPO": "⏱️", "BLOQUEADO": "🛑",
                 "ERROR": "❌"}.get(d["estado"], "🔄")
        with open(ruta, "a", encoding="utf-8") as f:
            f.write(f"## {icono} Shard {d['shard']}/{d['total_shards']} — {d['estado']}\n\n")
            f.write("| Métrica | Valor |\n|---|---|\n")
            f.write(f"| fecha_scan | `{d['fecha_scan']}` |\n")
            f.write(f"| Unidades asignadas | {d['destinos_asignados']} |\n")
            if "fichas_ok" in stats:   # motor http
                f.write(f"| Fichas OK | {stats.get('fichas_ok', 0)} |\n")
                f.write(f"| Fichas con error | {stats.get('fichas_error', 0)} |\n")
                f.write(f"| Fichas 404 | {stats.get('fichas_404', 0)} |\n")
                f.write(f"| Sin operador declarado | {stats.get('sin_operador', 0)} |\n")
                f.write(f"| Respuestas 429/406 | {stats.get('http_bloqueos', 0)} |\n")
            else:                      # motor navegador
                f.write(f"| Destinos OK | {stats.get('destinos_ok', 0)} |\n")
                f.write(f"| Destinos sin actividades | {stats.get('destinos_vacios', 0)} |\n")
                f.write(f"| Destinos con error | {stats.get('destinos_error', 0)} |\n")
                f.write(f"| Actividades nuevas scrapeadas | {stats.get('actividades', 0)} |\n")
            f.write(f"| Omitidas (ya estaban) | {d['actividades_ya_cargadas_al_inicio']} |\n")
            f.write(f"| Filas subidas a BigQuery | {d['filas_subidas']} |\n")
            f.write(f"| Lotes subidos | {d['lotes_subidos']} |\n")
            if d["estado"] == "INCOMPLETO_POR_TIEMPO":
                f.write(
                    f"\n> ⏱️ Se alcanzó el tiempo máximo. Volvé a lanzar el workflow con "
                    f"`fecha_scan = {d['fecha_scan']}` y continuará donde quedó.\n"
                )


# ====================================================================== #
# Orquestación
# ====================================================================== #

async def ejecutar(args):
    fecha_iso = args.fecha_scan or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    paises = [p.strip() for p in args.paises.split(",") if p.strip()] if args.paises else None
    http = args.motor == "http"

    if http:
        # Unidad de trabajo = una ficha de actividad. El reparto es round-robin
        # porque todas cuestan prácticamente lo mismo (1 request cada una).
        trabajos = construir_trabajos(args, paises)
        if not trabajos:
            print("⚠️ No hay actividades que procesar.")
            return 0
        if args.plan:
            print(f"\n📊 {len(trabajos)} actividades en {args.total_shards} shards:")
            for i in range(args.total_shards):
                print(f"   shard {i:>3}: {len(trabajos[i::args.total_shards]):>6} fichas")
            return 0
        mis_items = trabajos[args.shard::args.total_shards]
        if args.limite_destinos:
            mis_items = mis_items[: args.limite_destinos]
        mis_paises = {t["pais"] for t in mis_items}
        unidad = "fichas"
    else:
        destinos = cargar_destinos(paises)
        if not destinos:
            print("⚠️ No hay destinos que procesar.")
            return 0
        shards, cargas = repartir_en_shards(destinos, args.total_shards)
        if args.plan:
            print(f"\n📊 Reparto de {len(destinos)} destinos en {args.total_shards} shards:")
            for i, (s, c) in enumerate(zip(shards, cargas)):
                print(f"   shard {i:>3}: {len(s):>4} destinos | peso {c:>6}")
            print(f"\n   peso total: {sum(cargas)} | min {min(cargas)} | max {max(cargas)}")
            return 0
        mis_items = shards[args.shard]
        if args.limite_destinos:
            mis_items = mis_items[: args.limite_destinos]
        mis_paises = {d["nameCountry"] for d in mis_items}
        unidad = "destinos"

    print(f"\n🚀 Shard {args.shard}/{args.total_shards} | motor={args.motor} "
          f"| {len(mis_items)} {unidad} | {len(mis_paises)} países "
          f"| fecha_scan={fecha_iso} | moneda={args.moneda}")
    print(f"   Tabla destino: {args.tabla} | lote={args.batch_size} filas")

    if args.sin_bigquery:
        cargador = CargadorCSV(args.salida_csv or f"data/operadores_shard_{args.shard}_{fecha_iso}.csv")
    else:
        cargador = BigQueryCargador(args.tabla, crear_si_no_existe=not args.no_crear_tabla)
    cargador.describir()
    cargador.validar_compatibilidad()

    urls_omitidas = set()
    if not args.no_reanudar:
        # Con motor http el shard es round-robin sobre todas las urls, así que
        # filtrar por país no acota nada y puede dejar fuera filas ya cargadas.
        urls_omitidas = cargador.urls_ya_cargadas(fecha_iso, None if http else mis_paises)

    ruta_progreso = args.progreso or f"progreso/operadores_shard_{args.shard}.json"
    progreso = Progreso(
        ruta_progreso, args.shard, args.total_shards, fecha_iso, len(mis_items), len(urls_omitidas)
    )

    # fecha_scan ya en el tipo que pide la tabla
    fecha_valor = cargador.valor_fecha_scan(fecha_iso)

    incluir_desc = "descripcion" in cargador.nombres and not args.sin_descripcion

    if http:
        from drivers.civitatis_operadores_http import CivitatisOperadoresHTTP
        # Canario: una ficha que YA scrapeamos bien para este fecha_scan. Si
        # ella empieza a devolver 406 es bloqueo, no bajas del catálogo.
        from drivers.civitatis_operadores_http import CANARIO_POR_DEFECTO
        canario = next(iter(sorted(urls_omitidas)), None) if urls_omitidas else CANARIO_POR_DEFECTO
        print(f"🐤 Canario: {canario}")
        scraper = CivitatisOperadoresHTTP(
            moneda=args.moneda,
            fecha_scan=fecha_valor,
            concurrencia=args.concurrencia,
            pausa=args.pausa,
            incluir_descripcion=incluir_desc,
            canario=canario,
        )
    else:
        from drivers.civitatis_operadores_full import CivitatisOperadoresScraper, proxy_apify
        proxy = None
        if not args.sin_proxy:
            proxy = proxy_apify(session_id=f"civop{args.shard}{fecha_iso.replace('-', '')}")
            if proxy is None:
                print("⚠️ Sin APIFY_PROXY_PASSWORD: se sale por la IP del runner. "
                      "Civitatis suele responder 429/406 y el shard puede quedar incompleto.")
        scraper = CivitatisOperadoresScraper(
            currency_code=args.moneda,
            fecha_scan=fecha_valor,
            concurrencia_destinos=args.concurrencia_destinos,
            concurrencia_detalle=args.concurrencia_detalle,
            incluir_descripcion=incluir_desc,
            headless=not args.ver_navegador,
            pausa_entre_fichas=args.pausa,
            proxy=proxy,
        )

    buffer = []
    lock = asyncio.Lock()

    async def flush(forzar=False):
        nonlocal buffer
        async with lock:
            if not buffer or (not forzar and len(buffer) < args.batch_size):
                return
            lote, buffer = buffer, []
        await asyncio.to_thread(cargador.subir, lote)
        if args.csv_respaldo:
            pd.DataFrame(lote).reindex(columns=COLUMNAS).to_csv(
                args.csv_respaldo,
                mode="a",
                index=False,
                header=not os.path.isfile(args.csv_respaldo),
                encoding="utf-8-sig",
                sep=";",
            )
        progreso.actualizar(
            filas_subidas=cargador.filas_subidas,
            lotes_subidos=cargador.lotes_subidos,
        )

    async def on_rows(filas, destino=None):
        async with lock:
            buffer.extend(filas)
            listo = len(buffer) >= args.batch_size
        if listo:
            await flush()

    deadline = time.monotonic() + args.max_minutos * 60
    estado = "COMPLETO"
    stats = {}

    async def correr():
        if http:
            return await scraper.run(mis_items, on_rows, urls_omitidas=urls_omitidas, deadline=deadline)
        await scraper.init_browser()
        await scraper.preparar_sesion()
        return await scraper.run(mis_items, on_rows, urls_omitidas=urls_omitidas, deadline=deadline)

    try:
        # Watchdog duro: en la corrida del 2026-10-08 diez shards se colgaron
        # dentro de un page.goto y el corte por tiempo nunca se evaluó, así que
        # murieron por timeout de GitHub perdiendo el buffer en memoria. Ahora
        # el límite se impone desde afuera, pase lo que pase adentro.
        margen = max(60, args.max_minutos * 60 * 0.1)
        stats = await asyncio.wait_for(correr(), timeout=args.max_minutos * 60 + margen)
        if getattr(scraper, "bloqueado", False):
            estado = "BLOQUEADO"
        elif getattr(scraper, "detenido_por_tiempo", False):
            estado = "INCOMPLETO_POR_TIEMPO"
    except asyncio.TimeoutError:
        estado = "INCOMPLETO_POR_TIEMPO"
        stats = dict(getattr(scraper, "stats", {}) or {})
        print(f"⏱️ Watchdog: se cortó a los {args.max_minutos} min. "
              f"Se sube lo que haya en memoria y se sale.")
    except Exception as e:
        estado = "ERROR"
        stats = dict(getattr(scraper, "stats", {}) or {})
        print(f"❌ Error fatal: {type(e).__name__}: {e}")
        raise
    finally:
        if not http:
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
        # Auditoría de cobertura (sólo motor navegador): slugs vistos en algún
        # listado que no existen como destino en destinos_civitatis.json. El
        # motor http no la necesita porque enumera por sitemap.
        descartados = getattr(scraper, "slugs_descartados", None)
        if descartados:
            conocidos = {d["url"].lower() for d in cargar_destinos(None)}
            huerfanos = {s: n for s, n in descartados.items() if s not in conocidos}
            if huerfanos:
                top = sorted(huerfanos.items(), key=lambda kv: -kv[1])[:30]
                print(f"\n⚠️ {len(huerfanos)} slugs sin destino propio en el JSON "
                      f"({sum(huerfanos.values())} tarjetas). Top: {top[:10]}")
                progreso.actualizar(slugs_sin_destino=dict(top),
                                    slugs_sin_destino_total=len(huerfanos))

        progreso.actualizar(
            estado=estado,
            filas_subidas=cargador.filas_subidas,
            lotes_subidos=cargador.lotes_subidos,
            **{f"scraper_{k}": v for k, v in (stats or {}).items()},
        )
        progreso.a_resumen_github(stats or {})

    print(
        f"\n{'✅' if estado == 'COMPLETO' else '⏱️' if estado == 'INCOMPLETO_POR_TIEMPO' else '❌'} "
        f"Shard {args.shard} terminó en estado {estado}: "
        f"{cargador.filas_subidas} filas en {cargador.lotes_subidos} lotes. "
        f"Progreso en {ruta_progreso}"
    )
    return 0


def parsear_args(argv):
    p = argparse.ArgumentParser(description="Scraper de operadores Civitatis (sitio completo) -> BigQuery")
    p.add_argument("--paises", default=None,
                   help="Lista separada por coma. Por defecto: TODOS los países del JSON.")
    p.add_argument("--shard", type=int, default=0, help="Índice de este shard (0-based).")
    p.add_argument("--total-shards", type=int, default=1, help="Cantidad total de shards.")
    p.add_argument("--moneda", default="USD",
                   help="Código de moneda de Civitatis (USD, CLP, EUR, ARS, BRL, COP, GBP, MXN, PEN).")
    p.add_argument("--tabla", default=TABLA_DESTINO, help="Tabla destino en BigQuery.")
    p.add_argument("--batch-size", type=int, default=500, help="Filas por lote subido a BigQuery.")
    p.add_argument("--motor", choices=("http", "navegador"), default="http",
                   help="http = 1 request por ficha, sin navegador ni proxy (recomendado). "
                        "navegador = Playwright + Apify (legado).")
    p.add_argument("--concurrencia", type=int, default=2,
                   help="[motor http] fichas en paralelo.")
    p.add_argument("--urls-desde", default=None,
                   help="[motor http] archivo con urls en vez de consultar el sitemap.")
    p.add_argument("--sin-semilla-bq", dest="semilla_bq", action="store_false",
                   help="[motor http] no sembrar urls desde el histórico de la tabla.")
    p.add_argument("--solo-baseline", action="store_true",
                   help="[motor http] no consultar el sitemap; usar civitatis_baseline.txt.")
    p.add_argument("--limite-sitemaps", type=int, default=None,
                   help="[motor http] tope de sub-sitemaps a leer (pruebas).")
    p.add_argument("--concurrencia-destinos", type=int, default=3)
    p.add_argument("--concurrencia-detalle", type=int, default=6)
    p.add_argument("--pausa", type=float, default=1.5,
                   help="Segundos de pausa tras cada ficha (subir si aparecen 429).")
    p.add_argument("--sin-proxy", action="store_true",
                   help="Ignorar APIFY_PROXY_PASSWORD y salir por la IP del runner.")
    p.add_argument("--max-minutos", type=int, default=320,
                   help="Corta de forma ordenada antes del timeout de GitHub Actions (6h).")
    p.add_argument("--fecha-scan", default=None,
                   help="YYYY-MM-DD. Repetir el mismo valor para reanudar una corrida previa.")
    p.add_argument("--no-reanudar", action="store_true",
                   help="No consultar BigQuery por lo ya cargado (vuelve a scrapear todo).")
    p.add_argument("--no-crear-tabla", action="store_true", help="Falla si la tabla no existe.")
    p.add_argument("--sin-bigquery", action="store_true",
                   help="Prueba en seco: escribe a CSV en vez de subir a BigQuery.")
    p.add_argument("--salida-csv", default=None, help="Ruta del CSV cuando se usa --sin-bigquery.")
    p.add_argument("--sin-descripcion", action="store_true", help="No extraer el texto de descripción.")
    p.add_argument("--csv-respaldo", default=None, help="Ruta de un CSV espejo de lo subido.")
    p.add_argument("--progreso", default=None, help="Ruta del JSON de progreso.")
    p.add_argument("--limite-destinos", type=int, default=None,
                   help="Tope de unidades de trabajo (destinos o fichas) para pruebas.")
    p.add_argument("--plan", action="store_true", help="Solo mostrar el reparto en shards y salir.")
    p.add_argument("--inspeccionar-tabla", action="store_true", help="Solo mostrar el schema de BigQuery y salir.")
    p.add_argument("--resumen-bq", action="store_true",
                   help="Solo consultar BigQuery y resumir lo cargado para --fecha-scan.")
    p.add_argument("--ver-navegador", action="store_true", help="Lanzar con interfaz (debug local).")

    args = p.parse_args(argv)
    if args.total_shards < 1:
        p.error("--total-shards debe ser >= 1")
    if not 0 <= args.shard < args.total_shards:
        p.error(f"--shard debe estar entre 0 y {args.total_shards - 1}")
    return args


if __name__ == "__main__":
    args = parsear_args(sys.argv[1:])

    if args.inspeccionar_tabla:
        cargador = BigQueryCargador(args.tabla, crear_si_no_existe=False)
        cargador.describir()
        sys.exit(0)

    if args.resumen_bq:
        fecha_iso = args.fecha_scan or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cargador = BigQueryCargador(args.tabla, crear_si_no_existe=False)
        r = cargador.resumen(fecha_iso)
        print(f"\n📊 {args.tabla} — fecha_scan = {fecha_iso}")
        for k, v in r.items():
            print(f"   {k:<18} {v:,}")

        ruta = os.environ.get("GITHUB_STEP_SUMMARY")
        if ruta:
            with open(ruta, "a", encoding="utf-8") as f:
                f.write(f"## 📊 Total cargado en `{args.tabla}` — fecha_scan `{fecha_iso}`\n\n")
                f.write("| Métrica | Valor |\n|---|---|\n")
                for k, v in r.items():
                    f.write(f"| {k} | {v:,} |\n")
        sys.exit(0)

    sys.exit(asyncio.run(ejecutar(args)))
