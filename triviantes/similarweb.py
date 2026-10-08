#!/usr/bin/env python3
"""
Extracción SimilarWeb — 5 reportes para el análisis de atracciones colombianas.
v2 — correcciones sobre la primera versión.

CAMBIOS EN ESTA VERSIÓN
  1. Orden de columnas FIJO por reporte (antes salía de un set y variaba entre corridas,
     lo que rompe un `bq load` o el apilado de varios archivos).
  2. Archivos versionados: out/r1_website_keywords_YYYYMMDD.csv — ya no se sobreescriben.
     Con --overwrite se vuelve al nombre sin fecha.
  3. El log distingue "la API respondió vacío" (sin datos en el panel) de "la llamada falló"
     (error HTTP). Antes ambos casos se veían como "0 dominios" y no se podía diagnosticar.
     Al final imprime un resumen de vacíos vs errores.
  4. R1 pagina con offset: los dominios grandes (tuboleta, salitremagico, mundoaventura)
     se truncaban en 100 keywords. Ahora se traen hasta --max-pages páginas.
  5. Flags nuevos: --limit-r1, --r5-seeds, --overwrite, --max-pages.

ENDPOINTS (verificados contra developers.similarweb.com, nov-2025)
  R1  GET /v4/website-analysis/keywords                        0.13 cr/keyword
  R2  GET /v4/keywords/{keyword}/analysis/overview             17 cr/keyword   (CARO)
  R3  GET /v4/keywords/{keyword}/analysis/competitors          0.07 cr/dominio
  R4  derivado de R1 en pandas                                 sin costo
  R5  GET /v4/website/{domain}/similar-sites/similarsites      2 cr/fila

R1, R2 y R3 solo aceptan UN MES por llamada (start_date == end_date). La serie
de 12 meses se arma iterando meses.

USO
  export SIMILARWEB_API_KEY="tu_key"
  python similarweb_extract.py --dry-run --report r1
  python similarweb_extract.py --report r1                      # 12 meses
  python similarweb_extract.py --report r3 --keywords-file kws.csv
  python similarweb_extract.py --report r5 --r5-seeds triviantes.com parquesencolombia.com

Requiere: pip install httpx pandas
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path
from urllib.parse import quote

import httpx
import truststore

truststore.inject_into_ssl()  # usa el keychain del sistema (necesario detras de proxies con SSL inspection, ej. Netskope)

try:
    import pandas as pd
except ImportError:
    pd = None

BASE = "https://api.similarweb.com"
COUNTRY = "co"
WEB_SOURCE = "Total"
TIMEOUT = 60.0
MAX_RETRIES = 4
SLEEP = 0.35
OUT = Path("./out")

COSTO = {"r1": 0.13, "r2": 17.0, "r3": 0.07, "r5": 2.0,
         "r6_visits": 1.0, "r6_pages": 3.0}

# R6 — dimensionamiento. OTAs + Triviantes + el competidor espejo.
DOMINIOS_R6 = [
    "triviantes.com", "parquesencolombia.com", "tur.com",
    "getyourguide.com", "civitatis.com", "viator.com", "tiqets.com",
]

# Paths que identifican producto COLOMBIANO dentro de las OTAs.
# Se usan para filtrar la salida de Popular Pages (post-proceso, no en la API).
PATHS_COLOMBIA = [
    "colombia", "bogota", "cartagena", "medellin", "santa-marta", "santamarta",
    "cali", "san-andres", "eje-cafetero", "salento", "guatape", "zipaquira",
    "barranquilla", "villa-de-leyva", "tayrona", "baru", "pereira", "armenia",
    "manizales", "leticia", "popayan", "bucaramanga",
]

# --------------------------------------------------------------------------- #
# ENTRADAS
# --------------------------------------------------------------------------- #

DOMINIOS = [

    "tur.com"   # calibración contra tu Search Console
]

# Dominios grandes que se truncan en limit=100: se paginan.
PAGINAR = {
    "tuboleta.com", "salitremagico.com.co", "mundoaventura.com.co",
    "haciendanapoles.com", "parquesencolombia.com", "catedraldesal.gov.co",
    "zoologicodecali.com.co", "triviantes.com", "turismoquindio.com",
    "zipaquiraturistica.com", "tur.com",
}

# Semillas por defecto para R5 (antes eran los 44 dominios = 3.520 créditos).
R5_SEEDS = [
    "triviantes.com", "parquesencolombia.com", "cafeyturismomaster.co",
    "exploreti.com", "tuboleta.com", "panaca.co",
]

KEYWORDS = [
    "entradas panaca", "boletas panaca", "comprar entradas panaca",
    "entradas ukumari", "boletas ukumari",
    "entradas catedral de sal zipaquira", "boletas catedral de sal",
    "entradas zoologico de cali", "boletas zoologico de cali",
    "entradas mundo aventura", "pasaporte mundo aventura",
    "entradas aviario nacional", "entradas acuario mundo marino santa marta",
    "entradas recinto del pensamiento", "entradas mina de sal nemocon",
    "entradas recuca", "entradas parque los arrieros",
    "entradas termales santa rosa de cabal", "entradas termales san vicente",
    "entradas parque del cafe", "entradas hacienda napoles", "entradas salitre magico",
]

GAP_A, GAP_B = "triviantes.com", "parquesencolombia.com"

# --------------------------------------------------------------------------- #
# ORDEN DE COLUMNAS FIJO (fix #1)
# --------------------------------------------------------------------------- #

COLUMNAS = {
    "r1_website_keywords": [
        "snapshot", "mes", "pais", "dominio", "keyword", "clicks", "traffic_share",
        "volume", "position", "difficulty", "cpc", "primary_intent",
        "secondary_intent", "zero_clicks_share", "competition", "top_url", "pagina",
    ],
    "r2_keywords_overview": [
        "snapshot", "mes", "pais", "keyword", "volume", "clicks", "zero_clicks",
        "organic_difficulty", "paid_competition", "intent",
        "cpc_low", "cpc_high", "total_spent",
    ],
    "r3_serp_players": [
        "snapshot", "mes", "pais", "keyword", "rank_en_respuesta", "dominio",
        "clicks", "traffic_share", "position", "top_url",
    ],
    "r4_keyword_gap": [
        "snapshot", "dominio_a", "dominio_b", "keyword",
        "clicks_a", "clicks_b", "gap_clicks", "situacion",
    ],
    "r5_similar_sites": [
        "snapshot", "dominio_semilla", "dominio_similar", "similarity_score",
    ],
    "r6_visits": [
        "snapshot", "mes", "pais", "dominio", "visits", "incluye_subdominios",
    ],
    "r6_popular_pages": [
        "snapshot", "mes", "pais", "dominio", "url_path", "share", "change",
        "es_producto_colombia", "match_path",
    ],
}

# --------------------------------------------------------------------------- #
# CLIENTE
# --------------------------------------------------------------------------- #

class SimilarWeb:
    """estado devuelto: 'ok' | 'empty' | 'error' | 'dry'  (fix #3)"""

    def __init__(self, api_key: str, dry_run: bool = False):
        self.api_key = api_key
        self.dry_run = dry_run
        self.credits = 0.0
        self.tally = Counter()
        self.client = httpx.Client(timeout=TIMEOUT, follow_redirects=True)

    def get(self, path: str, params: dict, costo: float = 0.0) -> tuple[dict | None, str]:
        self.credits += costo
        if self.dry_run:
            self.tally["dry"] += 1
            return None, "dry"
        params = {**params, "api_key": self.api_key}
        url = f"{BASE}{path}"
        for intento in range(1, MAX_RETRIES + 1):
            try:
                r = self.client.get(url, params=params)
                if r.status_code == 429:
                    espera = min(60, 5 * intento)
                    print(f"      · 429 rate limit, esperando {espera}s")
                    time.sleep(espera)
                    continue
                if r.status_code in (500, 502, 503, 504):
                    raise httpx.HTTPStatusError(str(r.status_code), request=r.request, response=r)
                if r.status_code == 401:
                    self.tally["error"] += 1
                    return None, "error:401 api key inválida"
                if r.status_code == 403:
                    self.tally["error"] += 1
                    return None, "error:403 fuera de tu suscripción"
                if r.status_code == 404:
                    self.tally["empty"] += 1
                    return None, "empty:404 sin datos para ese recurso"
                if r.status_code != 200:
                    self.tally["error"] += 1
                    return None, f"error:{r.status_code} {r.text}"
                self.tally["ok"] += 1
                return r.json(), "ok"
            except (httpx.TransportError, httpx.HTTPStatusError, ValueError) as e:
                if intento == MAX_RETRIES:
                    self.tally["error"] += 1
                    return None, f"error:{type(e).__name__}"
                time.sleep(2 * intento)
        self.tally["error"] += 1
        return None, "error:agotó reintentos"

    def close(self):
        self.client.close()


def extraer_lista(data: dict, *claves: str) -> list:
    """La doc no publica el schema; probamos las claves plausibles."""
    for k in claves:
        v = data.get(k)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            for k2 in claves:
                if isinstance(v.get(k2), list):
                    return v[k2]
    return []


def escribir_csv(nombre: str, filas: list[dict], overwrite: bool = False) -> None:
    if not filas:
        print(f"  (sin filas para {nombre} — no se escribe archivo)")
        return
    OUT.mkdir(exist_ok=True)
    sufijo = "" if overwrite else f"_{date.today():%Y%m%d}"
    ruta = OUT / f"{nombre}{sufijo}.csv"
    campos = COLUMNAS[nombre]
    extra = sorted({k for f in filas for k in f} - set(campos))
    if extra:
        print(f"  · campos no previstos (se agregan al final): {extra}")
        campos = campos + extra
    with open(ruta, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(filas)
    print(f"  → {ruta} ({len(filas):,} filas, {len(campos)} columnas)")


def meses_ultimos_12() -> list[str]:
    hoy = date.today()
    out, y, m = [], hoy.year, hoy.month
    for _ in range(12):
        m -= 1
        if m == 0:
            m, y = 12, y - 1
        out.append(f"{y}-{m:02d}")
    return sorted(out)


def kw_path(kw: str) -> str:
    return quote(kw, safe="")


# --------------------------------------------------------------------------- #
# R1 — WEBSITE KEYWORDS (con paginado, fix #4)
# --------------------------------------------------------------------------- #

def r1_website_keywords(sw: SimilarWeb, dominios: list[str], meses: list[str],
                        limit: int = 100, max_pages: int = 5) -> list[dict]:
    print(f"\nR1 · Website Keywords — {len(dominios)} dominios × {len(meses)} meses (limit {limit})")
    filas, snap = [], date.today().isoformat()
    for dom in dominios:
        paginas = max_pages if dom in PAGINAR else 1
        for mes in meses:
            total_dom = 0
            for pag in range(paginas):
                data, estado = sw.get(
                    "/v4/website-analysis/keywords",
                    {"URL": dom, "start_date": mes, "end_date": mes, "country": COUNTRY,
                     "traffic_source": "Organic", "web_source": WEB_SOURCE,
                     "branded_type": "All", "limit": str(limit),
                     "offset": str(pag * limit)},
                    costo=COSTO["r1"] * limit,
                )
                if estado != "dry":
                    time.sleep(SLEEP)
                if estado == "dry":
                    continue          # sigue contando páginas: el costo debe ser el techo real
                if estado.startswith("error"):
                    print(f"  {dom:40s} {mes} p{pag}  ✗ {estado}")
                    break
                registros = extraer_lista(data or {}, "data", "keywords")
                for k in registros:
                    filas.append({
                        "snapshot": snap, "mes": mes, "pais": COUNTRY, "dominio": dom,
                        "keyword": k.get("keyword"), "clicks": k.get("clicks"),
                        "traffic_share": k.get("traffic_share"), "volume": k.get("volume"),
                        "position": k.get("position"), "difficulty": k.get("difficulty"),
                        "cpc": k.get("cpc"), "primary_intent": k.get("primary_intent"),
                        "secondary_intent": k.get("secondary_intent"),
                        "zero_clicks_share": k.get("zero_clicks_share"),
                        "competition": k.get("competition"),
                        "top_url": k.get("top_url"), "pagina": pag + 1,
                    })
                total_dom += len(registros)
                if len(registros) < limit:
                    break            # última página
            if estado == "dry":
                print(f"  {dom:40s} {mes}  [dry] {paginas} pág × {limit} kw")
            else:
                marca = "∅ vacío" if total_dom == 0 else f"{total_dom:4d} kw"
                print(f"  {dom:40s} {mes}  {marca}")
    return filas


# --------------------------------------------------------------------------- #
# R2 — KEYWORDS OVERVIEW  (17 cr/keyword)
# --------------------------------------------------------------------------- #

def r2_keywords_overview(sw: SimilarWeb, keywords: list[str], meses: list[str]) -> list[dict]:
    print(f"\nR2 · Keywords Overview — {len(keywords)} kw × {len(meses)} meses  ⚠ 17 cr c/u")
    filas, snap = [], date.today().isoformat()
    for kw in keywords:
        for mes in meses:
            data, estado = sw.get(
                f"/v4/keywords/{kw_path(kw)}/analysis/overview",
                {"start_date": mes, "end_date": mes, "country": COUNTRY,
                 "web_source": WEB_SOURCE},
                costo=COSTO["r2"],
            )
            if estado == "dry":
                continue
            time.sleep(SLEEP)
            if estado.startswith("error"):
                print(f"  {kw:46s} {mes}  ✗ {estado}")
                continue
            d = (data or {}).get("data") or data or {}
            if not d:
                print(f"  {kw:46s} {mes}  ∅ vacío")
                continue
            cpc = d.get("cpc_range") or {}
            filas.append({
                "snapshot": snap, "mes": mes, "pais": COUNTRY, "keyword": kw,
                "volume": d.get("volume"), "clicks": d.get("clicks"),
                "zero_clicks": d.get("zero_clicks"),
                "organic_difficulty": d.get("organic_difficulty"),
                "paid_competition": d.get("paid_competition"),
                "intent": d.get("intent"),
                "cpc_low": cpc.get("low"), "cpc_high": cpc.get("high"),
                "total_spent": d.get("total_spent"),
            })
            print(f"  {kw:46s} {mes}  ✓")
    return filas


# --------------------------------------------------------------------------- #
# R3 — SERP PLAYERS
# --------------------------------------------------------------------------- #

def r3_serp_players(sw: SimilarWeb, keywords: list[str], meses: list[str],
                    limit: int = 25) -> list[dict]:
    print(f"\nR3 · SERP Players — {len(keywords)} kw × {len(meses)} meses (limit {limit})")
    filas, snap = [], date.today().isoformat()
    for kw in keywords:
        for mes in meses:
            data, estado = sw.get(
                f"/v4/keywords/{kw_path(kw)}/analysis/competitors",
                {"start_date": mes, "end_date": mes, "country": COUNTRY,
                 "traffic_source": "Organic", "web_source": WEB_SOURCE,
                 "limit": str(limit), "sort": "Clicks"},
                costo=COSTO["r3"] * limit,
            )
            if estado == "dry":
                continue
            time.sleep(SLEEP)
            if estado.startswith("error"):
                print(f"  {kw:46s} {mes}  ✗ {estado}")
                continue
            registros = extraer_lista(data or {}, "data", "competitors")
            for pos, c in enumerate(registros, 1):
                filas.append({
                    "snapshot": snap, "mes": mes, "pais": COUNTRY, "keyword": kw,
                    "rank_en_respuesta": pos,
                    "dominio": c.get("domain") or c.get("site"),
                    "clicks": c.get("clicks"), "traffic_share": c.get("traffic_share"),
                    "position": c.get("position"), "top_url": c.get("top_url"),
                })
            marca = "∅ vacío (bajo umbral del panel)" if not registros else f"{len(registros):3d} dominios"
            print(f"  {kw:46s} {mes}  {marca}")
    return filas


# --------------------------------------------------------------------------- #
# R4 — KEYWORD GAP (derivado de R1)
# --------------------------------------------------------------------------- #

def r4_keyword_gap(filas_r1: list[dict], dom_a: str = GAP_A, dom_b: str = GAP_B) -> list[dict]:
    print(f"\nR4 · Keyword Gap — {dom_a} vs {dom_b} (derivado de R1)")
    if pd is None:
        print("  pandas no instalado; omitido")
        return []
    if not filas_r1:
        print("  R1 vacío: corré R1 primero")
        return []
    df = pd.DataFrame(filas_r1)
    sub = df[df["dominio"].isin([dom_a, dom_b])]
    if sub.empty:
        print("  ninguno de los dos dominios tiene filas en R1")
        return []
    piv = sub.groupby(["keyword", "dominio"])["clicks"].sum().unstack(fill_value=0)
    for d in (dom_a, dom_b):
        if d not in piv.columns:
            piv[d] = 0
    piv = piv.reset_index().rename(columns={dom_a: "clicks_a", dom_b: "clicks_b"})
    piv["gap_clicks"] = piv["clicks_b"] - piv["clicks_a"]
    piv["situacion"] = piv.apply(
        lambda r: "solo_competidor" if r.clicks_a == 0 and r.clicks_b > 0
        else ("solo_triviantes" if r.clicks_b == 0 and r.clicks_a > 0 else "ambos"), axis=1)
    piv["dominio_a"], piv["dominio_b"] = dom_a, dom_b
    piv["snapshot"] = date.today().isoformat()
    piv = piv.sort_values("gap_clicks", ascending=False)
    print(f"  {(piv.situacion == 'solo_competidor').sum()} keywords que solo captura {dom_b}")
    return piv.to_dict("records")


# --------------------------------------------------------------------------- #
# R5 — SIMILAR SITES
# --------------------------------------------------------------------------- #

def r5_similar_sites(sw: SimilarWeb, semillas: list[str], limit: int = 40) -> list[dict]:
    print(f"\nR5 · Similar Sites — {len(semillas)} semillas × {limit} filas")
    filas, snap = [], date.today().isoformat()
    for dom in semillas:
        data, estado = sw.get(
            f"/v4/website/{dom}/similar-sites/similarsites",
            {"format": "json", "limit": str(limit)},
            costo=COSTO["r5"] * limit,
        )
        if estado == "dry":
            continue
        time.sleep(SLEEP)
        if estado.startswith("error"):
            print(f"  {dom:40s} ✗ {estado}")
            continue
        registros = extraer_lista(data or {}, "similar_sites", "data")
        for s in registros:
            filas.append({
                "snapshot": snap, "dominio_semilla": dom,
                "dominio_similar": s.get("url") or s.get("domain"),
                "similarity_score": s.get("score") or s.get("similarity_score"),
            })
        marca = "∅ vacío" if not registros else f"{len(registros):3d} similares"
        print(f"  {dom:40s} {marca}")
    return filas


# --------------------------------------------------------------------------- #
# R6a — VISITAS TOTALES (dimensionamiento)
#   GET /v1/website/{dom}/total-traffic-and-engagement/visits
#   A diferencia de R1/R2/R3, este endpoint SÍ acepta un rango de meses en
#   una sola llamada. 1 crédito por resultado (= por mes devuelto).
#
#   OJO CON LA INTERPRETACIÓN:
#   country=co  -> visitas DESDE Colombia = colombianos comprando en cualquier
#                  destino (Madrid, Cancún...). NO es "producto colombiano".
#   country=world -> tamaño global del sitio, para calcular el share Colombia.
#   El corte de producto colombiano sale de R6b, no de acá.
# --------------------------------------------------------------------------- #

def r6_visits(sw: SimilarWeb, dominios: list[str], meses: list[str],
              paises: tuple[str, ...] = ("co", "world"),
              main_domain_only: bool = False) -> list[dict]:
    ini, fin = meses[0], meses[-1]
    print(f"\nR6a · Visitas — {len(dominios)} dominios × {len(paises)} países "
          f"({ini} a {fin}, {len(meses)} meses por llamada)")
    filas, snap = [], date.today().isoformat()
    for dom in dominios:
        for pais in paises:
            data, estado = sw.get(
                f"/v1/website/{dom}/total-traffic-and-engagement/visits",
                {"start_date": ini, "end_date": fin, "country": pais,
                 "granularity": "monthly", "format": "json",
                 "main_domain_only": str(main_domain_only).lower()},
                costo=COSTO["r6_visits"] * len(meses),
            )
            if estado == "dry":
                print(f"  {dom:26s} {pais:6s} [dry] {len(meses)} meses")
                continue
            time.sleep(SLEEP)
            if estado.startswith("error"):
                print(f"  {dom:26s} {pais:6s} ✗ {estado}")
                continue
            registros = extraer_lista(data or {}, "visits", "data")
            for v in registros:
                fecha = (v.get("date") or "")[:7]
                filas.append({
                    "snapshot": snap, "mes": fecha, "pais": pais, "dominio": dom,
                    "visits": v.get("visits"),
                    "incluye_subdominios": not main_domain_only,
                })
            marca = "∅ vacío" if not registros else f"{len(registros):3d} meses"
            print(f"  {dom:26s} {pais:6s} {marca}")
    return filas


# --------------------------------------------------------------------------- #
# R6b — POPULAR PAGES (el corte de PRODUCTO COLOMBIANO)
#   GET /v5/website-content/pages/popular-pages/aggregated
#   3 créditos por fila. ⚠ Requiere el add-on "Popular Pages" en tu suscripción:
#   si no lo tenés, devuelve 403 y el script lo va a reportar como error.
#
#   Acá está la respuesta a "tamaño de producto colombiano": se piden las
#   páginas más visitadas de cada OTA y se marca cuáles corresponden a destinos
#   colombianos según su URL. Con country=world medís el mercado de productos
#   colombianos comprado por cualquiera; con country=co, solo por colombianos.
# --------------------------------------------------------------------------- #

def _es_colombia(path: str) -> tuple[bool, str]:
    p = (path or "").lower()
    for token in PATHS_COLOMBIA:
        if token in p:
            return True, token
    return False, ""


def r6_popular_pages(sw: SimilarWeb, dominios: list[str], meses: list[str],
                     paises: tuple[str, ...] = ("co", "world"),
                     limit: int = 100) -> list[dict]:
    ini, fin = meses[0], meses[-1]
    print(f"\nR6b · Popular Pages — {len(dominios)} dominios × {len(paises)} países "
          f"(limit {limit})  ⚠ requiere add-on")
    filas, snap = [], date.today().isoformat()
    for dom in dominios:
        for pais in paises:
            data, estado = sw.get(
                "/v5/website-content/pages/popular-pages/aggregated",
                {"domain": dom, "start_date": ini, "end_date": fin,
                 "country": "ww" if pais == "world" else pais,
                 "granularity": "monthly", "classify": "all",
                 "format": "json", "limit": str(limit)},
                costo=COSTO["r6_pages"] * limit,
            )
            if estado == "dry":
                print(f"  {dom:26s} {pais:6s} [dry] {limit} filas")
                continue
            time.sleep(SLEEP)
            if estado.startswith("error"):
                nota = " (¿falta el add-on Popular Pages?)" if "403" in estado else ""
                print(f"  {dom:26s} {pais:6s} ✗ {estado}{nota}")
                continue
            registros = extraer_lista(data or {}, "popular_pages", "pages", "data")
            n_col = 0
            for p in registros:
                url = p.get("page") or p.get("url") or p.get("url_path") or ""
                es_col, token = _es_colombia(url)
                n_col += es_col
                filas.append({
                    "snapshot": snap, "mes": f"{ini}..{fin}", "pais": pais,
                    "dominio": dom, "url_path": url,
                    "share": p.get("share"), "change": p.get("change"),
                    "es_producto_colombia": es_col, "match_path": token,
                })
            marca = "∅ vacío" if not registros else f"{len(registros):3d} pág ({n_col} Colombia)"
            print(f"  {dom:26s} {pais:6s} {marca}")
    return filas


# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description="Extracción SimilarWeb — atracciones Colombia")
    ap.add_argument("--report", default="all",
                    choices=["all", "r1", "r2", "r3", "r4", "r5", "r6", "r6a", "r6b"])
    ap.add_argument("--months", nargs="*", default=None,
                    help="YYYY-MM. Default: últimos 12 para R1, últimos 11 (sin el mes en "
                         "curso) para R6, penúltimo mes para R2/R3")
    ap.add_argument("--limit-r1", type=int, default=100, help="keywords por página en R1")
    ap.add_argument("--max-pages", type=int, default=5, help="páginas máx. por dominio grande")
    ap.add_argument("--limit-r3", type=int, default=25, help="dominios por keyword en R3")
    ap.add_argument("--r5-seeds", nargs="*", default=None, help="semillas para R5")
    ap.add_argument("--max-keywords", type=int, default=0)
    ap.add_argument("--keywords-file", default="", help="CSV con columna 'keyword'")
    ap.add_argument("--overwrite", action="store_true", help="nombre sin fecha (sobreescribe)")
    ap.add_argument("--main-domain-only", action="store_true",
                    help="R6a: excluir subdominios (por defecto se incluyen)")
    ap.add_argument("--limit-r6b", type=int, default=100, help="páginas por dominio en R6b")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    api_key = os.environ.get("SIMILARWEB_API_KEY", "")
    if not api_key and not args.dry_run:
        print("Falta SIMILARWEB_API_KEY en el entorno.")
        return 1

    keywords = list(KEYWORDS)
    if args.keywords_file:
        with open(args.keywords_file, encoding="utf-8-sig") as f:
            keywords = [r["keyword"].strip() for r in csv.DictReader(f) if r.get("keyword")]
        print(f"Keywords cargadas de {args.keywords_file}: {len(keywords)}")
    if args.max_keywords:
        keywords = keywords[:args.max_keywords]

    meses_r1 = args.months or meses_ultimos_12()
    # El dataset de keywords tiene ~2 meses de rezago (más que tráfico web): si se usa
    # el último mes calendario la API responde 400 "Dates not in range" (error_code 101).
    meses_kw = args.months or [meses_ultimos_12()[-2]]
    # total-traffic-and-engagement/visits (R6) tiene el mismo rezago que las keywords:
    # el último mes calendario todavía no está disponible, se descarta del rango.
    meses_r6 = args.months or meses_ultimos_12()[:-1]
    semillas = args.r5_seeds or R5_SEEDS

    sw = SimilarWeb(api_key or "DRY", dry_run=args.dry_run)
    filas_r1: list[dict] = []

    try:
        if args.report in ("all", "r1", "r4"):
            filas_r1 = r1_website_keywords(sw, DOMINIOS, meses_r1,
                                           limit=args.limit_r1, max_pages=args.max_pages)
            if args.report != "r4":
                escribir_csv("r1_website_keywords", filas_r1, args.overwrite)
        if args.report in ("all", "r2"):
            escribir_csv("r2_keywords_overview",
                         r2_keywords_overview(sw, keywords, meses_kw), args.overwrite)
        if args.report in ("all", "r3"):
            escribir_csv("r3_serp_players",
                         r3_serp_players(sw, keywords, meses_kw, limit=args.limit_r3),
                         args.overwrite)
        if args.report in ("all", "r4"):
            escribir_csv("r4_keyword_gap", r4_keyword_gap(filas_r1), args.overwrite)
        if args.report in ("all", "r5"):
            escribir_csv("r5_similar_sites",
                         r5_similar_sites(sw, semillas), args.overwrite)
        if args.report in ("all", "r6", "r6a"):
            escribir_csv("r6_visits",
                         r6_visits(sw, DOMINIOS_R6, meses_r6,
                                   main_domain_only=args.main_domain_only),
                         args.overwrite)
        if args.report in ("all", "r6", "r6b"):
            escribir_csv("r6_popular_pages",
                         r6_popular_pages(sw, DOMINIOS_R6, meses_r6,
                                          limit=args.limit_r6b),
                         args.overwrite)
    finally:
        sw.close()

    t = sw.tally
    print("\n" + "─" * 62)
    print(f"Llamadas   ok:{t['ok']}   vacías:{t['empty']}   con error:{t['error']}")
    if t["error"]:
        print("  ⚠ Hubo errores: revisá las líneas con ✗ arriba. NO son 'sin datos'.")
    if t["empty"] and not t["error"]:
        print("  Las vacías son ausencia de datos en el panel, no fallas técnicas.")
    print(f"{'[DRY RUN] ' if args.dry_run else ''}Créditos estimados: {sw.credits:,.0f}")
    if args.dry_run:
        print("Nada se llamó ni se gastó.")
    return 0


if __name__ == "__main__":
    sys.exit(main())