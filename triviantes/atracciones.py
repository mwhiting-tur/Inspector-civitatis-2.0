#!/usr/bin/env python3
"""
Scraper de supply de Triviantes (18 microsites de parques) — corrida única local.

Por cada dominio intenta, en orden:
  1. WooCommerce Store API  ->  /wp-json/wc/store/v1/products   (JSON, sin auth, sin JS)
  2. Fallback HTML          ->  /shop/  parseado con selectolax  (si el Store API está cerrado)

Salida (en la carpeta actual):
  triviantes_supply.csv   -> 1 fila por producto/variante
  triviantes_errors.csv   -> hosts que fallaron (solo si hubo alguno)

Instalar y correr:
  pip install httpx selectolax
  python triviantes_scraper.py
  # opcional, probar un solo parque:
  python triviantes_scraper.py --only panaca
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass, asdict, fields as dc_fields
from datetime import datetime, timezone

import httpx

try:
    from selectolax.parser import HTMLParser
except ImportError:
    HTMLParser = None

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #

# (slug, nombre del parque, host)  -- columna C del sheet
HOSTS: list[tuple[str, str, str]] = [
    ("acuario_mundo_marino",  "Acuario Mundo Marino",         "acuariomundomarino.triviantes.com"),
    ("recinto_pensamiento",   "Recinto del Pensamiento",      "recintodelpensamiento.triviantes.com"),
    ("aviario_nacional",      "Aviario Nacional",             "aviarionacional.triviantes.com"),
    ("bioparque_ukumari",     "Bioparque Ukumarí",            "bioparqueukumari.triviantes.com"),
    ("caribe_aventura",       "Caribe Aventura",              "caribeaventura.triviantes.com"),
    ("catedral_sal",          "Catedral de Sal",              "catedraldesal.triviantes.com"),
    ("mil_caminos",           "Laberinto Mil Caminos",        "milcaminos.triviantes.com"),
    ("las_bailarinas",        "Las Bailarinas",               "lasbailarinas.triviantes.com"),
    ("los_arrieros",          "Los Arrieros",                 "losarrieros.triviantes.com"),
    ("mundo_aventura",        "Mundo Aventura",               "mundoaventura.triviantes.com"),
    ("mina_sal_nemocon",      "Mina de Sal de Nemocón",       "minadesalnemocon.triviantes.com"),
    ("panaca",                "Panaca",                       "panacaquindio.triviantes.com"),
    ("parque_kanaloa",        "Parque Kanaloa",               "parquekanaloa.triviantes.com"),
    ("parque_recuca",         "Parque Recuca",                "parquerecuca.triviantes.com"),
    ("playa_hawai",           "Playa Hawái",                  "playahawai.triviantes.com"),
    ("termales_santa_rosa",   "Termales Santa Rosa de Cabal", "alianzas.triviantes.com"),
    ("termales_san_vicente",  "Termales San Vicente",         "sanvicentetermales.triviantes.com"),
    ("zoologico_cali",        "Zoológico de Cali",            "zoologicodecali.triviantes.com"),
]

USER_AGENT = "tur-supply-research/1.0 (competitive price monitoring)"
TIMEOUT = 25.0
MAX_RETRIES = 3
BACKOFF_BASE = 1.5      # segundos entre reintentos: 1.5, 3, ...
POLITE_SLEEP = 0.4     # pausa entre requests al mismo host
PER_PAGE = 100


# --------------------------------------------------------------------------- #
# MODELO DE SALIDA
# --------------------------------------------------------------------------- #

@dataclass
class Row:
    scrape_ts: str
    slug: str
    parque: str
    host: str
    source: str            # store_api | shop_html
    product_id: str
    product_name: str
    sku: str
    categories: str        # "cat1|cat2"
    price_regular: float | None
    price_sale: float | None
    price_current: float | None
    currency: str
    is_on_sale: bool
    is_in_stock: bool | None
    is_variable: bool
    price_min: float | None
    price_max: float | None
    permalink: str


# --------------------------------------------------------------------------- #
# HELPERS
# --------------------------------------------------------------------------- #

def make_client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        follow_redirects=True,
        timeout=TIMEOUT,
        verify=False,
    )


def get_with_retry(client: httpx.Client, url: str, params: dict | None = None) -> httpx.Response | None:
    """GET con reintentos en 429/5xx/timeouts. Devuelve None si agota reintentos."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.get(url, params=params)
            if r.status_code in (429, 500, 502, 503, 504):
                raise httpx.HTTPStatusError(f"status {r.status_code}", request=r.request, response=r)
            return r
        except (httpx.TransportError, httpx.HTTPStatusError) as e:
            if attempt == MAX_RETRIES:
                print(f"    GET agotó reintentos {url} ({e})")
                return None
            time.sleep(BACKOFF_BASE * attempt)
    return None


def money(prices: dict, key: str) -> float | None:
    """Store API entrega montos como string en 'unidad menor'. En COP minor_unit=0."""
    raw = prices.get(key)
    if raw in (None, "", "null"):
        return None
    try:
        minor = int(prices.get("currency_minor_unit", 0) or 0)
        return round(int(raw) / (10 ** minor), 2)
    except (ValueError, TypeError):
        return None


def _parse_cop(text: str) -> float | None:
    """'$99.000' -> 99000.0  (en COP el '.' es separador de miles)."""
    digits = "".join(ch for ch in text if ch.isdigit())
    return float(digits) if digits else None


# --------------------------------------------------------------------------- #
# STORE API
# --------------------------------------------------------------------------- #

def fetch_store_products(client: httpx.Client, host: str) -> list[dict] | None:
    """Lista cruda de productos vía Store API, o None si el endpoint no da JSON."""
    base = f"https://{host}/wp-json/wc/store/v1/products"
    products: list[dict] = []
    page = 1
    while True:
        r = get_with_retry(client, base, params={"per_page": PER_PAGE, "page": page})
        time.sleep(POLITE_SLEEP)
        if r is None:
            return products or None
        if r.status_code == 404:
            return None                     # Store API no disponible -> fallback
        if r.status_code != 200:
            print(f"    productos page={page} status={r.status_code}")
            return products or None
        try:
            batch = r.json()
        except ValueError:
            return None                     # respondió HTML, no JSON
        if not isinstance(batch, list) or not batch:
            break
        products.extend(batch)
        total_pages = r.headers.get("X-WP-TotalPages")
        if total_pages and page >= int(total_pages):
            break
        if len(batch) < PER_PAGE:
            break
        page += 1
    return products


def rows_from_store(host: str, slug: str, parque: str, ts: str, products: list[dict]) -> list[Row]:
    rows: list[Row] = []
    for p in products:
        prices = p.get("prices", {}) or {}
        pr = prices.get("price_range") or {}
        cat_names = "|".join(c.get("name", "") for c in (p.get("categories") or []))
        rows.append(Row(
            scrape_ts=ts, slug=slug, parque=parque, host=host, source="store_api",
            product_id=str(p.get("id", "")),
            product_name=(p.get("name") or "").strip(),
            sku=(p.get("sku") or "").strip(),
            categories=cat_names,
            price_regular=money(prices, "regular_price"),
            price_sale=money(prices, "sale_price"),
            price_current=money(prices, "price"),
            currency=prices.get("currency_code", "COP"),
            is_on_sale=bool(p.get("on_sale")),
            is_in_stock=p.get("is_in_stock"),
            is_variable=p.get("type") == "variable" or bool(pr),
            price_min=money(pr, "min_amount") if pr else None,
            price_max=money(pr, "max_amount") if pr else None,
            permalink=p.get("permalink", ""),
        ))
    return rows


# --------------------------------------------------------------------------- #
# FALLBACK HTML  (/shop/)
# --------------------------------------------------------------------------- #

def rows_from_shop_html(client: httpx.Client, host: str, slug: str, parque: str, ts: str) -> list[Row]:
    if HTMLParser is None:
        print("    selectolax no instalado; sin fallback HTML")
        return []
    r = get_with_retry(client, f"https://{host}/shop/")
    time.sleep(POLITE_SLEEP)
    if r is None or r.status_code != 200:
        return []
    tree = HTMLParser(r.text)
    rows: list[Row] = []
    for li in tree.css("li.product"):
        name_node = li.css_first(".woocommerce-loop-product__title, h2, h3")
        name = name_node.text(strip=True) if name_node else ""
        bdis = li.css(".price bdi")
        price_current = _parse_cop(bdis[-1].text()) if bdis else None
        price_regular = _parse_cop(bdis[0].text()) if len(bdis) >= 2 else price_current
        link = li.css_first("a.woocommerce-LoopProduct-link, a")
        permalink = link.attributes.get("href", "") if link else ""
        if name:
            rows.append(Row(
                scrape_ts=ts, slug=slug, parque=parque, host=host, source="shop_html",
                product_id="", product_name=name, sku="", categories="",
                price_regular=price_regular, price_sale=None, price_current=price_current,
                currency="COP", is_on_sale=(price_regular != price_current),
                is_in_stock=None, is_variable=False, price_min=None, price_max=None,
                permalink=permalink,
            ))
    return rows


# --------------------------------------------------------------------------- #
# ORQUESTA UN HOST
# --------------------------------------------------------------------------- #

def scrape_host(slug: str, parque: str, host: str, ts: str) -> tuple[list[Row], dict | None]:
    print(f"→ {parque} ({host})")
    with make_client() as client:
        try:
            products = fetch_store_products(client, host)
            if products is not None:
                rows = rows_from_store(host, slug, parque, ts, products)
                print(f"    ✓ Store API: {len(rows)} productos")
                if rows:
                    return rows, None
            rows = rows_from_shop_html(client, host, slug, parque, ts)
            if rows:
                print(f"    ✓ fallback HTML: {len(rows)} productos")
                return rows, None
            print("    ✗ sin productos (Store API y HTML vacíos)")
            return [], {"host": host, "parque": parque, "reason": "sin productos"}
        except Exception as e:  # noqa: BLE001 — captura por host, no frena el run
            print(f"    ✗ {type(e).__name__}: {e}")
            return [], {"host": host, "parque": parque, "reason": f"{type(e).__name__}: {e}"}


# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description="Scraper de supply de Triviantes (corrida única, CSV)")
    ap.add_argument("--only", default="", help="scrapea solo estos slugs (coma-separados)")
    ap.add_argument("--out", default="triviantes_supply.csv", help="archivo CSV de salida")
    args = ap.parse_args()

    ts = datetime.now(timezone.utc).isoformat()
    targets = HOSTS
    if args.only:
        wanted = {s.strip() for s in args.only.split(",")}
        targets = [h for h in HOSTS if h[0] in wanted]

    all_rows: list[Row] = []
    all_errors: list[dict] = []
    for slug, parque, host in targets:          # secuencial: simple y predecible en local
        rows, err = scrape_host(slug, parque, host, ts)
        all_rows.extend(rows)
        if err:
            all_errors.append(err)

    all_rows.sort(key=lambda r: (r.parque, r.product_name))

    field_names = [f.name for f in dc_fields(Row)]
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:   # utf-8-sig: acentos OK en Excel
        w = csv.DictWriter(f, fieldnames=field_names)
        w.writeheader()
        for r in all_rows:
            w.writerow(asdict(r))
    print(f"\nEscrito {args.out} — {len(all_rows)} filas")

    if all_errors:
        with open("triviantes_errors.csv", "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=["host", "parque", "reason"])
            w.writeheader()
            w.writerows(all_errors)
        print(f"Escrito triviantes_errors.csv — {len(all_errors)} hosts con error")

    print(f"Resumen: {len(all_rows)} productos en {len(targets) - len(all_errors)}/{len(targets)} hosts")
    return 0


if __name__ == "__main__":
    sys.exit(main())