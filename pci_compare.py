#!/usr/bin/env python3.10
"""
PCI Compare — Price Comparison Index: Tur.com vs GetYourGuide
=============================================================
Steps:
  1. Load & filter products for the target country (Chile default)
  2. Deduplicate each side independently (same city + similar name → keep one)
  3. Match unique Tur activities vs unique GYG activities
  4. Scrape prices — GYG URLs cached so each is fetched only once
  5. Calculate PCI = tur_price_usd / gyg_price_usd
  6. Save results to CSV

Usage:
  python3.10 pci_compare.py

Config:
  Edit the CONFIGURATION block below.
"""

import csv
import json
import os
import re
import time
import random
import unicodedata
import concurrent.futures
import threading
from collections import defaultdict

from curl_cffi import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

# ================================================================
# CONFIGURATION
# ================================================================

TUR_FILE  = "tur/productos_con_url_es_15-05.csv"
GYG_FILE  = "gyg/tours_chile_IDs_2026-05-25.csv"
OUT_DIR   = "pci"
OUT_MATCHES = os.path.join(OUT_DIR, "matches.csv")
OUT_PRICES  = os.path.join(OUT_DIR, "pci_results.csv")

# CLP → USD conversion rate (update periodically)
CLP_TO_USD = 1 / 940.0   # 1 USD ≈ 940 CLP

# Fuzzy match threshold for final Tur↔GYG matching (0–100)
MATCH_THRESHOLD = 52

# Deduplication threshold within each platform (higher = stricter)
# Activities scoring above this within the same city are considered duplicates
TUR_DEDUP_THRESHOLD = 85
GYG_DEDUP_THRESHOLD = 80

# GYG global rate limit: min seconds between any two GYG requests across all threads
GYG_MIN_INTERVAL = 2.0

# Retry on 403
GYG_MAX_RETRIES  = 2
GYG_BACKOFF_BASE = 6   # seconds (6 → 12 → 24 …)

# Concurrent threads
MAX_WORKERS = 3

# ================================================================
# CHILE CITY SLUGS — Tur URL segment after /es/
# ================================================================
CHILE_TUR_CITIES = {
    "ancud-272", "antofagasta-204", "arica-199", "balmaceda-288",
    "calama-203", "canete-254", "cartagena-chile-220", "casablanca-223",
    "castro-271", "chanaral-208", "chanco-379", "chile-chico-374",
    "chiloe-373", "chusmiza-367", "cisnes-285", "cochamo-281",
    "cochrane-295", "concepcion-252", "concon-364", "copiapo-205",
    "coquimbo-210", "coyhaique-289", "curacautin-260", "curico-242",
    "frutillar-280", "futaleufu-282", "hornopiren-283", "hualpen-255",
    "hualqui-251", "iquique-201", "isla-robinson-crusoe-226",
    "la-junta-368", "la-serena-209", "la-union-268", "lago-ranco-266",
    "lican-ray-259", "llanquihue-278", "lo-barnechea-229",
    "los-angeles-250", "los-lagos-267", "los-vilos-215",
    "malalcahuello-261", "melipeuco-258", "molina-244", "nancagua-239",
    "navidad-232", "olmue-365", "ovalle-211", "paihuano-214",
    "palmilla-236", "panguipulli-372", "panimavida-243", "panquehue-376",
    "papudo-216", "pichidegua-237", "pichilemu-230", "pinto-247",
    "pirque-361", "placilla-238", "portezuelo-249", "pucon-256",
    "puerto-aysen-290", "puerto-bertrand-294", "puerto-guadal-293",
    "puerto-montt-273", "puerto-murta-297", "puerto-natales-298",
    "puerto-rio-tranquilo-291", "puerto-sanchez-296", "puerto-varas-270",
    "puerto-williams-301", "putre-200", "quellon-274", "quillon-246",
    "quilpue-366", "rancagua-231", "rapa-nui-225", "rapel-369",
    "rio-bueno-269", "san-antonio-222", "san-fabian-248",
    "san-fernando-235", "san-jose-de-maipo-227", "san-pedro-de-atacama-202",
    "san-vicente-de-tagua-tagua-240", "santa-cruz-233", "santiago-228",
    "santo-domingo-219", "talca-245", "temuco-262", "tierra-del-fuego-371",
    "torres-del-paine-300", "tortel-287", "valdivia-265", "vallenar-370",
    "valparaiso-221", "vichuquen-377", "vicuna-212", "vilcun-263",
    "villa-cerro-castillo-292", "villa-ohiggins-286", "villarrica-257",
    "vina-del-mar-224", "zapallar-217",
}

# ================================================================
# BILINGUAL KEYWORD TRANSLATION  (Spanish → English)
# ================================================================
ES_TO_EN = {
    "aguas calientes":  "hot springs",
    "astronomico":      "stargazing",
    "astronómica":      "stargazing",
    "aerostatico":      "balloon",
    "arcoiris":         "rainbow",
    "amanecer":         "sunrise",
    "atardecer":        "sunset",
    "ascenso":          "summit",
    "ballenas":         "whales",
    "bosque":           "forest",
    "bicicleta":        "bike",
    "cabalgata":        "horseback",
    "cascadas":         "waterfalls",
    "cascada":          "waterfall",
    "ciclismo":         "cycling",
    "ciudad":           "city",
    "cima":             "summit",
    "cumbre":           "summit",
    "cuevas":           "caves",
    "cueva":            "cave",
    "degustacion":      "tasting",
    "delfines":         "dolphins",
    "desierto":         "desert",
    "estrellas":        "stargazing",
    "estrella":         "stars",
    "geiser":           "geyser",
    "geysers":          "geysers",
    "geyser":           "geyser",
    "globo":            "balloon",
    "isla":             "island",
    "islas":            "islands",
    "kayak":            "kayak",
    "laguna":           "lagoon",
    "lagunas":          "lagoons",
    "luna":             "moon",
    "mina":             "mine",
    "minas":            "mines",
    "nieve":            "snow",
    "navegacion":       "sailing",
    "nocturno":         "night",
    "nocturna":         "night",
    "parque":           "park",
    "piedras":          "stones",
    "pinguinos":        "penguins",
    "playa":            "beach",
    "playas":           "beaches",
    "privado":          "private",
    "privada":          "private",
    "roja":             "red",
    "rojas":            "red",
    "rafting":          "rafting",
    "salar":            "salt flat",
    "salares":          "salt flats",
    "selva":            "jungle",
    "termas":           "hot springs",
    "valle":            "valley",
    "viñedo":           "vineyard",
    "vinedos":          "vineyards",
    "vino":             "wine",
    "volcan":           "volcano",
    "vuelo":            "flight",
    "zipline":          "zipline",
}

STOP_WORDS = {
    # Spanish
    "tour", "tours", "excursion", "actividad", "desde", "hacia", "con", "por",
    "del", "una", "unos", "unas", "los", "las", "entre", "maravilloso",
    "increible", "espectacular", "unico", "unica", "aventura", "experiencia",
    "visita", "recorrido", "paseo", "completo", "completa", "incluido",
    "incluida", "guiado", "guiada", "guiados", "guiadas",
    # English
    "the", "and", "from", "with", "for", "day", "half", "full", "private",
    "shared", "guided", "group", "small", "big", "amazing", "incredible",
    "spectacular", "unique", "best", "experience", "excursion", "activity",
    "trip", "visit", "discover", "explore", "enjoy", "adventure",
}

# ================================================================
# TEXT NORMALISATION
# ================================================================

def remove_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def normalize_name(text: str) -> str:
    """Lowercase, remove accents, translate ES→EN keywords, strip stop words."""
    if not text:
        return ""
    t = remove_accents(text.lower())
    for es, en in sorted(ES_TO_EN.items(), key=lambda x: -len(x[0])):
        t = re.sub(r"\b" + re.escape(remove_accents(es)) + r"\b", en, t)
    words = [w for w in t.split() if w not in STOP_WORDS and len(w) > 1]
    return " ".join(words)


def similarity(a: str, b: str) -> float:
    """Composite score: 60% token_sort_ratio + 40% partial_ratio."""
    if not a or not b:
        return 0.0
    return 0.6 * fuzz.token_sort_ratio(a, b) + 0.4 * fuzz.partial_ratio(a, b)


def extract_city_tur(url: str) -> str:
    m = re.search(r"/es/([^/]+)/", url)
    if m:
        slug = re.sub(r"-\d+$", "", m.group(1))
        return slug.replace("-", " ").lower()
    return ""


def extract_city_gyg(ciudad_id: str) -> str:
    city = re.sub(r"-l\d+$", "", ciudad_id)
    return city.replace("-", " ").lower()


# ================================================================
# PHASE 1: LOAD & FILTER
# ================================================================

def load_tur_products(filepath: str) -> list[dict]:
    products = []
    with open(filepath, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            url = row.get("url", "")
            m = re.search(r"/es/([^/]+)/", url)
            if not m or m.group(1) not in CHILE_TUR_CITIES:
                continue
            row["city_slug"] = m.group(1)
            row["city_name"] = extract_city_tur(url)
            row["name_norm"] = normalize_name(row.get("product_name", ""))
            products.append(row)
    print(f"  Tur raw Chile products:     {len(products)}")
    return products


def load_gyg_products(filepath: str) -> list[dict]:
    products = []
    with open(filepath, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter=";"):
            row["city_name"] = extract_city_gyg(row.get("ciudad_id", ""))
            row["name_norm"] = normalize_name(row.get("titulo_referencia", ""))
            products.append(row)
    print(f"  GYG raw Chile products:     {len(products)}")
    return products


# ================================================================
# PHASE 2: DEDUPLICATION (within each platform)
# ================================================================

def deduplicate(products: list[dict], threshold: float,
                name_key: str = "name_norm",
                city_key: str = "city_name") -> list[dict]:
    """
    Within each city group, cluster products by fuzzy name similarity.
    Keep only one representative per cluster (the first encountered).
    Returns the deduplicated list and a mapping {kept_norm → [all_norms_in_cluster]}.
    """
    by_city = defaultdict(list)
    for p in products:
        by_city[p[city_key]].append(p)

    kept = []
    for city, group in by_city.items():
        used = [False] * len(group)
        for i, pi in enumerate(group):
            if used[i]:
                continue
            used[i] = True
            kept.append(pi)
            # Mark all similar items in this city as duplicates
            for j in range(i + 1, len(group)):
                if not used[j]:
                    s = similarity(pi[name_key], group[j][name_key])
                    if s >= threshold:
                        used[j] = True

    return kept


# ================================================================
# PHASE 3: MATCHING (unique Tur vs unique GYG)
# ================================================================

def match_activities(tur_products: list[dict],
                     gyg_products: list[dict]) -> list[dict]:
    """
    For each unique Tur product find the best unique GYG match.
    Same-city candidates are checked first; cross-city only if same-city
    fails to reach the threshold.
    """
    gyg_by_city = defaultdict(list)
    for p in gyg_products:
        gyg_by_city[p["city_name"]].append(p)

    matches = []
    for tur in tur_products:
        tur_city = tur["city_name"]
        tur_norm = tur["name_norm"]

        best_score  = 0.0
        best_match  = None
        same_city   = False

        for gyg in gyg_by_city.get(tur_city, []):
            s = similarity(tur_norm, gyg["name_norm"])
            if s > best_score:
                best_score, best_match, same_city = s, gyg, True

        # Cross-city fallback only if same-city didn't clear the threshold
        if best_score < MATCH_THRESHOLD:
            for gyg in gyg_products:
                if gyg["city_name"] == tur_city:
                    continue
                s = similarity(tur_norm, gyg["name_norm"])
                if s > best_score:
                    best_score, best_match, same_city = s, gyg, False

        if best_match and best_score >= MATCH_THRESHOLD:
            matches.append({
                "tur_id":        tur.get("internal_id", ""),
                "tur_name":      tur.get("product_name", ""),
                "tur_city":      tur_city,
                "tur_url":       tur.get("url", ""),
                "tur_norm":      tur_norm,
                "gyg_id":        best_match.get("tour_id", ""),
                "gyg_name":      best_match.get("titulo_referencia", ""),
                "gyg_city":      best_match.get("city_name", ""),
                "gyg_url":       best_match.get("url", ""),
                "gyg_norm":      best_match.get("name_norm", ""),
                "match_score":   round(best_score, 1),
                "same_city":     same_city,
                "tur_price_clp": None,
                "tur_currency":  None,
                "tur_price_usd": None,
                "gyg_price_usd": None,
                "gyg_currency":  None,
                "pci":           None,
                "scrape_status": "pending",
            })

    matches.sort(key=lambda x: (-x["same_city"], -x["match_score"]))
    return matches


# ================================================================
# PHASE 4: PRICE SCRAPING
# ================================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "es-ES,es;q=0.9",
}

# --- GYG global rate limiter ---
_gyg_lock      = threading.Lock()
_gyg_last_req  = 0.0

def _gyg_throttle() -> None:
    """Ensures all threads combined never hit GYG faster than GYG_MIN_INTERVAL."""
    global _gyg_last_req
    with _gyg_lock:
        wait = GYG_MIN_INTERVAL - (time.time() - _gyg_last_req)
        if wait > 0:
            time.sleep(wait + random.uniform(0, 0.8))
        _gyg_last_req = time.time()

# --- GYG price cache (url → (price, currency)) ---
_gyg_cache      = {}
_gyg_cache_lock = threading.Lock()


def scrape_tur_price(url: str) -> tuple:
    """Returns (price: float|None, currency: str)"""
    try:
        r = requests.get(url, headers=HEADERS, impersonate="chrome", timeout=25)
        if r.status_code != 200:
            return None, f"HTTP_{r.status_code}"

        # JSON-LD Product schema
        for script in BeautifulSoup(r.text, "html.parser").find_all(
                "script", type="application/ld+json"):
            try:
                data  = json.loads(script.string or "")
                graph = data if isinstance(data, list) else data.get("@graph", [data])
                for item in graph:
                    if not isinstance(item, dict) or item.get("@type") != "Product":
                        continue
                    offers = item.get("offers", {})
                    if isinstance(offers, list):
                        offers = offers[0]
                    if isinstance(offers, dict):
                        price    = offers.get("price") or offers.get("lowPrice")
                        currency = offers.get("priceCurrency", "CLP")
                        if price is not None:
                            return float(price), currency
            except Exception:
                pass

        # Regex fallback
        m = re.search(r'"price"\s*:\s*"?(\d+(?:\.\d+)?)"?', r.text)
        if m:
            return float(m.group(1)), "CLP"

        return None, "NOT_FOUND"
    except Exception as e:
        return None, f"ERROR: {str(e)[:60]}"


def scrape_gyg_price(url: str) -> tuple:
    """
    Returns (price: float|None, currency: str).
    Results are cached by URL so duplicate GYG URLs are only fetched once.
    Rate-limited globally; retries on 403 with backoff.
    """
    # Check cache first (no network needed)
    with _gyg_cache_lock:
        if url in _gyg_cache:
            return _gyg_cache[url]

    for attempt in range(GYG_MAX_RETRIES + 1):
        try:
            _gyg_throttle()

            r = requests.get(
                url,
                headers=HEADERS,
                cookies={"currency": "USD"},
                params={"currency": "USD"},
                impersonate="chrome",
                timeout=25,
            )

            if r.status_code == 403:
                if attempt < GYG_MAX_RETRIES:
                    backoff = GYG_BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 3)
                    time.sleep(backoff)
                    continue
                result = (None, "HTTP_403_blocked")
                break

            if r.status_code != 200:
                result = (None, f"HTTP_{r.status_code}")
                break

            soup = BeautifulSoup(r.text, "html.parser")

            # OG meta tags (most reliable on GYG)
            meta_price = soup.find("meta", property="product:price:amount")
            meta_cur   = soup.find("meta", property="product:price:currency")
            if meta_price:
                result = (float(meta_price["content"]),
                          meta_cur["content"] if meta_cur else "USD")
                break

            # JSON-LD fallback
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data   = json.loads(script.string or "")
                    offers = data.get("offers", {})
                    if isinstance(offers, list):
                        offers = offers[0]
                    if isinstance(offers, dict):
                        price = offers.get("price") or offers.get("lowPrice")
                        cur   = offers.get("priceCurrency", "USD")
                        if price is not None:
                            result = (float(price), cur)
                            break
                except Exception:
                    pass
            else:
                result = (None, "NOT_FOUND")
            break

        except Exception as e:
            if attempt < GYG_MAX_RETRIES:
                time.sleep(GYG_BACKOFF_BASE * (2 ** attempt))
                continue
            result = (None, f"ERROR: {str(e)[:60]}")
            break
    else:
        result = (None, "RETRY_EXHAUSTED")

    with _gyg_cache_lock:
        _gyg_cache[url] = result

    return result


def process_match(match: dict) -> dict:
    """Scrape both URLs and compute PCI for one matched pair."""
    out = match.copy()

    # Tur
    tur_price, tur_cur = scrape_tur_price(match["tur_url"])
    out["tur_price_clp"] = tur_price
    out["tur_currency"]  = tur_cur
    if tur_price is not None:
        out["tur_price_usd"] = round(
            tur_price * CLP_TO_USD if tur_cur == "CLP" else tur_price, 2
        )
    else:
        out["tur_price_usd"] = None

    # Brief pause before hitting GYG
    time.sleep(random.uniform(0.5, 1.2))

    # GYG (rate-limited + cached)
    gyg_price, gyg_cur = scrape_gyg_price(match["gyg_url"])
    out["gyg_price_usd"] = gyg_price
    out["gyg_currency"]  = gyg_cur

    # PCI
    tur_usd = out["tur_price_usd"]
    if tur_usd and gyg_price and gyg_price > 0:
        out["pci"]           = round(tur_usd / gyg_price, 4)
        out["scrape_status"] = "ok"
    else:
        out["pci"] = None
        missing    = []
        if not tur_usd:   missing.append(f"tur={tur_cur}")
        if not gyg_price: missing.append(f"gyg={gyg_cur}")
        out["scrape_status"] = "partial|" + "|".join(missing)

    return out


# ================================================================
# UTILITIES
# ================================================================

def save_csv(rows: list[dict], filepath: str) -> None:
    if not rows:
        print(f"  ⚠️  No rows to save → {filepath}")
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  💾 {len(rows)} rows → {filepath}")


def banner(title: str) -> None:
    print(f"\n{'='*62}\n  {title}\n{'='*62}")


# ================================================================
# MAIN
# ================================================================

def main():
    banner("PCI COMPARE: Tur.com vs GetYourGuide — Chile")

    # ── Phase 1: Load ───────────────────────────────────────────
    banner("PHASE 1 — Load")
    tur_raw = load_tur_products(TUR_FILE)
    gyg_raw = load_gyg_products(GYG_FILE)
    if not tur_raw or not gyg_raw:
        print("❌ Missing data. Check file paths."); return

    # ── Phase 2: Deduplicate ────────────────────────────────────
    banner("PHASE 2 — Deduplicate")
    tur_dedup = deduplicate(tur_raw, TUR_DEDUP_THRESHOLD)
    gyg_dedup = deduplicate(gyg_raw, GYG_DEDUP_THRESHOLD)
    print(f"  Tur: {len(tur_raw)} → {len(tur_dedup)} unique  "
          f"(removed {len(tur_raw) - len(tur_dedup)} duplicates, threshold={TUR_DEDUP_THRESHOLD})")
    print(f"  GYG: {len(gyg_raw)} → {len(gyg_dedup)} unique  "
          f"(removed {len(gyg_raw) - len(gyg_dedup)} duplicates, threshold={GYG_DEDUP_THRESHOLD})")

    # ── Phase 3: Match ──────────────────────────────────────────
    banner("PHASE 3 — Match")
    matches = match_activities(tur_dedup, gyg_dedup)
    same    = sum(1 for m in matches if m["same_city"])
    print(f"  Matched pairs:    {len(matches)}  (threshold={MATCH_THRESHOLD})")
    print(f"  ↳ same-city:      {same}")
    print(f"  ↳ cross-city:     {len(matches) - same}")

    unique_gyg_urls = len(set(m["gyg_url"] for m in matches))
    print(f"  Unique GYG URLs:  {unique_gyg_urls}  (these will actually be fetched)")
    est_min = unique_gyg_urls * GYG_MIN_INTERVAL / 60
    print(f"  Estimated time:   ~{est_min:.0f} min  "
          f"({unique_gyg_urls} GYG fetches × {GYG_MIN_INTERVAL}s / {MAX_WORKERS} threads  "
          f"+ Tur in parallel)")

    if not matches:
        print("❌ No matches. Try lowering MATCH_THRESHOLD."); return

    print(f"\n  🏆 Top 15 matches:")
    print(f"  {'Sc':>4} {'C':1} {'City':14}  {'Tur name':36}  {'GYG name'}")
    print(f"  {'-'*4} {'-'} {'-'*14}  {'-'*36}  {'-'*32}")
    for m in matches[:15]:
        c = "✓" if m["same_city"] else " "
        print(f"  {m['match_score']:4.0f} {c} {m['tur_city'][:14]:14}  "
              f"{m['tur_name'][:36]:36}  {m['gyg_name'][:32]}")

    save_csv(matches, OUT_MATCHES)

    # ── Phase 4: Scrape prices ───────────────────────────────────
    banner(f"PHASE 4 — Scrape prices ({len(matches)} pairs, {MAX_WORKERS} threads)")
    print(f"  GYG rate limit: 1 request every {GYG_MIN_INTERVAL}s (global across all threads)")
    print(f"  GYG price cache: duplicate URLs fetched only once\n")

    results   = []
    completed = 0
    lock      = threading.Lock()

    def task(m):
        nonlocal completed
        r = process_match(m)
        with lock:
            completed += 1
            cached = " [cached]" if r["gyg_url"] in _gyg_cache and completed > 1 else ""
            pci_s  = f"PCI={r['pci']:.3f}" if r["pci"] else r["scrape_status"]
            print(f"  [{completed:3d}/{len(matches)}] "
                  f"{r['tur_name'][:36]:36}  {pci_s}{cached}")
        return r

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(task, m) for m in matches]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    results.sort(key=lambda x: (x["pci"] is None, x["pci"] or 999))

    # ── Phase 5: Save & summarise ────────────────────────────────
    banner("PHASE 5 — Results")
    save_csv(results, OUT_PRICES)

    valid = [r for r in results if r["pci"] is not None]
    if valid:
        pcis   = [r["pci"] for r in valid]
        avg    = sum(pcis) / len(pcis)
        above  = sum(1 for p in pcis if p > 1)
        below  = sum(1 for p in pcis if p < 1)

        print(f"\n  📊 PCI SUMMARY  (PCI = Tur price USD / GYG price USD)")
        print(f"  {'─'*45}")
        print(f"  Pairs with valid PCI:       {len(valid):>4} / {len(results)}")
        print(f"  Average PCI:                {avg:>7.3f}")
        print(f"  Median PCI:                 {sorted(pcis)[len(pcis)//2]:>7.3f}")
        print(f"  Min PCI  (Tur cheaper):     {min(pcis):>7.3f}")
        print(f"  Max PCI  (GYG cheaper):     {max(pcis):>7.3f}")
        print(f"  Tur more expensive (>1):    {above:>4}  ({100*above/len(valid):.0f}%)")
        print(f"  Tur cheaper        (<1):    {below:>4}  ({100*below/len(valid):.0f}%)")

        print(f"\n  ⬇  Lowest PCI — Tur cheapest vs GYG:")
        print(f"  {'PCI':>6}  {'Tur USD':>8}  {'GYG USD':>8}  Name")
        for r in results[:8]:
            if r["pci"]:
                print(f"  {r['pci']:6.3f}  ${r['tur_price_usd']:>7.2f}  "
                      f"${r['gyg_price_usd']:>7.2f}  {r['tur_name'][:50]}")

        print(f"\n  ⬆  Highest PCI — Tur most expensive vs GYG:")
        for r in [x for x in reversed(results) if x["pci"]][:8]:
            print(f"  {r['pci']:6.3f}  ${r['tur_price_usd']:>7.2f}  "
                  f"${r['gyg_price_usd']:>7.2f}  {r['tur_name'][:50]}")

    print(f"\n  ✅ Done! → {OUT_PRICES}")


if __name__ == "__main__":
    main()
