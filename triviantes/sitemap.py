"""
Descubrimiento de supply de Triviantes (o cualquier dominio multi-host).

Qué hace, en orden:
  1. Enumera subdominios via Certificate Transparency (crt.sh).
  2. Por cada host vivo: lee robots.txt y extrae directivas Sitemap:.
  3. Prueba rutas de sitemap comunes (incluye variantes WordPress).
  4. Recorre sitemapindex -> sitemaps hijos -> URLs (soporta .gz).
  5. Detecta WP REST API y cuenta objetos por post type (X-WP-Total),
     que es la forma mas barata de saber el tamano real del catalogo.
  6. Escribe hosts.csv y urls.csv, e imprime un resumen por patron de URL.

Requiere: httpx
"""

import csv
import gzip
import io
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections import Counter
from urllib.parse import urlparse

import httpx
import truststore

truststore.inject_into_ssl()  # usa el keychain del sistema (necesario detras de proxies con SSL inspection, ej. Netskope)

DOMAIN = "triviantes.com"
UA = "Mozilla/5.0 (compatible; supply-research/1.0)"
TIMEOUT = 20.0
SLEEP = 0.4  # cortesia entre requests

SITEMAP_CANDIDATES = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/wp-sitemap.xml",          # WordPress core >= 5.5
    "/sitemap.xml.gz",
    "/sitemap/sitemap-index.xml",
    "/robots.txt",              # se procesa aparte, queda por completitud
]

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def client():
    return httpx.Client(
        headers={"User-Agent": UA},
        follow_redirects=True,
        timeout=TIMEOUT,
        verify=True,
    )


# ---------------------------------------------------------------- 1. hosts

def hosts_from_crtsh(domain: str) -> set[str]:
    """Certificate Transparency: todo host que alguna vez tuvo cert TLS."""
    url = f"https://crt.sh/?q=%25.{domain}&output=json"
    out: set[str] = set()
    with client() as c:
        try:
            r = c.get(url)
            r.raise_for_status()
            for row in r.json():
                for name in str(row.get("name_value", "")).split("\n"):
                    name = name.strip().lower().lstrip("*.")
                    if name.endswith(domain):
                        out.add(name)
        except Exception as e:  # crt.sh se cae seguido; no bloquea el resto
            print(f"[crt.sh] fallo: {e}", file=sys.stderr)
    out.add(domain)
    out.add(f"www.{domain}")
    return out


def probe_host(c: httpx.Client, host: str) -> dict | None:
    for scheme in ("https", "http"):
        try:
            r = c.get(f"{scheme}://{host}/")
            generator = ""
            m = re.search(
                r'<meta[^>]+name=["\']generator["\'][^>]+content=["\']([^"\']+)',
                r.text[:200_000],
                re.I,
            )
            if m:
                generator = m.group(1)
            return {
                "host": host,
                "status": r.status_code,
                "final_url": str(r.url),
                "server": r.headers.get("server", ""),
                "generator": generator,
                "title": (re.search(r"<title[^>]*>(.*?)</title>", r.text, re.I | re.S).group(1).strip()[:120]
                          if re.search(r"<title[^>]*>(.*?)</title>", r.text, re.I | re.S) else ""),
            }
        except Exception:
            continue
    return None


# ------------------------------------------------------------- 2/3. sitemaps

def sitemaps_from_robots(c: httpx.Client, host: str) -> list[str]:
    found = []
    try:
        r = c.get(f"https://{host}/robots.txt")
        if r.status_code == 200:
            for line in r.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    found.append(line.split(":", 1)[1].strip())
    except Exception:
        pass
    return found


def find_sitemaps(c: httpx.Client, host: str) -> list[str]:
    urls = sitemaps_from_robots(c, host)
    for path in SITEMAP_CANDIDATES:
        if path == "/robots.txt":
            continue
        u = f"https://{host}{path}"
        if u in urls:
            continue
        try:
            r = c.get(u)
            if r.status_code == 200 and "<" in r.text[:500] and "xml" in r.text[:500].lower():
                urls.append(u)
        except Exception:
            pass
        time.sleep(SLEEP)
    return urls


def fetch_xml(c: httpx.Client, url: str) -> ET.Element | None:
    try:
        r = c.get(url)
        if r.status_code != 200:
            return None
        raw = r.content
        if url.endswith(".gz") or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return ET.parse(io.BytesIO(raw)).getroot()
    except Exception:
        return None


def walk_sitemaps(c: httpx.Client, seeds: list[str], max_depth: int = 3) -> list[dict]:
    """Devuelve [{sitemap, loc, lastmod}] recorriendo indices recursivamente."""
    seen: set[str] = set()
    rows: list[dict] = []
    queue = [(u, 0) for u in seeds]
    while queue:
        url, depth = queue.pop(0)
        if url in seen or depth > max_depth:
            continue
        seen.add(url)
        root = fetch_xml(c, url)
        time.sleep(SLEEP)
        if root is None:
            continue
        tag = root.tag.split("}")[-1]
        if tag == "sitemapindex":
            for sm in root.findall("sm:sitemap", NS):
                loc = sm.findtext("sm:loc", default="", namespaces=NS).strip()
                if loc:
                    queue.append((loc, depth + 1))
        elif tag == "urlset":
            for u in root.findall("sm:url", NS):
                rows.append({
                    "sitemap": url,
                    "loc": u.findtext("sm:loc", default="", namespaces=NS).strip(),
                    "lastmod": u.findtext("sm:lastmod", default="", namespaces=NS).strip(),
                })
    return rows


# ------------------------------------------------------------- 5. WP REST

def wp_rest_inventory(c: httpx.Client, host: str) -> list[dict]:
    """Si es WordPress, /wp-json/wp/v2/types lista los post types y
    X-WP-Total da el conteo exacto por tipo sin paginar."""
    out = []
    try:
        r = c.get(f"https://{host}/wp-json/wp/v2/types")
        if r.status_code != 200:
            return out
        for key, meta in r.json().items():
            rest_base = meta.get("rest_base") or key
            try:
                rr = c.get(f"https://{host}/wp-json/wp/v2/{rest_base}", params={"per_page": 1})
                total = rr.headers.get("X-WP-Total")
            except Exception:
                total = None
            out.append({
                "host": host,
                "post_type": key,
                "rest_base": rest_base,
                "name": meta.get("name", ""),
                "total": total,
            })
            time.sleep(SLEEP)
    except Exception:
        pass
    return out


# ------------------------------------------------------------------ main

def main() -> None:
    hosts = sorted(hosts_from_crtsh(DOMAIN))
    print(f"crt.sh devolvio {len(hosts)} hosts candidatos\n")

    live, all_urls, wp_rows = [], [], []
    with client() as c:
        for h in hosts:
            info = probe_host(c, h)
            time.sleep(SLEEP)
            if not info:
                continue
            sitemaps = find_sitemaps(c, h)
            info["sitemaps"] = " | ".join(sitemaps)
            rows = walk_sitemaps(c, sitemaps)
            info["n_urls"] = len(rows)
            all_urls.extend(rows)
            wp_rows.extend(wp_rest_inventory(c, h))
            live.append(info)
            print(f"{h:45s} {info['status']}  sitemaps={len(sitemaps):2d}  urls={len(rows):5d}  {info['generator']}")

    with open("hosts.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["host", "status", "final_url", "server",
                                          "generator", "title", "sitemaps", "n_urls"])
        w.writeheader()
        w.writerows(live)

    with open("urls.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["sitemap", "loc", "lastmod"])
        w.writeheader()
        w.writerows(all_urls)

    if wp_rows:
        with open("wp_types.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["host", "post_type", "rest_base", "name", "total"])
            w.writeheader()
            w.writerows(wp_rows)

    # resumen: primer segmento de path, que suele separar producto de blog
    seg = Counter()
    for row in all_urls:
        p = urlparse(row["loc"]).path.strip("/").split("/")
        seg[f"{urlparse(row['loc']).netloc}/{p[0] if p and p[0] else '(raiz)'}"] += 1
    print("\n--- URLs por host/primer-segmento ---")
    for k, v in seg.most_common(60):
        print(f"{v:6d}  {k}")


if __name__ == "__main__":
    main()