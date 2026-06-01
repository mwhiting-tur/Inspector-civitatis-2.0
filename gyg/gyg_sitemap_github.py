import re
import time
import xml.etree.ElementTree as ET
import pandas as pd
import requests

SITEMAP_BASE = "https://www.getyourguide.com/es-es/sitemap-activity-{index}.xml"
SITEMAP_INDICES = range(0, 45)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/xml, text/xml, */*",
}
REQUEST_DELAY = 1.5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# ⚠️ IMPORTANTE: PEGA AQUÍ TUS DICCIONARIOS COMPLETOS DE CIUDADES
ciudades_brasil = ["maxaranguape-l133602", "natal-l2112"] # etc...
ciudades_chile = ["cerro-castillo-l192281", "santiago-de-chile-l226"] # etc...

PAISES = {
    "Brasil": ciudades_brasil,
    "Chile": ciudades_chile,
    # Añade el resto de tus países...
}

def fetch_sitemap(index: int) -> list[str]:
    url = SITEMAP_BASE.format(index=index)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # En GitHub Actions usamos verificación normal (verify=True por defecto)
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            locs = [
                el.text.strip() for el in root.iter()
                if el.tag in ("{http://www.sitemaps.org/schemas/sitemap/0.9}loc", "loc") and el.text
            ]
            print(f"  [{index:02d}] fetched {len(locs):,} URLs")
            return locs
        except Exception as exc:
            print(f"  [{index:02d}] attempt {attempt} failed: {exc}")
            time.sleep(REQUEST_DELAY * attempt)
    return []

def match_cities(urls: list[str], city_slugs: list[str], pais: str) -> list[dict]:
    hits = []
    slug_set = set(city_slugs)
    for url in urls:
        for ciudad_slug in slug_set:
            if f"/{ciudad_slug}/" in url:
                match_id = re.search(r"-t(\d+)/?$", url)
                tour_id = match_id.group(1) if match_id else "N/A"
                try:
                    slug_actividad = url.split(f"/{ciudad_slug}/")[1]
                    slug_actividad = re.sub(r"-t\d+/?$", "", slug_actividad)
                    titulo_bruto = slug_actividad.replace("-", " ").title()
                except IndexError:
                    titulo_bruto = "Desconocido"
                hits.append({
                    "pais": pais,
                    "ciudad_id": ciudad_slug,
                    "tour_id": tour_id,
                    "titulo_referencia": titulo_bruto,
                    "url": url,
                })
                break
    return hits

if __name__ == "__main__":
    all_hits = []
    for idx in SITEMAP_INDICES:
        urls = fetch_sitemap(idx)
        for pais, slugs in PAISES.items():
            all_hits.extend(match_cities(urls, slugs, pais))
        time.sleep(REQUEST_DELAY)

    if all_hits:
        df = pd.DataFrame(all_hits).drop_duplicates(subset=["tour_id"])
        # Exportamos a un nombre genérico para que el Script 2 lo consuma
        df.to_csv("tours_latam_urls.csv", index=False, sep=";", encoding="utf-8-sig")
        print(f"\n✅ Total actividades únicas: {len(df):,}. Guardado en tours_latam_urls.csv")
    else:
        print("⚠️ No se encontraron actividades.")