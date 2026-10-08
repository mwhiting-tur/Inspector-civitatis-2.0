"""
SimilarWeb - Top 50 keywords organicas NonBranded por pais y dominio.

Objetivo: identificar que destinos/actividades empujan trafico organico hacia
los competidores (GetYourGuide y Civitatis) en los 6 mercados LATAM.
Entregable para equipo comercial -> presentacion a marketing.

- Trafico organico, segmento NonBranded (lo accionable para "que empujar").
- Ventana de 12 meses, agregando clicks por keyword a lo largo del ano
  para neutralizar estacionalidad turistica (hemisferios desfasados).
- 1 URL por keyword: la top_url (pagina con mejor posicion organica).

Endpoint: /v4/website-analysis/keywords (1 mes por llamada -> se itera).
Costo aprox: 2 dominios x 6 paises x 12 meses = 144 llamadas (~13 creditos c/u).

Dependencias: pip install requests python-dateutil
"""
import certifi
import os
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import time
import csv
from collections import defaultdict
from datetime import date
from dateutil.relativedelta import relativedelta

# ----------------------------------------------------------------------
# Configuracion
# ----------------------------------------------------------------------
API_KEY = "88a5369ee04943cba850fe2422d54400"
BASE_URL = "https://api.similarweb.com/v4/website-analysis/keywords"

DOMAINS = ["getyourguide.com", "civitatis.com"]
COUNTRIES = ["cl", "ar", "br", "co", "mx", "pe"]
SEGMENT = "NonBranded"          # accionable para "que destinos/actividades empujar"
TOP_N = 50
REQUEST_LIMIT = 100            # pedimos mas por mes para no perder keywords
                              # que entran al top anual pero no destacan mensual
SLEEP = 1.0                    # pausa entre requests
OUTPUT = "similarweb_top50_keywords.csv"


def last_12_months():
    """Ultimos 12 meses cerrados (YYYY-MM), excluyendo el mes en curso."""
    end = date.today().replace(day=1) - relativedelta(months=1)
    return [(end - relativedelta(months=i)).strftime("%Y-%m") for i in range(12)][::-1]


def fetch_keywords(domain, country, month):
    params = {
        "api_key": API_KEY,
        "URL": domain,
        "country": country,
        "start_date": month,
        "end_date": month,          # debe ser igual a start_date
        "traffic_source": "Organic",
        "web_source": "Total",
        "branded_type": SEGMENT,
        "limit": REQUEST_LIMIT,
    }
    r = requests.get(BASE_URL, params=params, timeout=60, verify=False)
    r.raise_for_status()
    return r.json().get("keywords", [])


def main():
    months = last_12_months()
    print(f"Periodo: {months[0]} a {months[-1]}  |  segmento: {SEGMENT}")

    rows = []

    for domain in DOMAINS:
        for country in COUNTRIES:
            agg_clicks = defaultdict(float)   # clicks acumulados por keyword
            last_seen = {}                    # ultimas metricas vistas (referencia)

            for month in months:
                try:
                    kws = fetch_keywords(domain, country, month)
                except requests.HTTPError as e:
                    print(f"  [WARN] {domain} {country} {month}: {e}")
                    time.sleep(SLEEP)
                    continue

                for kw in kws:
                    name = kw.get("keyword")
                    agg_clicks[name] += kw.get("clicks", 0) or 0
                    last_seen[name] = kw

                print(f"  {domain} {country} {month}: {len(kws)} keywords")
                time.sleep(SLEEP)

            # top 50 por clicks acumulados en los 12 meses
            top = sorted(agg_clicks.items(), key=lambda x: x[1], reverse=True)[:TOP_N]

            for rank, (name, total_clicks) in enumerate(top, start=1):
                ref = last_seen.get(name, {})
                rows.append({
                    "domain": domain,
                    "country": country,
                    "rank": rank,
                    "keyword": name,
                    "clicks_12m": int(total_clicks),
                    "volume_last": ref.get("volume"),
                    "cpc_last": ref.get("cpc"),
                    "difficulty_last": ref.get("difficulty"),
                    "primary_intent_last": ref.get("primary_intent"),
                    "top_url": ref.get("top_url"),
                })

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nListo: {len(rows)} filas en {OUTPUT}")


if __name__ == "__main__":
    main()