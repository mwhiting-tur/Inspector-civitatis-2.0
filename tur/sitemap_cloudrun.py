import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from google.cloud import bigquery
from flask import Flask, jsonify
from datetime import datetime, timezone

app = Flask(__name__)

SITEMAP_URL = os.environ.get("SITEMAP_URL", "https://www.tur.com/sitemap.xml")
BQ_PROJECT = os.environ.get("BQ_PROJECT", "datatur")
BQ_OUTPUT_TABLE = os.environ.get("BQ_OUTPUT_TABLE", "inspector.tur_sitemap_urls")
BQ_INPUT_QUERY = os.environ.get(
    "BQ_INPUT_QUERY",
    "SELECT internal_id, slug FROM `datatur.dbt_int_db_tur.int_products` WHERE publish IS TRUE"
)


def fetch_sitemap(url: str) -> bytes:
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.content


def extract_spanish_urls(xml_content: bytes) -> list[str]:
    root = ET.fromstring(xml_content)
    ns = {
        "smp": "http://www.sitemaps.org/schemas/sitemap/0.9",
        "xhtml": "http://www.w3.org/1999/xhtml",
    }

    # Handle sitemap index (nested sitemaps)
    child_sitemaps = root.findall("smp:sitemap/smp:loc", ns)
    if child_sitemaps:
        urls = []
        for loc in child_sitemaps:
            child_content = fetch_sitemap(loc.text.strip())
            urls.extend(extract_spanish_urls(child_content))
        return urls

    url_set = set()
    for url_tag in root.findall("smp:url", ns):
        loc = url_tag.find("smp:loc", ns)
        if loc is not None and "/es/" in loc.text:
            url_set.add(loc.text)
            continue
        for link in url_tag.findall("xhtml:link", ns):
            href = link.get("href", "")
            if "/es/" in href:
                url_set.add(href)

    return list(url_set)


def match_slug(slug: str, urls: list[str]) -> str | None:
    if pd.isna(slug) or slug == "":
        return None
    for url in urls:
        if str(slug) in url:
            return url
    return None


@app.route("/", methods=["GET", "POST"])
def run():
    try:
        print(f"Fetching sitemap from {SITEMAP_URL}...")
        xml_content = fetch_sitemap(SITEMAP_URL)
        urls = extract_spanish_urls(xml_content)
        print(f"Extracted {len(urls)} Spanish URLs from sitemap")

        client = bigquery.Client(project=BQ_PROJECT)

        print("Querying BigQuery for published products...")
        df = client.query(BQ_INPUT_QUERY).to_dataframe()
        print(f"Loaded {len(df)} products")

        df["url"] = df["slug"].apply(lambda s: match_slug(s, urls))
        df["matched"] = df["url"].notna()
        df["extracted_at"] = datetime.now(timezone.utc).isoformat()

        table_id = f"{BQ_PROJECT}.{BQ_OUTPUT_TABLE}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

        matched = df["matched"].sum()
        print(f"Done. {matched}/{len(df)} slugs matched. Written to {table_id}")

        return jsonify({
            "status": "ok",
            "products": len(df),
            "sitemap_urls": len(urls),
            "matched": int(matched),
            "output_table": table_id,
        }), 200

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
