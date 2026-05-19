"""
Script para contar actividades únicas por destino en GetYourGuide.
- Extrae la URL base del destino desde el CSV
- Scrape cada página de destino (con paginación)
- Deduplica actividades similares con fuzzy matching
- Exporta resultados a CSV
"""

import sys
# Forzar UTF-8 en Windows para que los acentos en print no fallen
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random
from rapidfuzz import fuzz, process as fuzz_process
import numpy as np

# ── Configuración ──────────────────────────────────────────────────────────────
CSV_INPUT   = "gyg/metadata_latam_BQ.csv"
CSV_OUTPUT  = "gyg/actividades_unicas_por_destino.csv"
SIMILARITY  = 85        # umbral % para considerar dos actividades iguales
DELAY_MIN   = 2.0       # segundos mínimo entre requests
DELAY_MAX   = 4.5       # segundos máximo entre requests
MAX_PAGES   = 50        # páginas máximas por destino (seguridad)

# Mapeo: nombre display → nombre exacto en el CSV (campo 'destino')
DESTINOS_MAP = {
    "Buenos Aires":          "Ciudad de buenos aires",
    "San Carlos de Bariloche": "San carlos de bariloche",
    "Mendoza":               "Mendoza",
    "El Calafate":           "El calafate",
    "Puerto Iguazú":         "Puerto iguazu",
    "Río de Janeiro":        "Rio de janeiro",
    "Foz de Iguazú":         "Foz de iguazu",
    "Florianópolis":         "Florianopolis",
    "São Paulo":             "Sao paulo",
    "Salvador de Bahía":     "Salvador brasil",
    "San Pedro de Atacama":  "San pedro de atacama",
    "Puerto Natales":        "Puerto natales",
    "Santiago":              "Santiago de chile",
    "Rapa Nui":              "Hanga roa",
    "Cartagena de Indias":   "Cartagena de indias",
    "Medellín":              "Medellin",
    "Bogotá":                "Bogota",
    "Cusco":                 "Cuzco",
    "Lima":                  "Lima",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.getyourguide.com/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ── Utilidades ────────────────────────────────────────────────────────────────

def normalizar(name: str) -> str:
    """Normaliza un nombre para comparación fuzzy."""
    name = str(name).lower().strip()
    name = re.sub(r"[^\w\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def get_dest_base_url(url: str) -> str | None:
    """Extrae la URL base del destino desde una URL de actividad.
    Ej: https://www.getyourguide.com/es-es/mendoza-l673/tour-t51403/ → .../mendoza-l673/
    """
    m = re.match(r"(https://www\.getyourguide\.com/[^/]+/[^/]+-l\d+/)", url)
    return m.group(1) if m else None


def parse_next_data(html: str) -> list[str]:
    """Extrae títulos de actividades del JSON __NEXT_DATA__ embebido (Next.js)."""
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return []
    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError:
        return []

    activities = []

    def walk(obj):
        if isinstance(obj, dict):
            # GYG embeds activity titles under varios keys
            for key in ("title", "name", "activityTitle", "tourTitle"):
                if key in obj and isinstance(obj[key], str) and len(obj[key]) > 8:
                    activities.append(obj[key])
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(data)
    return activities


def parse_html_titles(html: str) -> list[str]:
    """Extrae títulos de actividades del HTML con BeautifulSoup como fallback."""
    soup = BeautifulSoup(html, "html.parser")
    titles = []

    # Selectores comunes de GYG para tarjetas de actividad
    selectors = [
        "h3.activity-card__name",
        "h3.activity-name",
        "[data-testid='activity-card-title']",
        ".activity-card h3",
        ".tour-card__title",
        "h2.activity-title",
        "h3",  # fallback genérico
    ]

    seen = set()
    for selector in selectors:
        tags = soup.select(selector)
        if tags:
            for tag in tags:
                t = tag.get_text(strip=True)
                if len(t) > 8 and t not in seen:
                    titles.append(t)
                    seen.add(t)
            if len(titles) > 3:  # si encontramos resultados reales, paramos
                break

    return titles


def get_total_from_page(html: str) -> int | None:
    """Intenta extraer el total de actividades anunciado en la página."""
    patterns = [
        r'"totalCount"\s*:\s*(\d+)',
        r'"total"\s*:\s*(\d+)',
        r'(\d[\d\.,]*)\s+(?:actividades?|tours?|experiences?|excursiones?|results?)',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.I)
        if m:
            n = m.group(1).replace(".", "").replace(",", "")
            if n.isdigit():
                return int(n)
    return None


def scrape_destino(base_url: str, nombre: str) -> dict:
    """
    Scrape todas las páginas del destino en GYG.
    Devuelve dict con actividades encontradas y metadatos.
    """
    all_titles: list[str] = []
    total_anunciado: int | None = None
    pages_scraped = 0
    status = "ok"

    print(f"  → Scrapeando: {base_url}")

    for page in range(1, MAX_PAGES + 1):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            resp = SESSION.get(url, timeout=20)
        except requests.RequestException as e:
            status = f"error_request: {e}"
            print(f"    ✗ Error en página {page}: {e}")
            break

        if resp.status_code == 403:
            status = "bloqueado_403"
            print(f"    ✗ Bloqueado (403) en página {page}")
            break
        if resp.status_code != 200:
            status = f"http_{resp.status_code}"
            print(f"    ✗ HTTP {resp.status_code} en página {page}")
            break

        html = resp.text
        pages_scraped += 1

        if page == 1 and total_anunciado is None:
            total_anunciado = get_total_from_page(html)
            if total_anunciado:
                print(f"    ℹ Total anunciado por GYG: {total_anunciado}")

        # Intentar __NEXT_DATA__ primero (más fiable)
        titles = parse_next_data(html)
        if not titles:
            titles = parse_html_titles(html)

        new_titles = [t for t in titles if t not in all_titles]
        all_titles.extend(new_titles)
        print(f"    Página {page}: +{len(new_titles)} títulos (total acumulado: {len(all_titles)})")

        # Si no hay títulos nuevos, llegamos al final
        if not new_titles and page > 1:
            print(f"    ✓ Sin más resultados nuevos. Fin de paginación.")
            break

        # Pausa cortés entre páginas
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return {
        "all_titles": all_titles,
        "pages_scraped": pages_scraped,
        "total_anunciado": total_anunciado,
        "status": status,
    }


# ── Deduplicación ─────────────────────────────────────────────────────────────

def clusterizar(nombres: list[str], threshold: int = SIMILARITY) -> list[list[str]]:
    """
    Agrupa nombres similares en clusters usando rapidfuzz vectorizado (cdist).
    Devuelve lista de clusters (cada cluster = una actividad 'única').
    """
    if not nombres:
        return []

    normalizados = [normalizar(n) for n in nombres]
    n = len(normalizados)

    # Matriz de similitudes (float32 para ahorrar memoria con destinos grandes)
    scores = fuzz_process.cdist(
        normalizados, normalizados,
        scorer=fuzz.token_sort_ratio,
        dtype=np.float32,
        workers=-1,   # usa todos los cores disponibles
    )

    asignados: set[int] = set()
    clusters: list[list[str]] = []

    for i in range(n):
        if i in asignados:
            continue
        # Todos los índices con similitud >= threshold respecto al item i
        similares = np.where(scores[i] >= threshold)[0]
        cluster = [nombres[j] for j in similares if j not in asignados]
        for j in similares:
            asignados.add(j)
        if cluster:
            clusters.append(cluster)

    return clusters


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  Contador de actividades únicas por destino — GYG")
    print("=" * 65)

    # Cargar CSV
    df = pd.read_csv(CSV_INPUT, sep=";", encoding="utf-8-sig")
    print(f"\nCSV cargado: {len(df)} filas\n")

    # csv_name → display_name (invertimos el mapa)
    csv_to_display = {v.strip().lower(): k for k, v in DESTINOS_MAP.items()}

    df["destino_norm"] = df["destino"].str.strip().str.lower()

    # Filtrar sólo destinos objetivo
    df_target = df[df["destino_norm"].isin(csv_to_display.keys())].copy()
    df_target["destino_display"] = df_target["destino_norm"].map(csv_to_display)

    print(f"Filas filtradas para {len(DESTINOS_MAP)} destinos objetivo: {len(df_target)}\n")

    # Preparar salida
    resultados = []
    detalle_clusters = []

    for destino_canonical, csv_name in DESTINOS_MAP.items():
        csv_name_lower = csv_name.strip().lower()
        grupo = df_target[df_target["destino_norm"] == csv_name_lower]

        if grupo.empty:
            print(f"\n[{destino_canonical}] (CSV: '{csv_name}') — Sin datos en CSV, saltando.")
            continue

        pais = grupo["pais"].mode()[0]
        print(f"\n{'─'*60}")
        print(f"[{pais}] {destino_canonical} (CSV: '{csv_name}') — {len(grupo)} actividades")

        # Extraer URL base del destino desde la primera URL válida
        base_url = None
        for url in grupo["url"].dropna():
            base_url = get_dest_base_url(url)
            if base_url:
                break

        if not base_url:
            print(f"  ✗ No se pudo extraer URL base. Usando sólo CSV.")
            scrape_result = {"all_titles": [], "pages_scraped": 0,
                             "total_anunciado": None, "status": "no_url"}
        else:
            scrape_result = scrape_destino(base_url, destino_canonical)

        # Títulos: usar scrapeados si hay; si no, usar CSV
        titulos_scrapeados = scrape_result["all_titles"]
        titulos_csv        = grupo["nombre_actividad"].dropna().tolist()

        if titulos_scrapeados:
            fuente = "GYG scraping"
            titulos_usar = titulos_scrapeados
        else:
            fuente = "CSV (scraping falló)"
            titulos_usar = titulos_csv
            print(f"  ↩  Usando datos del CSV ({len(titulos_csv)} actividades)")

        # Deduplicar
        clusters = clusterizar(titulos_usar)
        n_unicos = len(clusters)
        n_total  = len(titulos_usar)
        n_dupes  = n_total - n_unicos
        pct      = round(n_dupes / n_total * 100, 1) if n_total else 0

        print(f"  Total encontradas : {n_total}")
        print(f"  Únicas (clusters) : {n_unicos}")
        print(f"  Similares/dupes   : {n_dupes} ({pct}%)")
        print(f"  Total anunciado   : {scrape_result['total_anunciado'] or 'N/A'}")
        print(f"  Fuente datos      : {fuente}")

        resultados.append({
            "pais":                  pais,
            "destino":               destino_canonical,
            "url_base_gyg":          base_url or "",
            "total_en_fuente":       n_total,
            "actividades_unicas":    n_unicos,
            "actividades_similares": n_dupes,
            "pct_duplicadas":        pct,
            "total_anunciado_gyg":   scrape_result["total_anunciado"] or "",
            "paginas_scrapeadas":    scrape_result["pages_scraped"],
            "fuente":                fuente,
            "status_scraping":       scrape_result["status"],
        })

        # Guardar detalle de clusters para análisis
        for cluster in clusters:
            if len(cluster) > 1:
                detalle_clusters.append({
                    "destino":       destino_canonical,
                    "representante": cluster[0],
                    "similares":     " | ".join(cluster[1:]),
                    "n_en_cluster":  len(cluster),
                })

        # Pausa entre destinos
        if base_url:
            time.sleep(random.uniform(DELAY_MIN + 1, DELAY_MAX + 2))

    # Exportar resultados
    df_out = pd.DataFrame(resultados)
    df_out.to_csv(CSV_OUTPUT, index=False, encoding="utf-8-sig")
    print(f"\n\n{'='*65}")
    print(f"  Resultado guardado en: {CSV_OUTPUT}")
    print(f"{'='*65}")
    print(df_out[["destino", "total_en_fuente", "actividades_unicas",
                  "actividades_similares", "pct_duplicadas",
                  "total_anunciado_gyg"]].to_string(index=False))

    # Exportar clusters duplicados para revisión
    if detalle_clusters:
        df_clusters = pd.DataFrame(detalle_clusters)
        clusters_file = "clusters_duplicados.csv"
        df_clusters.to_csv(clusters_file, index=False, encoding="utf-8-sig")
        print(f"\n  Detalle de clusters duplicados → {clusters_file}")

    print("\nFin del script.")


if __name__ == "__main__":
    main()
