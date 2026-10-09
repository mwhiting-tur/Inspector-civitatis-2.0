"""
Piezas compartidas por los scrapers de operadores de Civitatis.

Vive aparte para que el motor HTTP no tenga que importar Playwright: el runner
self-hosted sólo necesita httpx + beautifulsoup4.
"""

import re

COLUMNAS = [
    "pais", "destino", "actividad", "url_actividad", "operador",
    "email", "telefono", "direccion", "descripcion",
    "precio_real", "opiniones", "viajeros", "rating",
    "moneda", "fecha_scan",
]

# datatur.supply.operators_civitatis no tiene un solo NULL: los datos de
# operador ausentes se guardan como 'N/A' y la descripción vacía como ''.
SIN_DATO = "N/A"
OPERADOR_GENERICO = "No especificado / Único"

MONEDAS_VALIDAS = {"ARS", "BRL", "CLP", "COP", "EUR", "GBP", "MXN", "PEN", "USD"}


def limpiar_numero(texto, tipo="float"):
    """
    Civitatis formatea a la española: '.' separa miles y ',' decimales.
    '40,68 US$' -> 40.68 | '7.977' -> 7977 | '193.083' -> 193083
    """
    if not texto:
        return 0 if tipo == "int" else 0.0
    limpio = re.sub(r"[^\d,]", "", str(texto)).replace(",", ".")
    try:
        valor = float(limpio)
        return int(valor) if tipo == "int" else valor
    except ValueError:
        return 0 if tipo == "int" else 0.0


def limpiar_rating(texto):
    """'9,5 / 10' -> 9.5"""
    if not texto:
        return 0.0
    if "/" in texto:
        texto = texto.split("/")[0]
    texto = re.sub(r"[^\d.]", "", texto.replace(",", "."))
    try:
        return float(texto)
    except ValueError:
        return 0.0


def slug_de_url(url):
    """https://www.civitatis.com/es/madrid/visita-guiada/ -> ('madrid', 'visita-guiada')"""
    m = re.match(r"https?://(?:www\.)?civitatis\.com/es/([^/?#]+)/([^/?#]+)/?", url or "")
    return (m.group(1).lower(), m.group(2).lower()) if m else (None, None)
