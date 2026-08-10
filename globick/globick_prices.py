#!/usr/bin/env python3
"""
globick_prices.py
-----------------
Itera las activity_id de un CSV, consulta el endpoint de sessions de Globick
y devuelve una fila por version del producto con su recommendedPrice.

Uso basico:
    python globick_prices.py \
        --credentials credentials.json \
        --input BOKUN-TUR.csv \
        --output globick_precios.csv \
        --date 2026-08-15

Flags utiles:
    --fallback-days 30   (default) si el dia no arroja precio adulto valido,
                         amplia la ventana y usa la fecha mas cercana con precio
    --min-price 0.01     descarta tarifas en 0.0 (infantes cargados como adulto)
    --adult-min-age 16   rango de edad que termina antes de esto = menor
    --keep-columns ...   columnas del CSV a arrastrar al output (default: las 11
                         de BOKUN-TUR); 'all' = todas, 'none' = solo activity_id
    --prefer-type adult  tarifa a usar cuando el producto trae varias (adult/child/...)
    --dedupe min         una fila por version: min | first | none
    --resume             continua un CSV existente sin repetir ids
    --limit 10           prueba con las primeras N actividades
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import requests

__version__ = "1.7.0"  # 1.7: paralelismo con rate limit, ventana en dos fases, ETA

BASE_URL = "https://api.globick.com/v1"
LOGIN_PATH = "/authentication/login"
SESSIONS_PATH = "/activities/{activity_id}/sessions"

# Columnas de BOKUN-TUR.csv que se arrastran al output (passthrough).
# Se respeta este orden; las que no existan en el CSV se ignoran con warning.
INPUT_KEEP_DEFAULT = [
    "supplier_id",
    "supplier_code",
    "operator_id",
    "operator_name",
    "activity_id",
    "activity_name",
    "category_id",
    "category_name",
    "reference_activity_id",
    "city",
    "country",
]

# Columnas que aporta la API. 'activity_id' vive en las de input.
API_FIELDS = [
    "type",              # session["name"] -> version del producto
    "price",             # recommendedPrice
    "currency",
    "net_price",
    "retail_price",
    "commission",
    "rate_name",
    "rate_type",
    "session_id",
    "session_date",
    "session_time",
    "availability",
    "session_operator",  # operator.name de la API (distinto de operator_name del CSV)
    "rate_selection",    # por que se eligio esa tarifa (auditoria)
    "rates_available",   # todas las tarifas que traia la session
    "days_ahead",        # dias entre la fecha base y la sesion usada
    "status",            # ok | sin_sesiones | sin_tarifa | error
    "detail",
    "fetched_at",
]

# Alias aceptados en el JSON de credenciales. Se comparan normalizados
# (minusculas, sin guiones/underscores/espacios), asi que 'api-key', 'API_KEY',
# 'x-api-key' y 'apiKey' caen todos en el mismo lugar.
KEY_ALIASES = {
    "api_key": ("api_key", "apikey", "x-api-key", "key", "globick_api_key",
                "secret", "client_secret","GLOBICK_API_KEY"),
    "username": ("username", "user", "usuario", "email", "login", "user_name","GLOBICK_USERNAME"),
    "password": ("password", "pass", "clave", "contrasena", "contraseña", "pwd","GLOBICK_PASSWORD"),
}

log = logging.getLogger("globick")

SSL_HELP = """\
[TLS] Fallo la verificacion del certificado de api.globick.com:
  {error}

Sintoma tipico de un proxy de inspeccion TLS corporativo (Zscaler, Netskope,
Fortinet): reemplaza la cadena del sitio con una firmada por el root CA de la
empresa, que esta en el Keychain de macOS pero no en el bundle de certifi que
usa requests. Tres opciones, de mejor a peor:

  1) Usar el trust store del sistema operativo (recomendado):
       pip install truststore
     El script lo detecta e inyecta solo. El root CA corporativo ya esta en el
     Keychain, asi que no hay que exportar nada.

  2) Armar un bundle con certifi + el CA corporativo:
       security find-certificate -a -p /Library/Keychains/System.keychain > ~/corp-ca.pem
       cat "$(python -c 'import certifi;print(certifi.where())')" ~/corp-ca.pem > ~/ca-bundle.pem
       python globick_prices.py --ca-bundle ~/ca-bundle.pem ...
     (equivalente: export REQUESTS_CA_BUNDLE=~/ca-bundle.pem)

  3) Ultimo recurso, saltarse la verificacion:
       python globick_prices.py --insecure ...
     Las credenciales viajan por una conexion que no se puede autenticar.
     Sirve para desbloquearte hoy, no para dejarlo agendado.

Nota: esto no va a pasar cuando lo muevas a Apify o GitHub Actions, porque ahi
no hay proxy de inspeccion en el medio.
"""


def enable_truststore() -> bool:
    """Usa el trust store del OS (Keychain en macOS) si truststore esta instalado."""
    try:
        import truststore
        truststore.inject_into_ssl()
        log.info("truststore activo: usando el trust store del sistema operativo.")
        return True
    except ImportError:
        return False
    except Exception as exc:  # noqa: BLE001
        log.warning("No pude activar truststore (%s); sigo con certifi.", exc)
        return False


def _norm(key: str) -> str:
    """api-key / API_KEY / x-api-key -> apikey / xapikey."""
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def flatten_credentials(obj: Any) -> Dict[str, tuple]:
    """
    Recorre el JSON completo (a cualquier profundidad) y devuelve
    {clave_normalizada: (ruta_original, valor)}. Gana la coincidencia
    mas superficial, por eso el recorrido es BFS.
    """
    found: Dict[str, tuple] = {}
    queue: List[tuple] = [("", obj)]
    while queue:
        path, node = queue.pop(0)
        if isinstance(node, dict):
            for k, v in node.items():
                child = f"{path}.{k}" if path else str(k)
                if isinstance(v, (dict, list)):
                    queue.append((child, v))
                else:
                    found.setdefault(_norm(k), (child, v))
        elif isinstance(node, list):
            for i, v in enumerate(node):
                queue.append((f"{path}[{i}]", v))
    return found


def resolve_path(obj: Any, dotted: str) -> Any:
    """Resuelve 'credenciales.globick.llave' contra el JSON crudo."""
    node = obj
    for part in dotted.split("."):
        m = re.match(r"^(.*?)\[(\d+)\]$", part)
        if m:
            part, idx = m.group(1), int(m.group(2))
            if part:
                node = node[part]
            node = node[idx]
        else:
            node = node[part]
    return node


def load_credentials(path: str, overrides: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    overrides = {k: v for k, v in (overrides or {}).items() if v}
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        raise SystemExit(f"[credenciales] no existe el archivo {path}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"[credenciales] {path} no es JSON valido: {exc}")

    flat = flatten_credentials(raw)
    creds: Dict[str, str] = {}
    problems: List[str] = []

    for target, aliases in KEY_ALIASES.items():
        hit = None
        if target in overrides:
            dotted = overrides[target]
            try:
                hit = (dotted, resolve_path(raw, dotted))
            except (KeyError, IndexError, TypeError):
                problems.append(f"  - '{target}': la ruta '{dotted}' no existe en el JSON")
                continue
        else:
            for alias in aliases:
                entry = flat.get(_norm(alias))
                if entry is not None:
                    hit = entry
                    break
        if hit is None:
            problems.append(f"  - '{target}': no encontrada")
            continue
        origin, value = hit
        if value is None or str(value).strip() == "":
            problems.append(f"  - '{target}': la clave '{origin}' existe pero esta vacia")
            continue
        creds[target] = str(value).strip()
        log.debug("%s <- '%s' (%d caracteres)", target, origin, len(creds[target]))

    if problems:
        present = sorted({p for p, _ in flat.values()})
        raise SystemExit(
            f"[credenciales] no pude armar el login desde {path}:\n"
            + "\n".join(problems)
            + "\n\nClaves que SI encontre en el archivo (solo nombres, "
            f"sin valores):\n  {', '.join(present) or '(ninguna)'}\n\n"
            "Renombra las claves en el JSON, o usa --api-key-field / "
            "--username-field / --password-field para apuntar a las tuyas.\n"
            "Ej: --api-key-field credenciales.globick.llave"
        )
    return creds


def find_token(obj: Any, depth: int = 0) -> Optional[str]:
    """Busca recursivamente el access token en la respuesta del login."""
    if depth > 6:
        return None
    if isinstance(obj, dict):
        priority = (
            "accessToken", "access_token", "token", "jwt",
            "idToken", "id_token", "bearerToken",
        )
        for key in priority:
            val = obj.get(key)
            if isinstance(val, str) and len(val) > 20:
                return val
        for val in obj.values():
            found = find_token(val, depth + 1)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_token(item, depth + 1)
            if found:
                return found
    return None


# --------------------------------------------------------------------------- #
# Cliente
# --------------------------------------------------------------------------- #
class RateLimiter:
    """Limita el ritmo global de requests, compartido entre todos los workers."""

    def __init__(self, rps: float) -> None:
        self.min_interval = 1.0 / rps if rps > 0 else 0.0
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        if not self.min_interval:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_at)
            self._next_at = slot + self.min_interval
        delay = slot - now
        if delay > 0:
            time.sleep(delay)


class GlobickClient:
    def __init__(
        self,
        creds: Dict[str, str],
        timeout: int = 30,
        retries: int = 3,
        backoff: float = 2.0,
        ca_bundle: Optional[str] = None,
        insecure: bool = False,
        rps: float = 0.0,
        pool_size: int = 8,
    ) -> None:
        self.creds = creds
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.token: Optional[str] = None
        self.pool_size = pool_size
        self.limiter = RateLimiter(rps) if rps else None

        # requests.Session no es thread-safe, asi que cada worker usa la suya.
        # El token se comparte y se refresca bajo lock.
        self._local = threading.local()
        self._login_lock = threading.Lock()

        self._base_headers = {
            "Accept": "application/json",
            "Content-Language": "en-US",
            "x-api-key": creds["api_key"],
        }
        self._verify: Any = True
        if insecure:
            self._verify = False
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:  # noqa: BLE001
                pass
            log.warning("VERIFICACION TLS DESACTIVADA (--insecure). El api_key y la "
                        "password viajan por una conexion que no se puede autenticar. "
                        "Usalo solo para desbloquearte, no en produccion.")
        elif ca_bundle:
            self._verify = ca_bundle
            log.info("Usando CA bundle: %s", ca_bundle)

    def _new_session(self) -> requests.Session:
        sess = requests.Session()
        sess.headers.update(self._base_headers)
        sess.verify = self._verify
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.pool_size, pool_maxsize=self.pool_size, max_retries=0
        )
        sess.mount("https://", adapter)
        return sess

    @property
    def http(self) -> requests.Session:
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = self._new_session()
            self._local.session = sess
        if self.token:
            sess.headers["Authorization"] = f"Bearer {self.token}"
        return sess

    # -- login ------------------------------------------------------------- #
    def login(self, force: bool = False) -> None:
        with self._login_lock:
            if self.token and not force:
                return          # otro worker ya renovo el token
            self._login()

    def _login(self) -> None:
        url = BASE_URL + LOGIN_PATH
        try:
            resp = self.http.post(
                url,
                headers={"Content-Type": "application/json"},
                json={"username": self.creds["username"], "password": self.creds["password"]},
                timeout=self.timeout,
            )
        except requests.exceptions.SSLError as exc:
            raise SystemExit(SSL_HELP.format(error=str(exc)[:300]))
        if resp.status_code >= 400:
            raise SystemExit(
                f"[login] HTTP {resp.status_code}: {resp.text[:400]}"
            )
        token = find_token(resp.json())
        if not token:
            raise SystemExit(
                "[login] no encontre el access token en la respuesta. "
                f"Payload: {resp.text[:400]}"
            )
        self.token = token
        log.info("Login OK (token %s...%s)", token[:8], token[-6:])

    # -- sessions ---------------------------------------------------------- #
    def get_sessions(self, activity_id: str, from_date: str, to_date: str) -> List[dict]:
        url = BASE_URL + SESSIONS_PATH.format(activity_id=activity_id)
        params = {"from-date": from_date, "to-date": to_date}
        relogged = False

        for attempt in range(self.retries + 1):
            if self.limiter:
                self.limiter.wait()
            try:
                resp = self.http.get(url, params=params, timeout=self.timeout)
            except requests.exceptions.SSLError as exc:
                raise SystemExit(SSL_HELP.format(error=str(exc)[:300]))
            except requests.RequestException as exc:
                if attempt == self.retries:
                    raise
                wait = self.backoff ** attempt
                log.warning("%s: %s -> retry en %.1fs", activity_id, exc, wait)
                time.sleep(wait)
                continue

            # token expirado: reintenta una sola vez tras re-login
            if resp.status_code in (401, 403) and not relogged:
                log.info("%s: %s -> re-login", activity_id, resp.status_code)
                stale = self.token
                with self._login_lock:
                    if self.token == stale:   # solo el primero renueva
                        self._login()
                relogged = True
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == self.retries:
                    resp.raise_for_status()
                wait = float(resp.headers.get("Retry-After", self.backoff ** attempt))
                log.warning("%s: HTTP %s -> retry en %.1fs",
                            activity_id, resp.status_code, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 404:
                return []

            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("data") if isinstance(payload, dict) else payload
            return data or []

        return []


# --------------------------------------------------------------------------- #
# Parseo
# --------------------------------------------------------------------------- #
# Patrones de nombre de tarifa. El campo 'type' de la API NO es confiable:
# hay operadores que cargan "Menores de 3 anos" con type="adult", asi que la
# clasificacion se hace por nombre + rango de edad y 'type' queda como respaldo.
MINOR_PAT = re.compile(
    r"\b(menor(?:es)?|nin[oa]s?|ni[nñ]{1,2}[oa]s?|infant(?:e|il)?s?|beb[eé]s?|baby|"
    r"child(?:ren)?|kids?|junior|youth|teen|ragazz[oi]|bambin[oi]|enfants?|"
    r"kinder|crian[cç]as?|under)\b", re.IGNORECASE)
SENIOR_PAT = re.compile(r"\b(senior|mayor(?:es)?|jubilad[oa]s?|anzian[oi]|pensioner)\b", re.IGNORECASE)
STUDENT_PAT = re.compile(r"\b(student|estudiante|studente|scolaire)\b", re.IGNORECASE)
ADULT_PAT = re.compile(r"\b(adult[oi]?s?|adulte|general|standard|regular)\b", re.IGNORECASE)
AGE_RANGE_PAT = re.compile(r"\((\s*\d+)\s*[-\u2013a]\s*(\d+\s*)\)")


def max_age_upper(name: str) -> Optional[int]:
    """De 'Adult (15 - 64)' saca 64; de 'Menores de 3 anos (0 - 3)' saca 3."""
    uppers = [int(m.group(2)) for m in AGE_RANGE_PAT.finditer(name or "")]
    return max(uppers) if uppers else None


def classify_rate(rate: dict, adult_min_age: int = 16) -> tuple:
    """Devuelve (flags, es_adulto_por_nombre)."""
    name = str(rate.get("name") or "")
    flags = set()
    if MINOR_PAT.search(name):
        flags.add("minor")
    upper = max_age_upper(name)
    if upper is not None and upper < adult_min_age:
        flags.add("minor")          # (0 - 3) es infante aunque el nombre no lo diga
    if SENIOR_PAT.search(name):
        flags.add("senior")
    if STUDENT_PAT.search(name):
        flags.add("student")
    is_adult = bool(ADULT_PAT.search(name)) and "minor" not in flags
    return flags, is_adult


def describe_rates(prices: List[dict]) -> str:
    """Resumen auditable de todas las tarifas de la session."""
    parts = []
    for r in prices or []:
        parts.append(f"{r.get('name')}={r.get('recommendedPrice')}")
    out = " | ".join(parts)
    return out[:250]


def pick_rate(
    prices: List[dict],
    prefer_type: Optional[str],
    min_price: float = 0.01,
    adult_min_age: int = 16,
) -> tuple:
    """
    Elige la tarifa representativa para el PCI y devuelve (tarifa, motivo).
    Descarta precios en cero, infantes, seniors y estudiantes antes de comparar.
    """
    scored = []
    for r in prices or []:
        raw = r.get("recommendedPrice")
        try:
            price = float(raw)
        except (TypeError, ValueError):
            continue
        if price < min_price:
            continue                      # 0.0 nunca es un precio de basket
        flags, is_adult = classify_rate(r, adult_min_age)
        scored.append((r, price, flags, is_adult))

    if not scored:
        return None, "sin_tarifa_valida"

    # 1) nombre explicitamente de adulto
    tier = [x for x in scored if x[3]]
    if tier:
        return min(tier, key=lambda x: x[1])[0], "adulto_por_nombre"

    # 2) sin marcas de menor/senior/estudiante y type=adult
    tier = [x for x in scored
            if not x[2] and str(x[0].get("type", "")).lower() == (prefer_type or "adult").lower()]
    if tier:
        return min(tier, key=lambda x: x[1])[0], "type_adult_sin_marcas"

    # 3) cualquiera sin marcas (ej. "Standard rate")
    tier = [x for x in scored if not x[2]]
    if tier:
        return min(tier, key=lambda x: x[1])[0], "sin_marcas"

    # 4) solo quedan menores/seniors/estudiantes -> no sirve para PCI
    return None, "solo_tarifas_no_adulto"


def resolve_currency(session: dict, rate: Optional[dict]) -> Optional[str]:
    """
    La moneda depende de la ubicacion del producto (EUR/GBP en Europa, moneda
    local en LATAM), asi que la buscamos en varios niveles antes de rendirnos.
    """
    candidates = [
        session.get("currency"),
        session.get("currencyCode"),
        (session.get("operator") or {}).get("currency"),
        (rate or {}).get("currency"),
        (rate or {}).get("currencyCode"),
        ((rate or {}).get("priceDetail") or {}).get("currency"),
        (session.get("priceDetail") or {}).get("currency"),
    ]
    for c in candidates:
        if isinstance(c, str) and c.strip():
            return c.strip().upper()
        if isinstance(c, dict):  # por si viene {"code": "CLP", ...}
            code = c.get("code") or c.get("iso") or c.get("currencyCode")
            if isinstance(code, str) and code.strip():
                return code.strip().upper()
    return None


def rows_from_sessions(
    activity_id: str,
    sessions: List[dict],
    prefer_type: Optional[str],
    fetched_at: str,
    min_price: float = 0.01,
    adult_min_age: int = 16,
) -> List[dict]:
    rows: List[dict] = []
    for s in sessions:
        prices = s.get("prices") or []
        rate, reason = pick_rate(prices, prefer_type, min_price, adult_min_age)
        detail = s.get("priceDetail") or (rate or {}).get("priceDetail") or {}
        operator = (s.get("operator") or {}).get("name")

        base = {
            "activity_id": activity_id,
            "type": s.get("name"),
            "currency": resolve_currency(s, rate),
            "session_id": s.get("id"),
            "session_date": s.get("date") or s.get("startDate"),
            "session_time": s.get("time") or s.get("startTime"),
            "availability": s.get("availability"),
            "session_operator": operator,
            "rate_selection": reason,
            "rates_available": describe_rates(prices),
            "fetched_at": fetched_at,
        }

        if rate is None:
            rows.append({**base, "price": None, "status": reason,
                         "detail": f"tarifas descartadas: {describe_rates(prices) or 'ninguna'}"})
            continue

        rows.append({
            **base,
            "price": rate.get("recommendedPrice"),
            "net_price": detail.get("netPrice"),
            "retail_price": detail.get("retailPrice"),
            "commission": detail.get("commission"),
            "rate_name": rate.get("name"),
            "rate_type": rate.get("type"),
            "status": "ok",
            "detail": "",
        })
    return rows


def keep_earliest_date(rows: List[dict]) -> List[dict]:
    """
    Modo fallback: se queda con la fecha mas cercana que efectivamente tenga
    precio valido (no sirve la primera fecha si esa fecha no arrojo tarifa).
    """
    dates = [r["session_date"] for r in rows
             if r.get("status") == "ok" and r.get("session_date")]
    if not dates:
        return rows
    first = min(dates)
    return [r for r in rows if r.get("session_date") == first]


def dedupe_rows(rows: List[dict], mode: str) -> List[dict]:
    """Una fila por (activity_id, type)."""
    if mode == "none":
        return rows
    out: Dict[tuple, dict] = {}
    for r in rows:
        key = (r["activity_id"], r.get("type"))
        cur = out.get(key)
        if cur is None:
            out[key] = r
            continue
        if mode == "min":
            new_p, cur_p = r.get("price"), cur.get("price")
            if new_p is not None and (cur_p is None or float(new_p) < float(cur_p)):
                out[key] = r
    return list(out.values())


# --------------------------------------------------------------------------- #
# I/O
# --------------------------------------------------------------------------- #
def read_activities(
    path: str,
    id_column: str,
    keep: List[str],
    limit: Optional[int],
) -> tuple:
    """
    Devuelve (lista de dicts con las columnas conservadas, columnas resueltas).
    Deduplica por activity_id preservando el orden del CSV.
    """
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        sample = fh.read(8192)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(fh, dialect=dialect)
        headers = [(h or "").strip() for h in (reader.fieldnames or [])]
        lower = {h.lower(): h for h in headers}

        id_match = lower.get(id_column.lower())
        if not id_match:
            raise SystemExit(
                f"[input] no encontre la columna '{id_column}'. "
                f"Columnas disponibles: {headers}"
            )

        # resolver que columnas conservar
        if keep == ["all"]:
            wanted = list(headers)
        elif keep == ["none"]:
            wanted = [id_match]
        else:
            wanted, missing = [], []
            for k in keep:
                real = lower.get(k.lower())
                (wanted if real else missing).append(real or k)
            if missing:
                log.warning("Columnas pedidas que no estan en el CSV: %s",
                            ", ".join(missing))
        if id_match not in wanted:
            wanted.insert(0, id_match)

        # colisiones con las columnas de la API -> prefijo src_
        rename = {}
        for col in wanted:
            if col in API_FIELDS:
                rename[col] = f"src_{col}"
                log.warning("Columna '%s' choca con una de la API -> se escribe "
                            "como 'src_%s'", col, col)
        resolved = [rename.get(c, c) for c in wanted]

        rows, seen = [], set()
        for raw_row in reader:
            act_id = (raw_row.get(id_match) or "").strip()
            if not act_id or act_id in seen:
                continue
            seen.add(act_id)
            meta = {rename.get(c, c): (raw_row.get(c) or "").strip() for c in wanted}
            meta["activity_id"] = act_id
            rows.append(meta)
            if limit and len(rows) >= limit:
                break

    if "activity_id" not in resolved:
        resolved.insert(0, "activity_id")
    return rows, resolved


def read_existing(path: str) -> tuple:
    """Devuelve (activity_ids ya procesados, header del archivo)."""
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            header = list(reader.fieldnames or [])
            ids = {r["activity_id"] for r in reader if r.get("activity_id")}
            return ids, header
    except FileNotFoundError:
        return set(), []


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def parse_day(value: str) -> "date":
    """Acepta YYYY-MM-DD, o 'today'/'hoy' para la fecha actual."""
    v = (value or "").strip().lower()
    if v in ("today", "hoy", "now"):
        return date.today()
    try:
        return datetime.strptime(v, "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit(
            f"[fecha] '{value}' no es valido. Usa YYYY-MM-DD o 'today'."
        )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Precios de actividades Globick")
    p.add_argument("--version", action="version",
                   version=f"globick_prices {__version__}")
    p.add_argument("--credentials", default="credentials.json")
    p.add_argument("--api-key-field", default=None,
                   help="ruta explicita al api key, ej: globick.llave")
    p.add_argument("--username-field", default=None)
    p.add_argument("--password-field", default=None)
    p.add_argument("--check-credentials", action="store_true",
                   help="solo valida el JSON y prueba el login, sin leer el CSV")
    p.add_argument("--ca-bundle", default=None,
                   help="ruta a un bundle PEM (certifi + CA corporativo)")
    p.add_argument("--insecure", action="store_true",
                   help="ultimo recurso: no verifica el certificado TLS")
    p.add_argument("--no-truststore", action="store_true",
                   help="no usar el trust store del OS aunque truststore este instalado")
    p.add_argument("--input", default="BOKUN-TUR.csv")
    p.add_argument("--output", default="globick_precios_v2.csv")
    p.add_argument("--id-column", default="activity_id")
    p.add_argument("--keep-columns", default=",".join(INPUT_KEEP_DEFAULT),
                   help="columnas del CSV a arrastrar al output; "
                        "'all' = todas, 'none' = solo activity_id")
    p.add_argument("--date", default="today",
                   help="fecha base: YYYY-MM-DD o 'today' (default)")
    p.add_argument("--fallback-days", type=int, default=60,
                   help="si el dia no arroja precio, amplia N dias y usa la "
                        "fecha mas cercana con precio (0 = desactivado)")
    p.add_argument("--min-price", type=float, default=0.01,
                   help="descarta tarifas por debajo de esto (0.0 = infantes)")
    p.add_argument("--adult-min-age", type=int, default=16,
                   help="una tarifa con rango de edad que termina antes de esta "
                        "edad se trata como menor, diga lo que diga el 'type'")
    p.add_argument("--prefer-type", default="adult",
                   help="tipo de tarifa preferido; vacio = la mas baja")
    p.add_argument("--dedupe", choices=["min", "first", "none"], default="min")
    p.add_argument("--workers", type=int, default=6,
                   help="requests en paralelo (1 = secuencial)")
    p.add_argument("--rps", type=float, default=10.0,
                   help="techo global de requests por segundo (0 = sin techo)")
    p.add_argument("--probe-days", type=int, default=7,
                   help="ventana corta de la primera pasada; solo se amplia a "
                        "--fallback-days si no encontro precio")
    p.add_argument("--log-every", type=int, default=25,
                   help="cada cuantas actividades reportar avance y ETA")
    p.add_argument("--sleep", type=float, default=0.0,
                   help="pausa entre requests (solo con --workers 1)")
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def process_activity(client, meta: dict, args, day, day_str: str, last_day) -> tuple:
    """
    Trae y parsea una actividad. Devuelve (rows, uso_fallback).
    Corre en un worker: no toca estado compartido ni escribe el CSV.
    """
    activity_id = meta["activity_id"]
    fetched_at = datetime.now().isoformat(timespec="seconds")
    used_fallback = False

    def parse(sessions):
        return rows_from_sessions(
            activity_id, sessions, args.prefer_type or None, fetched_at,
            args.min_price, args.adult_min_age,
        )

    try:
        # Fase 1: ventana corta. La mayoria de los productos resuelve aca y el
        # payload es una fraccion del de 60 dias, que es lo que cuesta tiempo.
        probe_end = min(day + timedelta(days=max(args.probe_days, 0)), last_day)
        rows = parse(client.get_sessions(
            activity_id, day_str, probe_end.isoformat()
        ))

        # Fase 2: solo si la ventana corta no dio ningun precio.
        if not any(r["status"] == "ok" for r in rows) and probe_end < last_day:
            wide = parse(client.get_sessions(
                activity_id, day_str, last_day.isoformat()
            ))
            if any(r["status"] == "ok" for r in wide) or len(wide) > len(rows):
                rows = wide

        exact = [r for r in rows if r.get("session_date") == day_str]
        if any(r["status"] == "ok" for r in exact):
            rows = exact
        elif any(r["status"] == "ok" for r in rows):
            rows = keep_earliest_date(rows)
            for r in rows:
                r["detail"] = (f"fallback: {day_str} sin precio, "
                               f"se uso {r.get('session_date')}")
            used_fallback = True
        elif exact:
            rows = exact          # sin precio en toda la ventana

        for r in rows:
            sd = r.get("session_date")
            if sd:
                try:
                    r["days_ahead"] = (
                        datetime.strptime(sd, "%Y-%m-%d").date() - day
                    ).days
                except ValueError:
                    pass

        rows = dedupe_rows(rows, args.dedupe)

        if not rows:
            rows = [{
                "activity_id": activity_id,
                "status": "sin_sesiones",
                "detail": (f"sin disponibilidad entre {day_str} y "
                           f"{last_day.isoformat()}"),
                "fetched_at": fetched_at,
            }]
    except Exception as exc:  # noqa: BLE001 - un fallo no corta el batch
        log.error("%s: %s", activity_id, exc)
        rows = [{
            "activity_id": activity_id,
            "status": "error",
            "detail": f"{type(exc).__name__}: {exc}"[:300],
            "fetched_at": fetched_at,
        }]

    # los metadatos del CSV se repiten en cada version del producto
    return [{**meta, **r} for r in rows], used_fallback


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    # --- credenciales y login PRIMERO: si algo falla, falla en el segundo cero
    if not args.no_truststore and not args.insecure:
        enable_truststore()

    creds = load_credentials(args.credentials, {
        "api_key": args.api_key_field,
        "username": args.username_field,
        "password": args.password_field,
    })
    client = GlobickClient(
        creds,
        timeout=args.timeout,
        retries=args.retries,
        ca_bundle=args.ca_bundle,
        insecure=args.insecure,
        rps=args.rps,
        pool_size=max(args.workers * 2, 8),
    )
    client.login()
    if args.check_credentials:
        log.info("Credenciales y login OK. Nada mas por hacer (--check-credentials).")
        return 0

    day = parse_day(args.date)
    day_str = day.isoformat()
    last_day = day + timedelta(days=max(args.fallback_days, 0))
    log.info("Ventana de consulta: %s -> %s (%d dia/s), sondeo inicial %d dia/s",
             day_str, last_day.isoformat(), (last_day - day).days + 1,
             min(args.probe_days, args.fallback_days) + 1)
    log.info("Concurrencia: %d worker/s, techo %s req/s",
             args.workers, args.rps or "sin")
    keep = [c.strip() for c in args.keep_columns.split(",") if c.strip()]
    activities, keep_cols = read_activities(
        args.input, args.id_column, keep, args.limit
    )
    output_fields = keep_cols + [f for f in API_FIELDS if f not in keep_cols]

    done, prev_header = read_existing(args.output) if args.resume else (set(), [])
    if prev_header:
        # al hacer append hay que respetar el header ya escrito
        if prev_header != output_fields:
            log.warning("El header existente difiere del calculado; uso el del "
                        "archivo para no desalinear las columnas.")
        output_fields = prev_header
    pending = [a for a in activities if a["activity_id"] not in done]
    log.info("%d actividades en el CSV | %d pendientes", len(activities), len(pending))
    log.info("Columnas del output (%d): %s", len(output_fields),
             ", ".join(output_fields))

    write_header = not done
    mode = "a" if done else "w"
    stats = {"ok": 0, "sin_sesiones": 0, "sin_tarifa_valida": 0,
             "solo_tarifas_no_adulto": 0, "error": 0}
    currencies: Counter = Counter()
    mixed_currency_ids: List[str] = []
    n_fb = 0
    lead_times: List[int] = []

    t0 = time.monotonic()
    with open(args.output, mode, encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=output_fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()

        def record(meta: dict, rows: List[dict], fb: bool, n: int) -> None:
            """Se ejecuta solo en el thread principal: el writer no es thread-safe."""
            nonlocal n_fb
            if fb:
                n_fb += 1
            for r in rows:
                writer.writerow(r)
                st = r.get("status", "error")
                stats[st] = stats.get(st, 0) + 1
                if st == "ok":
                    currencies[r.get("currency") or "(sin moneda)"] += 1
                    if isinstance(r.get("days_ahead"), int):
                        lead_times.append(r["days_ahead"])
            fh.flush()

            found = {r.get("currency") for r in rows
                     if r.get("status") == "ok" and r.get("currency")}
            if len(found) > 1:
                mixed_currency_ids.append(meta["activity_id"])
                log.warning("%s: versiones en monedas distintas (%s)",
                            meta["activity_id"], ", ".join(sorted(found)))

            if n % args.log_every == 0 or n == len(pending):
                done_n = n
                elapsed = time.monotonic() - t0
                rate = done_n / elapsed if elapsed else 0
                eta = (len(pending) - done_n) / rate if rate else 0
                log.info("[%d/%d] %.1f act/s | ETA %s | ok=%d sin_precio=%d error=%d",
                         done_n, len(pending), rate,
                         f"{int(eta // 60)}m{int(eta % 60):02d}s",
                         stats["ok"],
                         stats["sin_sesiones"] + stats["sin_tarifa_valida"]
                         + stats["solo_tarifas_no_adulto"],
                         stats["error"])

        if args.workers <= 1:
            for n, meta in enumerate(pending, 1):
                rows, fb = process_activity(client, meta, args, day, day_str, last_day)
                record(meta, rows, fb, n)
                if args.sleep:
                    time.sleep(args.sleep)
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(process_activity, client, m, args,
                                day, day_str, last_day): m
                    for m in pending
                }
                for n, fut in enumerate(as_completed(futures), 1):
                    meta = futures[fut]
                    rows, fb = fut.result()
                    record(meta, rows, fb, n)

    total = sum(stats.values())
    elapsed = time.monotonic() - t0
    log.info("Listo en %dm%02ds. %d filas en %s (%d actividades, %.1f act/s)",
             int(elapsed // 60), int(elapsed % 60), total, args.output,
             len(pending), len(pending) / elapsed if elapsed else 0)
    log.info("Resumen: %s", " | ".join(f"{k}={v}" for k, v in stats.items()))
    if currencies:
        log.info("Monedas: %s", " | ".join(
            f"{cur}={n}" for cur, n in currencies.most_common()))
    if currencies.get("(sin moneda)"):
        log.warning("%d fila(s) con precio pero sin moneda: filtralas o revisa "
                    "el payload crudo antes de convertir a una moneda comun.",
                    currencies["(sin moneda)"])
    if mixed_currency_ids:
        log.warning("%d actividad(es) con versiones en monedas distintas: %s",
                    len(mixed_currency_ids), ", ".join(mixed_currency_ids[:10]))
    if lead_times:
        lt = sorted(lead_times)
        p50 = lt[len(lt)//2]
        log.info("days_ahead de las filas con precio: min=%d | p50=%d | max=%d",
                 lt[0], p50, lt[-1])
        if lt[-1] - lt[0] > 30:
            log.warning("La dispersion de days_ahead es alta (%d dias entre el "
                        "primero y el ultimo). Los precios no son de la misma "
                        "antelacion, revisa esto antes de armar el basket PCI.",
                        lt[-1] - lt[0])
    if n_fb:
        log.info("%d actividad(es) resueltas con fallback de fecha "
                 "(revisa la columna detail).", n_fb)
    if stats.get("solo_tarifas_no_adulto"):
        log.warning("%d session(es) solo tenian tarifas de menor/senior/estudiante: "
                    "quedaron sin precio a proposito, mira 'rates_available'.",
                    stats["solo_tarifas_no_adulto"])
    if stats.get("error") or stats.get("sin_sesiones"):
        log.warning(
            "Revisa las filas con status error/sin_sesiones antes de calcular PCI."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())