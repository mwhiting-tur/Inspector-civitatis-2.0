"""
proxy_rotation.py
Módulo de proxies gratuitos rotativos para usar localmente
cuando GitHub Actions no está disponible.

USO:
    from gyg.proxy_rotation import ProxyPool
    pool = ProxyPool()
    proxy = pool.siguiente()   # dict listo para curl_cffi proxies=...

FUENTE: ProxyScrape (gratis, sin API key)
"""

import requests
import random
import threading
import time
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FUENTES_PROXIES = [
    # ProxyScrape — lista fresca cada 5 min
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=elite,anonymous",
    # Backup: ProxyList.to
    "https://www.proxy-list.download/api/v1/get?type=http&anon=elite",
]

TEST_URL     = "https://httpbin.org/ip"
REFRESH_CADA = 300     # refrescar lista cada 5 minutos


class ProxyPool:
    """Pool de proxies HTTP gratuitos con rotación automática y test de calidad."""

    def __init__(self, min_proxies: int = 5, verbose: bool = True):
        self._lock       = threading.Lock()
        self._proxies    = []
        self._malos      = set()
        self._verbose    = verbose
        self._ultimo_ref = 0
        self.min_proxies = min_proxies
        self._refrescar()

    # ── Público ──────────────────────────────────────────────────────────────

    def siguiente(self) -> dict | None:
        """Devuelve el próximo proxy como dict `{http: ..., https: ...}`."""
        self._refrescar_si_necesario()
        with self._lock:
            buenos = [p for p in self._proxies if p not in self._malos]
            if not buenos:
                if self._verbose:
                    print("⚠️  Pool vacío — recargando...")
                self._refrescar()
                buenos = [p for p in self._proxies if p not in self._malos]
            if not buenos:
                return None
            proxy = random.choice(buenos)
            return {"http": f"http://{proxy}", "https": f"http://{proxy}"}

    def marcar_malo(self, proxy_dict: dict):
        """Reporta un proxy que falló para que no se vuelva a usar."""
        if not proxy_dict:
            return
        url = proxy_dict.get("http", "").replace("http://", "")
        with self._lock:
            self._malos.add(url)

    @property
    def disponibles(self) -> int:
        with self._lock:
            return len([p for p in self._proxies if p not in self._malos])

    # ── Privado ──────────────────────────────────────────────────────────────

    def _refrescar_si_necesario(self):
        if time.time() - self._ultimo_ref > REFRESH_CADA or self.disponibles < self.min_proxies:
            self._refrescar()

    def _refrescar(self):
        nuevos = []
        for fuente in FUENTES_PROXIES:
            try:
                r = requests.get(fuente, timeout=10, verify=False)
                if r.status_code == 200:
                    lineas = [l.strip() for l in r.text.strip().split("\n") if ":" in l]
                    nuevos.extend(lineas)
                    if self._verbose:
                        print(f"  📡 {len(lineas)} proxies desde {fuente[:50]}…")
                    break
            except Exception:
                pass

        with self._lock:
            self._proxies = list(set(nuevos))
            self._malos   = set()   # resetear malos al refrescar
        self._ultimo_ref = time.time()

        if self._verbose:
            print(f"  🔄 Pool: {len(self._proxies)} proxies cargados")


# ── Función de conveniencia para gyg_prices_api.py ───────────────────────────

def fetch_con_proxy_rotativo(url: str, pool: ProxyPool, impersonate="chrome",
                              reintentos: int = 4) -> "requests.Response | None":
    """
    Hace GET a `url` rotando proxies del pool en cada reintento.
    Retorna la Response o None si todos los intentos fallan.
    """
    from curl_cffi import requests as cffi_req

    for intento in range(reintentos):
        proxy = pool.siguiente()
        try:
            resp = cffi_req.get(
                url,
                headers={"Accept-Language": "es-ES,es;q=0.9"},
                impersonate=impersonate,
                proxies=proxy,
                timeout=15,
                verify=False,
            )
            if resp.status_code in (403, 429):
                pool.marcar_malo(proxy)
                time.sleep(2)
                continue
            return resp
        except Exception:
            if proxy:
                pool.marcar_malo(proxy)
            time.sleep(1)

    return None


# ── Modo standalone: test del pool ───────────────────────────────────────────
if __name__ == "__main__":
    print("=== Test del pool de proxies ===")
    pool = ProxyPool()
    print(f"Proxies disponibles: {pool.disponibles}")

    for i in range(3):
        p = pool.siguiente()
        print(f"\nTest {i+1} con proxy: {p}")
        try:
            r = requests.get(TEST_URL, proxies=p, timeout=8, verify=False)
            print(f"  ✅ IP vista por el servidor: {r.json().get('origin', '?')}")
        except Exception as e:
            print(f"  ❌ Falló: {e}")
            pool.marcar_malo(p)
