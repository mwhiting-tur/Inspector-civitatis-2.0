import csv
import re
import time
import random
import requests

INPUT_CSV = 'viator/viator.csv'
OUTPUT_CSV = 'viator/viator_con_proveedores.csv'

# =============================================================================
# SESSION COOKIES — refresh these when the script starts getting 403s
# Get fresh values: Chrome on viator.com → F12 → Network → any supplier-details
# request → Headers → copy the Cookie header and the values below
# =============================================================================

DATADOME = (
    "d32yBKSk~Y~wZIFuLvoomkGyOa1BFK9Dzjdmo9A2T5P5eBHpRDv1e6YG2j_RLq3"
    "MYwFHzb3LblgK2SJNowPVMA~Ujfa5emwfms_Pe7M0dmRk7Z1gPYDygL~D15nswala"
)

XSRF_TOKEN = "726aae99-b748-42ca-bfe4-8577cc73a2d3"

ORION_SESSION = (
    "g11qYezhNtKgqP2aox8Z2g%3D%3D%7CeeZYC8HjXDiqAvlC7AZot8fInAHfKCXTLu"
    "UJIxAYoTZ5BRcr%2F1%2FuG5YaBDGdZOqrOJePGPaJB631O4DynMrx5WbBKUeDIFN"
    "0nAIZWDSh9Z%2BzmpBEMAhODaql8fXBNF%2FQ0W5V3lAPv6B%2B1%2B27Iv9vL6WvS"
    "6UftBSJmm%2FcMdsa4hrMS4clLXsoBtGOP2UhAwyJXGxMAN2gVb%2FVm8g%2BsSBFx"
    "p60sW9cklOaaK0X3PtZP%2Bm9iyeBA7kBeYIZp0NrFFla2WtBgAHI7wvdzwLMoIcah"
    "b2mErQzTw%2B6WlmeGRoCwDouo0bfxTTh%2BQeBHnaWGNaTBZHdp34i946gWkFwqw9"
    "U0ZgRoa13Z7PNUkhE1K8SRgb1r%2BZO4mAQvWo4m%2FCpTbvR0I5KdW2gZMvVgaCP4"
    "IzA047SsKatF6OLQpVVZP1nZcLSdCWhhICX9q05WSYwY2TiOg%2Be5ApDNCLwDb0lJ"
    "5SX8KZUocgdrmYFSmwPi2DQd9Wjw631HO9nMlnKYCRpz1RtqBqGWfnQHpMfMPyzmZ"
    "lbs5W%2FCmbrgZ%2FMgmrjwEpd8ATYYpOYpnEnIMf2h5aNw0rfty7AYrM1Jk%2Fnf7"
    "4h14Xn9irignDClrakP%2FEDF1nDyOjmQ1Z%2BFpXRNuCa8N%2Fm93DYeXJQEtSXUl"
    "yYbLW%2BKBwDBdlaGfWSu%2BhdCvGqc5xQAoicrGJCCwhQo6L1aMSb7B6XQcFdy5Eg"
    "1Ryt4RX0AH%2BaSZFT3vY%2Fkz%2FgMGxoFvtRTh4LW2vBzuN2G6HOgYIdn%2FcVau"
    "c1qeypOz0ct9mk3wqqw%2BM9f6cf6U72chDG7YCx2ktFJBUyeUwFBQm4%2FLurNvuN"
    "Ep%2BhGogjzNJUJ6S7lqKbDOjTE8ccOhpPdChdsNBqXoQXiFCOpk1LiMFJj%2FfUt5"
    "TCC4Qhhe97%2BKNSSiu6a7ma7%2Fb4dz%2B2Yb16uHRi3MyKxeePehwrrOwEKuTUVf1"
    "30iamMA0PbtytvjGpN3WRDcZ0ELlLvoZDZUEQnXEZJaiA8I6%2Ffw7A3B%2BuoIArli"
    "7xkpRN9j9eW525DCD3%2Bl5%2FjsTZLvV6u5rEh29T01llEbcGfyDwT%2Fx1IzUmNFf"
    "Gq0Z4stYOjOJ%2FQvAxsTasyFYAqf27gHRRIgpKLdXj%2B9PjjNAbFN2jQvUjvg4K%"
    "2F5ZZkShdlVlbAahHn4LrowcZVQS1Jmzha3oJhXqxy7Va9Pz4ov4XJVJZUUWB4JCRB"
    "2UmRCHO4u1AzPh1UqgqF45adSBaylP0URPyefnD080WnwFmA3oFUZuWX2vovP4cifHB"
    "%2F5eGIMGF4gRqLB4Q2YxStFnQxZFaqHGAkiSnhUaXsGVmhA2KUshK%2FVMib3LRJX"
    "VgJMaVxXH%2BNb4sOphanYAEZWOTN1PuM3SLNuJghG8E87IkVthTn4psC2ACra9xW%"
    "2FnrA2jp9qZ6Bu8ifJbrAjcBrs%2Bl1tyFkzGa%2B725qXLsIrUtppGlJwiR5Ud59Fe"
    "0IshLD6FUDWZSxBZ4wNuzmUu%2Fs1%2BtAXvM0FY%2BUou6e1yEcZyRyJs811SeKEKv"
    "m9DIRu6BxSH6DrebY3szfcHXSTqW%2FkKbTZXWarlXMa6U1Whk3MBJPhJmLWkU2A6hF"
    "xxOe5Eh0LOTPaG5pb2St2iEW6CgOizgudOE9EybapElZXBp8xkBrYMM6DiLGvhfbKIH"
    "yNjP0LpxAwpTmqNqTgNGym4qUBf3FAtskePTnHl4NA5DJ5QpRrMWE3zExTLnMXPySq5"
    "KVJAlakgy024J0ptFPIrg4EIoYZTAcXWLmw1vTd8%2BHvtR4LFkQ4gwT4ZFkhIB7Wl0"
    "PeEF1cpA%2Bs56dk7ETchuAxCtXjU5R2UZYWvGdZeQMytgM03yNVsCZM2mKNWwy4KyQt"
    "HIRUUflEOzKF5tGRJyLLBk7JnntJ2wjML%2FGToeyqtfjV6YvcgjFoLEdh9Np%2BmLOr"
    "JElhM2NPWF%2BbIP2%2B1P%2FjGFSDsBC6afgz514jACF1iAXoT%2BIU0qAzdvaqjMzN"
    "ocbzICcJ1qbD1uU%2FBn%2F4rSIi7TMbJ5jl1IGiqeW42H0tl%2BJZcIgM0M0%2BwPOn"
    "H6FCgAKGN9DgGIqtX4q8ZuB1XhZx7zFRTJlgxSfWxeZQdOt1Nn5KVp7vcHhroWcUZy"
    "NP0AllKRVbnT2Echhbo0I6igvv%2FTwXIsm5%2Bsbc0IeAIPPRZBamo6AczBr%2Bz%2F"
    "Wx0x8eXeneOA7TWYbyAC%2BmTqh3qOboUZvG3u2GaoWdVvgGchjwVhaQdop3AL%2BW6O"
    "JWoLvv1cOFW597Qg3VwRhLOlk3IeUoN3eA5%2FNkDi1LZSuetjJd09bkAy7NWUqoDno"
    "VfkznxoiraR37f7wrB5PpYZFtxWnuiM6vwumYPaGQdp%2Ba24CmHLroM43ZmNUiso9%"
    "2FYmRJreQ2LvM%2F35QgP64hYcSLHsnZf6DxRNjgWJBu4YL7b7e8ibHOtUBCFqDz7kd"
    "LkJxdjjBw%3D%3D%7CsoNBjlfiEWM%3D%3A0uv9Dr2wbj2OPIShiuSLim0CIBIwPELY"
    "dPhcWuIhUjA%3D"
)

VIATOR_PERSISTENT = "f8e13ee8-6c99-44af-abd7-53c21abbc5c1"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_product_code(url):
    """Extract product code and numeric prefix from a Viator product URL.

    Example:
        .../d687-5250LIBERTYELLIS  ->  code='5250LIBERTYELLIS', prefix='5250'
    """
    match = re.search(r'/d\d+-([A-Za-z0-9]+?)(?:/[^/]*)?$', url)
    if not match:
        return None, None
    code = match.group(1)
    prefix_match = re.match(r'(\d+)', code)
    prefix = prefix_match.group(1) if prefix_match else None
    return code, prefix


def parse_address(addr):
    """Flatten Viator's address object into a single string."""
    if not addr:
        return ''
    if isinstance(addr, dict):
        parts = [
            addr.get('line1', ''),
            addr.get('line2', ''),
            addr.get('city', ''),
            addr.get('state', ''),
            addr.get('postcode', ''),
            addr.get('countryCode', ''),
        ]
        return ', '.join(p for p in parts if p)
    return str(addr)


def build_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/146.0.0.0 Safari/537.36'
        ),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'es-ES,es;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Datadome-Clientid': DATADOME,
        'X-XSRF-TOKEN': XSRF_TOKEN,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-CH-UA': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"Windows"',
    })
    # Set all cookies that were present in the real browser request
    for name, value in [
        ('datadome', DATADOME),
        ('XSRF-TOKEN', XSRF_TOKEN),
        ('ORION_SESSION', ORION_SESSION),
        ('x-viator-tapersistentcookie', VIATOR_PERSISTENT),
    ]:
        session.cookies.set(name, value, domain='.viator.com')

    return session


def fetch_supplier(session, code, prefix, referer_url):
    """Call the Viator supplier-details AJAX endpoint."""
    api_url = f'https://www.viator.com/orion/ajax/supplier-details/{prefix}/{code}'
    resp = session.get(
        api_url,
        headers={'Referer': referer_url},
        timeout=15,
    )
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 403:
        print('  403 — session cookies have expired. Refresh them in the script.')
    else:
        print(f'  HTTP {resp.status_code}')
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print('Starting Viator supplier scraper...')
    session = build_session()

    with open(INPUT_CSV, 'r', encoding='utf-8') as infile, \
         open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        header = next(reader)
        writer.writerow(header + ['Trading Name', 'Legal Name', 'Address', 'Phone', 'Email', 'Website'])

        for row_idx, row in enumerate(reader, start=1):
            url = row[0].strip().strip('"')
            if not url.startswith('http'):
                writer.writerow(row + [''] * 6)
                continue

            code, prefix = extract_product_code(url)
            if not code or not prefix:
                print(f'[{row_idx}] Cannot parse product code from: {url}')
                writer.writerow(row + [''] * 6)
                continue

            print(f'[{row_idx}] {code}', end=' ... ', flush=True)

            trading_name = legal_name = address = phone = email = website = ''

            try:
                data = fetch_supplier(session, code, prefix, url)
                if data:
                    trading_name = data.get('tradingName', '')
                    legal_name   = data.get('legalName', '')
                    phone        = data.get('contactNumber', '')
                    email        = data.get('contactEmail', '')
                    website      = data.get('websiteUrl', '')
                    address      = parse_address(data.get('address', {}))
                    print(trading_name or '(no name)')
                else:
                    print('no data')
            except Exception as e:
                print(f'error: {e}')

            writer.writerow(row + [trading_name, legal_name, address, phone, email, website])

            # Small delay — lightweight API, no need for long waits
            time.sleep(random.uniform(0.3, 0.8))

    print(f'\nDone. Saved to {OUTPUT_CSV}')


if __name__ == '__main__':
    main()
