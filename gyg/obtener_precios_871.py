import requests
import pandas as pd
import re
import time
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Obtener tasa de cambio CLP/USD en tiempo real
print("Obteniendo tasa de cambio CLP/USD...")
try:
    fx = requests.get("https://api.frankfurter.app/latest?from=CLP&to=USD", timeout=10, verify=False).json()
    tasa_clp_usd = fx["rates"]["USD"]
    print(f"  Tasa actual: 1 CLP = {tasa_clp_usd} USD\n")
except:
    tasa_clp_usd = 1 / 950
    print(f"  No se pudo obtener tasa, usando 1/950\n")

df = pd.read_csv("gyg/tours_chile_IDs_2026-05-26.csv", sep=";")

# Filtrar solo las ciudades de interés
CIUDADES = {
    "santiago-de-chile-l226"
}
df = df[df["ciudad_id"].isin(CIUDADES)].reset_index(drop=True)
print(f"Ciudades filtradas: {sorted(df['ciudad_id'].unique())}\n")

total = len(df)
print(f"Total tours: {total}\n")

results = []

for i, row in df.iterrows():
    tour_id = row["tour_id"]
    url = row["url"]
    print(f"[{i+1}/{total}] tour_id: {tour_id}")

    try:
        r = requests.get(url, headers=headers, timeout=15, allow_redirects=True, verify=False)

        if r.status_code != 200:
            print(f"  Status: {r.status_code} — saltando\n")
            results.append({"tour_id": tour_id, "precio_original": f"ERROR_{r.status_code}", "precio_usd": ""})
            time.sleep(1)
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        precio_original = None

        for el in soup.find_all(class_=re.compile(r'price', re.I)):
            text = el.get_text(strip=True)
            if any(c.isdigit() for c in text) and len(text) < 60:
                precio_original = text
                break

        # Convertir a USD
        precio_usd = ""
        if precio_original:
            numeros = re.sub(r'[^\d]', '', precio_original.replace(".", "").replace(",", ""))
            if numeros:
                clp = int(numeros)
                precio_usd = round(clp * tasa_clp_usd * 1.02, 2)

        print(f"  Original: {precio_original} | USD: {precio_usd}\n")
        results.append({
            "tour_id": tour_id,
            "precio_original": precio_original or "NO ENCONTRADO",
            "precio_usd": precio_usd
        })

    except Exception as e:
        print(f"  ERROR: {e}\n")
        results.append({"tour_id": tour_id, "precio_original": "ERROR", "precio_usd": ""})

    time.sleep(1.5)

    if (i + 1) % 50 == 0:
        pd.DataFrame(results).to_csv("precios_progreso.csv", index=False)
        print(f"  >>> Progreso guardado ({i+1}/{total})\n")

df_precios = pd.DataFrame(results)
df_final = df.merge(df_precios, on="tour_id", how="left")
df_final.to_excel(f"gyg/tours_{df['tour_id'].iloc[0]}_con_precios.xlsx", index=False)
print(f"\n✅ Listo! Guardado en tours_{df['tour_id'].iloc[0]}_con_precios.xlsx")
print(f"   Precios encontrados: {(df_precios['precio_usd'] != '').sum()}/{total}")
