import pandas as pd
import csv

archivo_original = 'gyg/metadata_latam_FINAL.csv'
archivo_limpio = 'gyg/metadata_latam_BQ.csv'

print("Limpiando comillas para BigQuery...")

# Leemos el CSV diciéndole a Pandas que ignore las comillas temporalmente (quoting=3)
df = pd.read_csv(archivo_original, sep=';', quoting=csv.QUOTE_NONE, dtype=str)

# Reemplazamos cualquier comilla doble (") por una comilla simple (') en todo el documento
df = df.replace('"', "'", regex=True)

# Guardamos el nuevo archivo limpio
df.to_csv(archivo_limpio, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONE)

print("¡Listo! Sube el archivo 'metadata_latam_BQ.csv' a BigQuery.")