import pandas as pd
import io
from google.cloud import bigquery

print("Limpiando tabla de Civitatis en BigQuery y fijando esquema...")

# ⚠️ IMPORTANTE: Asegúrate de que el nombre de la tabla sea correcto para Civitatis.
# No uses la misma tabla de GYG o este script la borrará.
table_id = "datatur.dbt_tools.civitatis_products_latam" 

bq_client = bigquery.Client()

# Esquema idéntico al de GYG (usando 'string' puro para evitar errores 400)
df_schema = pd.DataFrame({
    'id': pd.Series(dtype='Int64'),
    'pais': pd.Series(dtype='string'),
    'destino': pd.Series(dtype='string'),
    'actividad': pd.Series(dtype='string'),
    'precio_usd': pd.Series(dtype='float64'),
    'content': pd.Series(dtype='string'),
    'url': pd.Series(dtype='string')
})

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    source_format=bigquery.SourceFormat.PARQUET,
)

parquet_buffer = io.BytesIO()
df_schema.to_parquet(parquet_buffer, engine="pyarrow", index=False)
parquet_buffer.seek(0)

load_job = bq_client.load_table_from_file(parquet_buffer, table_id, job_config=job_config)
load_job.result()

print("✅ Tabla de Civitatis limpia y lista para recibir datos en paralelo.")