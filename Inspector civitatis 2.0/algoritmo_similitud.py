import os
import ssl
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer, CrossEncoder, util
from scipy.optimize import linear_sum_assignment
from tqdm import tqdm


# NOMBRES DE TUS ARCHIVOS (Asegúrate que coincidan)
ARCHIVO_PEQUENO = './data/actividades_triviantes.csv' # Tu archivo de 200
ARCHIVO_GRANDE = './data/colombia_civitatis_20260130_112927.csv' # Tu archivo de 1000

# PARCHE SSL
os.environ['CURL_CA_BUNDLE'] = ''
ssl._create_default_https_context = ssl._create_unverified_context

# 1. CARGAR DATOS
# Asegúrate de que los nombres de las columnas coincidan con tus CSV
df_a = pd.read_csv(ARCHIVO_PEQUENO) 
df_b = pd.read_csv(ARCHIVO_GRANDE)

def preparar(df):
    return (df['destino'].astype(str) + " | " + df['actividad'].astype(str)).tolist()

def preparar_texto_enriquecido(df, es_query=True):
    prefix = "query: " if es_query else "passage: "
    textos = []
    for _, r in df.iterrows():
        # Estructura: DESTINO - ACTIVIDAD. Contexto: DESCRIPCION
        combinado = f"{r['destino']} - {r['actividad']}. Contexto: {r['descripcion']}"
        textos.append(prefix + combinado)
    return textos

textos_a = preparar_texto_enriquecido(df_a)
textos_b = preparar_texto_enriquecido(df_b)

# 2. MODELO LIGERO (FILTRO)
print("Fase 1: Generando candidatos rápidos...")
bi_model = SentenceTransformer('intfloat/multilingual-e5-large')
emb_a = bi_model.encode(textos_a, convert_to_tensor=True, show_progress_bar=True)
emb_b = bi_model.encode(textos_b, convert_to_tensor=True, show_progress_bar=True)

# Buscamos los 50 mejores candidatos
top_k = 50
hits = util.semantic_search(emb_a, emb_b, top_k=top_k)

# 3. MODELO PESADO (PRECISIÓN CRÍTICA)
print(f"Fase 2: Re-ranking de alta precisión (Usando Core Ultra 7)...")
cross_model = CrossEncoder('cross-encoder/ms-marco-MiniLM-L12-v2', max_length=512)

sim_matrix = np.full((len(textos_a), len(textos_b)), -100.0)

for i, hit_list in enumerate(tqdm(hits, desc="Analizando pares")):
    indices_candidatos = [h['corpus_id'] for h in hit_list]
    pares = [[textos_a[i], textos_b[idx]] for idx in indices_candidatos]
    
    # batch_size=32 aprovecha mejor tus 14 núcleos
    scores = cross_model.predict(pares, batch_size=16, show_progress_bar=False)
    
    for score, idx_b in zip(scores, indices_candidatos):
        sim_matrix[i, idx_b] = score

# 4. ASIGNACIÓN ÚNICA
print("Fase 3: Ejecutando Algoritmo Húngaro para parejas únicas...")
row_ind, col_ind = linear_sum_assignment(-sim_matrix)

# 5. RESULTADOS
resultados = []
for i, j in zip(row_ind, col_ind):
    score = sim_matrix[i, j]
    if score > -10: # Filtro de seguridad
        resultados.append({
            'ID_A': i,
            'Destino_A': df_a.iloc[i]['destino'],
            'Actividad_A': df_a.iloc[i]['actividad'],
            'Precio_A': df_a.iloc[i]['precio'],
            'Destino_B': df_b.iloc[j]['destino'],
            'Actividad_B': df_b.iloc[j]['actividad'],
            'Precio_B': df_b.iloc[j]['precio'],
            'Confianza_Score': round(float(score), 4)
        })

df_res = pd.DataFrame(resultados).sort_values(by='Confianza_Score', ascending=False)
df_res.to_csv('cruce_final_preciso.csv', index=False)
print(f"Éxito. Se encontraron {len(df_res)} parejas óptimas.")