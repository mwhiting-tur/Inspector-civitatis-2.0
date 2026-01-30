import pandas as pd
from sentence_transformers import SentenceTransformer, util
import torch

def cruzar_con_ia(archivo200, archivo1000, output_name="comparativa_semantica.csv"):
    # 1. Cargar datos
    df1 = pd.read_csv(archivo200)
    df2 = pd.read_csv(archivo1000)

    # 2. Cargar el modelo de IA (Multilingüe para español)
    print("Cargando modelo de lenguaje... (esto ocurre solo la primera vez)")
    model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

    # 3. Convertir actividades en "vectores" (Embeddings)
    print("Analizando significados de las actividades...")
    actividades1 = df1['actividad'].tolist()
    actividades2 = df2['actividad'].tolist()

    embeddings1 = model.encode(actividades1, convert_to_tensor=True)
    embeddings2 = model.encode(actividades2, convert_to_tensor=True)

    # 4. Calcular similitud de coseno
    # Compara cada vector de la lista 1 contra todos los de la lista 2
    cos_scores = util.cos_sim(embeddings1, embeddings2)

    resultados = []

    print("Buscando las mejores coincidencias...")
    for i in range(len(actividades1)):
        # Buscamos el índice con el puntaje más alto en la matriz de similitud
        score, idx_match = torch.max(cos_scores[i], dim=0)
        
        fila_p1 = df1.iloc[i]
        fila_p2 = df2.iloc[idx_match.item()]

        resultados.append({
            'destino': fila_p1['destino'],
            'actividad1': fila_p1['actividad'],
            'precio1': fila_p1['precio_real'],
            'actividad2': fila_p2['actividad'],
            'precio2': fila_p2['precio_real'],
            'similitud_%': round(score.item() * 100, 2)
        })

    # 5. Guardar resultado
    df_final = pd.DataFrame(resultados)
    df_final = df_final.sort_values(by='similitud_%', ascending=False)
    df_final.to_csv(output_name, index=False, encoding='utf-8-sig')
    
    print(f"¡Hecho! Comparación semántica guardada en: {output_name}")

# Uso del script
cruzar_con_ia('./data/actividades_triviantes.csv', './data/colombia_civitatis_20260129_140055.csv')