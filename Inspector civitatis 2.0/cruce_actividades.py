import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# NOMBRES DE TUS ARCHIVOS (Asegúrate que coincidan)
ARCHIVO_PEQUENO = './data/actividades_triviantes.csv' # Tu archivo de 200
ARCHIVO_GRANDE = './data/colombia_civitatis_20260129_140055.csv' # Tu archivo de 1000

def cruzar_archivos():
    print("Cargando archivos...")
    try:
        df1 = pd.read_csv(ARCHIVO_PEQUENO)
        df2 = pd.read_csv(ARCHIVO_GRANDE)
    except FileNotFoundError as e:
        print(f"Error: No encuentro el archivo {e.filename}. Verifica el nombre.")
        return

    # Limpieza básica para asegurar que sean texto
    df1['actividad'] = df1['actividad'].fillna('').astype(str)
    df2['actividad'] = df2['actividad'].fillna('').astype(str)

    print("Analizando similitudes entre textos (esto tomará unos segundos)...")
    
    # Usamos TF-IDF con n-gramas (compara grupos de letras, no solo palabras)
    # Esto ayuda a detectar que "Caminata" y "Caminar" son parecidos
    vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(2, 4))
    
    # Entrenamos con todas las actividades para que conozca el vocabulario
    all_activities = pd.concat([df1['actividad'], df2['actividad']]).unique()
    vectorizer.fit(all_activities)
    
    # Transformamos a matrices
    tfidf_1 = vectorizer.transform(df1['actividad'])
    tfidf_2 = vectorizer.transform(df2['actividad'])
    
    # Calculamos similitud
    cosine_sim = cosine_similarity(tfidf_1, tfidf_2)
    
    resultados = []
    
    print(f"Procesando {len(df1)} actividades...")
    
    for idx, row in df1.iterrows():
        # Obtenemos los puntajes de similitud para la fila actual
        sim_scores = cosine_sim[idx]
        
        # Buscamos el mejor candidato
        best_idx = np.argmax(sim_scores)
        best_score = sim_scores[best_idx]
        
        match_row = df2.iloc[best_idx]
        
        resultados.append({
            'Destino': row.get('destino', ''),
            'Actividad_Triviantes': row['actividad'],
            'Precio_Triviantes': row.get('precio_real', ''),
            'Actividad_Civitatis': match_row['actividad'],
            'Precio_Civitatis': match_row.get('precio_real', ''),
            'Similitud_%': round(best_score * 100, 2),
            'Url_Match': match_row.get('url_fuente', ''), # Agregué la URL por si quieres verificar
            'Cantidad_Viajeros': match_row['viajeros']
        })

    # Guardar archivo final
    df_final = pd.DataFrame(resultados)
    
    # Ordenamos: primero los matches más seguros
    df_final = df_final.sort_values(by='Similitud_%', ascending=False)
    
    nombre_salida = 'resultado_cruce_actividades.csv'
    df_final.to_csv(nombre_salida, index=False, encoding='utf-8-sig')
    
    print(f"¡Listo! Archivo generado: {nombre_salida}")

if __name__ == "__main__":
    cruzar_archivos()