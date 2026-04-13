import pandas as pd
import unicodedata

def limpiar_texto(texto):
    """
    Convierte el texto a minúsculas, quita espacios en los extremos 
    y elimina los acentos (diacríticos).
    """
    if pd.isna(texto):
        return ""
    
    # Convertir a string, minúsculas y quitar espacios extra
    texto = str(texto).lower().strip()
    
    # Quitar acentos
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto

def main():
    # 1. Cargar los archivos CSV
    # (Asegúrate de que los nombres de los archivos sean los correctos en tu carpeta)
    print("Cargando archivos CSV...")
    try:
        df_wtm = pd.read_csv('wtm-latam-2026.csv', encoding='utf-8') 
        df_bq = pd.read_csv('gyg/metadata_latam_BQ.csv', sep=';', encoding='utf-8-sig')
    except FileNotFoundError as e:
        print(f"Error: No se encontró el archivo. {e}")
        return

    # 2. Verificar que existan las columnas
    if 'text-clamp' not in df_wtm.columns:
        print("Error: La columna 'text-clamp' no existe en el archivo de WTM.")
        return
    if 'proveedor' not in df_bq.columns:
        print("Error: La columna 'proveedor' no existe en el archivo BQ.")
        return

    # 3. Limpiar los textos en ambos DataFrames creando columnas auxiliares
    print("Limpiando textos (minúsculas y sin acentos)...")
    df_wtm['wtm_limpio'] = df_wtm['text-clamp'].apply(limpiar_texto)
    df_bq['proveedor_limpio'] = df_bq['proveedor'].apply(limpiar_texto)

    # Convertimos la lista de WTM limpia a una lista de Python para iterar rápido
    lista_wtm = df_wtm['wtm_limpio'].tolist()
    lista_wtm_original = df_wtm['text-clamp'].tolist()

    # Listas para guardar los resultados
    asiste_wtm = []
    nombre_en_wtm = []

    print("Realizando el cruce de datos (estilo SQL LIKE)...")
    # 4. Iterar sobre tu base de datos para buscar coincidencias
    for prov in df_bq['proveedor_limpio']:
        if prov == "":
            asiste_wtm.append("No")
            nombre_en_wtm.append("N/A")
            continue
            
        encontrado = False
        nombre_match = "N/A"
        
        for i, wtm_nombre in enumerate(lista_wtm):
            if wtm_nombre == "":
                continue
                
            # Condición tipo "LIKE": revisa si el proveedor está dentro del nombre de WTM
            # o si el nombre de WTM está dentro del nombre del proveedor
            if (prov in wtm_nombre) or (wtm_nombre in prov):
                encontrado = True
                nombre_match = lista_wtm_original[i] # Guardamos cómo aparece realmente en la web
                break # Si encuentra coincidencia, pasa al siguiente proveedor
                
        if encontrado:
            asiste_wtm.append("Sí")
            nombre_en_wtm.append(nombre_match)
        else:
            asiste_wtm.append("No")
            nombre_en_wtm.append("N/A")

    # 5. Agregar los resultados al DataFrame original
    df_bq['Asiste_WTM'] = asiste_wtm
    df_bq['Nombre_en_catalogo_WTM'] = nombre_en_wtm

    # Borramos la columna auxiliar para dejar el archivo limpio
    df_bq = df_bq.drop(columns=['proveedor_limpio'])

    # 6. Guardar el resultado en un nuevo CSV
    output_file = 'resultado_cruce_wtm.csv'
    df_bq.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"¡Cruce finalizado! Se ha guardado el resultado en: {output_file}")
    
    # Pequeño resumen
    total = len(df_bq)
    asisten = asiste_wtm.count("Sí")
    print(f"\nResumen:")
    print(f"- Total de proveedores en tu lista: {total}")
    print(f"- Proveedores encontrados en WTM: {asisten}")

if __name__ == "__main__":
    main()