import os
import pandas as pd
import zipfile

def consolidar_zips():
    print("🔄 Buscando archivos ZIP en la carpeta actual...")
    archivos_zip = [f for f in os.listdir('.') if f.endswith('.zip')]
    
    if not archivos_zip:
        print("⚠️ No se encontraron archivos .zip en la carpeta.")
        return

    df_list = []
    
    for archivo in archivos_zip:
        try:
            # Abrimos el ZIP sin extraerlo en el disco
            with zipfile.ZipFile(archivo, 'r') as z:
                for filename in z.namelist():
                    # Solo leemos los archivos CSV que NO sean los viejos de países
                    if filename.endswith('.csv') and 'paises' not in filename:
                        print(f"   📥 Extrayendo: {filename} (desde {archivo})")
                        with z.open(filename) as f:
                            df = pd.read_csv(f, sep=';', encoding='utf-8-sig')
                            df_list.append(df)
        except Exception as e:
            print(f"❌ Error leyendo el zip {archivo}: {e}")

    if df_list:
        print("\n⚙️ Uniendo todos los datos...")
        df_final = pd.concat(df_list, ignore_index=True)
        
        # Eliminar posibles duplicados absolutos
        df_final = df_final.drop_duplicates()
        
        archivo_salida = 'BASE_CIVITATIS_COMPLETA.csv'
        df_final.to_csv(archivo_salida, sep=';', index=False, encoding='utf-8-sig')
        print(f"✅ ¡Éxito! Se ha creado '{archivo_salida}' con un total de {len(df_final)} reviews únicas.")
    else:
        print("⚠️ No se encontraron datos CSV válidos dentro de los zips.")

if __name__ == "__main__":
    consolidar_zips()