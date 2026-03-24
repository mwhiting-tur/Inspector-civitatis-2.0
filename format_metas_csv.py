import pandas as pd

def generar_csv_porcentajes():
    input_file = "Metas_Supply_Mensuales.csv"
    output_file = "Metas_Supply_Mensuales_Porcentajes.csv"
    
    try:
        print(f"Cargando datos desde '{input_file}'...")
        df = pd.read_csv(input_file)
        
        # Get all month columns (everything except the first two identifier columns)
        meses_cols = [col for col in df.columns if col not in ['Ciudad / Destino', 'País']]
        
        # Multiply by 100 and apply the percentage formatting with 1 decimal place
        for col in meses_cols:
            df[col] = (pd.to_numeric(df[col], errors='coerce') * 100).map(lambda x: f"{x:.1f}%" if pd.notnull(x) else "")
            
        # Save to a new CSV file
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"✅ Archivo con porcentajes exportado exitosamente a '{output_file}'")
    except Exception as e:
        print(f"❌ Ocurrió un error al procesar el archivo: {e}")

if __name__ == "__main__":
    generar_csv_porcentajes()