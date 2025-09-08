import pandas as pd
import re
import argparse
import unicodedata

def clean_number(value):
    """
    Convierte un valor en formato europeo a float estándar en Python.
    - Elimina separadores de miles (punto).
    - Cambia coma decimal por punto.
    - Convierte valores entre paréntesis en negativos.
    """
    if pd.isna(value):
        return None
    value = str(value).strip()

    is_negative = False
    if value.startswith("(") and value.endswith(")"):
        is_negative = True
        value = value[1:-1]

    value = value.replace(".", "")
    value = value.replace(",", ".")

    try:
        num = float(value)
        if is_negative:
            num = -num
        return num
    except ValueError:
        return None

def normalize_text(text: str) -> str:
    """
    Normaliza un texto eliminando tildes, espacios extras y convirtiendo a mayúsculas.
    """
    text = str(text).strip().upper()
    text = "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text

def find_column(df, keywords):
    """
    Busca una columna en df que contenga todas las palabras clave dadas (ignorando mayúsculas, tildes y espacios).
    """
    normalized_cols = {normalize_text(c): c for c in df.columns}
    for norm_name, orig_name in normalized_cols.items():
        if all(k in norm_name for k in keywords):
            return orig_name
    return None

def detectar_fila_cabecera(input_file: str) -> int:
    preview = pd.read_excel(input_file, None)
    first_sheet = next(iter(preview))
    df_preview = pd.read_excel(input_file, sheet_name=first_sheet, header=None)

    for i, row in df_preview.iterrows():
        normalized = row.astype(str).apply(normalize_text).tolist()
        # Consideramos fila válida si tiene al menos dos columnas con palabras clave
        if any("CUENTA" in c for c in normalized) and any("SALDO" in c for c in normalized):
            return i

    raise ValueError("No se encontró la fila de cabecera con columnas tipo 'CUENTA' y 'SALDO'")

def transformar_excel_a_csv(input_file: str, output_file: str):
    """
    Lee un fichero Excel con columnas de saldos en formato europeo y genera un CSV limpio.

    Parámetros:
    - input_file: ruta del Excel de entrada.
    - output_file: ruta del CSV de salida.
    """
    # Detectar fila de cabecera
    header_row = detectar_fila_cabecera(input_file)

    # Leer Excel usando esa fila como encabezados
    df = pd.read_excel(input_file, header=header_row)

    # Buscar columnas de forma flexible
    col_gl_account = find_column(df, ["CUENTA", "IFRS"])
    col_ending_balance = find_column(df, ["SALDO", "FINAL"])

    if not col_gl_account or not col_ending_balance:
        print("Columnas detectadas en el Excel:")
        for col in df.columns:
            print(f"- '{col}'")
        raise ValueError("No se encontraron las columnas necesarias ('CUENTA IFRS' o 'SALDO FINAL DEFINITIVO').")

    # Filtrar columnas que sean del tipo SALDO 31/12/20XX
    saldo_cols = [c for c in df.columns if re.match(r"SALDO 31/12/20\\d{2,4}", normalize_text(c))]

    # Identificar la más antigua (menor año)
    oldest_col = None
    if saldo_cols:
        years = [(c, int(re.search(r"(20\\d{2})", c).group(1))) for c in saldo_cols if re.search(r"(20\\d{2})", c)]
        if years:
            oldest_col = min(years, key=lambda x: x[1])[0]

    # Crear DataFrame final
    result = pd.DataFrame()
    result["gl_account_number"] = df[col_gl_account]
    result["period_ending_balance"] = df[col_ending_balance].apply(clean_number)
    if oldest_col:
        result["period_beginning_balance"] = df[oldest_col].apply(clean_number)
    else:
        result["period_beginning_balance"] = None

    # Guardar en CSV
    result.to_csv(output_file, index=False, sep=",", float_format="%.2f")

    print(f"Archivo CSV generado en: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transformar Excel de saldos a CSV limpio.")
    parser.add_argument("input_file", help="Ruta del archivo Excel de entrada.")
    parser.add_argument("output_file", help="Ruta del archivo CSV de salida.")
    args = parser.parse_args()

    transformar_excel_a_csv(args.input_file, args.output_file)
