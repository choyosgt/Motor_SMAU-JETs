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
    value = value.replace(".", "").replace(",", ".")
    try:
        num = float(value)
        return -num if is_negative else num
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
    Devuelve el nombre original de la columna o None.
    """
    normalized_cols = {normalize_text(c): c for c in df.columns}
    for norm_name, orig_name in normalized_cols.items():
        if all(k in norm_name for k in keywords):
            return orig_name
    return None

def detectar_fila_cabecera(input_file: str) -> int:
    """
    Escanea la primera hoja para encontrar la fila que contiene columnas tipo 'CUENTA' y 'SALDO'.
    Devuelve el índice de fila que se debe usar como header.
    """
    preview = pd.read_excel(input_file, None)
    first_sheet = next(iter(preview))
    df_preview = pd.read_excel(input_file, sheet_name=first_sheet, header=None)
    for i, row in df_preview.iterrows():
        normalized = row.astype(str).apply(normalize_text).tolist()
        if any("CUENTA" in c for c in normalized) and any("SALDO" in c for c in normalized):
            return i
    raise ValueError("No se encontró la fila de cabecera con columnas tipo 'CUENTA' y 'SALDO'")

def clean_account_number(value):
    """
    Convierte un valor de cuenta a string sin decimales.
    - Si es float y es entero, lo convierte a int antes de string (evita '185.0' -> '185').
    - Si ya es string, lo limpia de espacios.
    - Si es NaN devuelve cadena vacía "".
    """
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    val_str = str(value).strip()
    if re.fullmatch(r"\d+\.0+", val_str):
        return str(int(float(val_str)))
    return val_str

def transformar_excel_a_csv(input_file: str, output_file: str):
    # Detectar fila de cabecera
    header_row = detectar_fila_cabecera(input_file)

    # Leer Excel usando esa fila como encabezados
    df = pd.read_excel(input_file, header=header_row)

    # Detectar columnas que contengan "CTA" o "CUENTA", excluyendo las que tengan "#"
    account_cols = [
        c for c in df.columns
        if any(k in normalize_text(c) for k in ["CUENTA", "CTA"])
        and "#" not in normalize_text(c)
    ]

    if len(account_cols) < 3:
        raise ValueError("No se encontraron suficientes columnas de cuenta (mínimo 3).")

    # Aplicar la lógica estricta
    if len(account_cols) == 3:
        gl_account_number_col = account_cols[2]
        gl_local_account_number_col = None
    else:
        gl_account_number_col = account_cols[-2]
        gl_local_account_number_col = account_cols[-1]

    # Columna de saldo final
    col_ending_balance = find_column(df, ["SALDO", "FINAL"])
    if not col_ending_balance:
        raise ValueError("No se encontró la columna de saldo final (SALDO FINAL).")

    # Filtrar columnas de saldo histórico (SALDO 31/12/20XX)
    saldo_cols = [c for c in df.columns if re.match(r"SALDO 31/12/20\d{2,4}", normalize_text(c))]
    oldest_col = None
    if saldo_cols:
        years = [(c, int(re.search(r"(20\d{2})", c).group(1))) for c in saldo_cols if re.search(r"(20\d{2})", c)]
        if years:
            oldest_col = min(years, key=lambda x: x[1])[0]

    # Si no hay oldest_col, buscar SALDO INICIAL
    beginning_col = oldest_col or find_column(df, ["SALDO", "INICIAL"])

    # Construir DataFrame resultado
    result = pd.DataFrame()
    result["gl_account_number"] = df[gl_account_number_col].apply(clean_account_number)
    if gl_local_account_number_col:
        result["gl_local_account_number"] = df[gl_local_account_number_col].apply(clean_account_number)
    result["period_ending_balance"] = df[col_ending_balance].apply(clean_number)

    if beginning_col:
        result["period_beginning_balance"] = df[beginning_col].apply(clean_number)
    else:
        result["period_beginning_balance"] = pd.Series([None] * len(result), index=result.index)

    # Guardar en CSV
    result.to_csv(output_file, index=False, sep=",", float_format="%.2f")
    print(f"Archivo CSV generado en: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transformar Excel de saldos a CSV limpio.")
    parser.add_argument("input_file", help="Ruta del archivo Excel de entrada.")
    parser.add_argument("output_file", help="Ruta del archivo CSV de salida.")
    args = parser.parse_args()
    transformar_excel_a_csv(args.input_file, args.output_file)
