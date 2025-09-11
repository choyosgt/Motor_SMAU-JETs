import pandas as pd
import re
import argparse
import unicodedata

def clean_number(value):
    if pd.isna(value):
        return None

    value = str(value).strip()
    is_negative = False

    # Números negativos entre paréntesis
    if value.startswith("(") and value.endswith(")"):
        is_negative = True
        value = value[1:-1]

    # Quitar espacios y caracteres extraños
    value = value.replace(" ", "").replace("\u200b", "")

    # Detectar si hay separador de miles y decimal
    # Ejemplo: 1.234,56 o 1,234.56
    # Primero detectar qué símbolo aparece al final
    last_comma = value.rfind(",")
    last_dot = value.rfind(".")
    
    if last_comma > last_dot:
        # Coma decimal, punto como miles
        value = value.replace(".", "").replace(",", ".")
    elif last_dot > last_comma:
        # Punto decimal, coma como miles
        value = value.replace(",", "")
    else:
        # Solo un número, nada que reemplazar
        pass

    try:
        num = float(value)
        return -num if is_negative else num
    except ValueError:
        return None



def normalize_text(text: str) -> str:
    text = str(text).strip().upper()
    text = "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text

def find_column(df, keywords):
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
        if any("CUENTA" in c for c in normalized) and any("SALDO" in c for c in normalized):
            return i
    raise ValueError("No se encontró la fila de cabecera con columnas tipo 'CUENTA' y 'SALDO'")

def clean_account_number(value):
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    val_str = str(value).strip()
    if re.fullmatch(r"\d+\.0+", val_str):
        return str(int(float(val_str)))
    return val_str

# Columnas completas de staging.trial_balance en el orden correcto
TRIAL_BALANCE_COLUMNS = [
    'gl_account_number',
    'reporting_account',
    'fiscal_year',
    'period_number',
    'period_ending_balance',
    'period_activity_debit',
    'period_activity_credit',
    'period_beginning_balance',
    'period_ending_date',
    'business_unit',
    'cost_center',
    'department',
    'user_defined_01',
    'user_defined_02',
    'user_defined_03'
]

def _ensure_all_columns_trial_balance(df: pd.DataFrame, required_fields: list) -> pd.DataFrame:
    for col in required_fields:
        if col not in df.columns:
            df[col] = ""
    return df[required_fields]

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

    # Garantizar todas las columnas de staging
    result = _ensure_all_columns_trial_balance(result, TRIAL_BALANCE_COLUMNS)

    # Guardar en CSV
    result.to_csv(output_file, index=False, sep=",", float_format="%.2f")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transformar Excel de saldos a CSV limpio.")
    parser.add_argument("input_file", help="Ruta del archivo Excel de entrada.")
    parser.add_argument("output_file", help="Ruta del archivo CSV de salida.")
    args = parser.parse_args()
    transformar_excel_a_csv(args.input_file, args.output_file)
