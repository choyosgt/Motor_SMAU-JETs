import yaml
from collections import defaultdict

def cargar_sinonimos_desde_yaml_y_estaticos(ruta_yaml: str) -> dict:
    """
    Carga los sinónimos desde el archivo YAML y agrega manualmente una lista de sinónimos estáticos
    definidos directamente en el código.
    
    Retorna:
        dict[str, set[str]]: Diccionario campo -> conjunto de sinónimos
    """
    sinonimos_por_campo = defaultdict(set)

    # 1. Cargar desde YAML
    with open(ruta_yaml, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    campos = config.get('field_definitions', {}).get('dynamic_fields', {})
    for campo, datos in campos.items():
        if not datos.get('active', False):
            continue
        sinonimos = datos.get('synonyms', {})
        for erp, lista in sinonimos.items():
            for sin in lista:
                nombre = sin.get('name')
                if nombre:
                    sinonimos_por_campo[campo].add(nombre.lower().strip())

    # 2. Añadir manualmente los sinónimos estáticos
    sinonimos_estaticos = {
        "journal_entry_id": ["journal", "journalid", "journal_id", "je_id", "entry_id", "asiento", "id_asiento", "nro_asiento"],
        "line_number": ["line_number", "journal_line_number", "je_line_num", "line_id", "nro_linea", "linea_asiento", "num_linea"],
        "description": ["je_header_description", "header_desc", "entry_description", "journal_description", "descripcion_cabecera", "descripcion_asiento", "glosa_cabecera"],
        "line_description": ["line_desc", "je_line_description", "line_description", "transaction_detail", "descripcion_linea", "detalle_linea", "glosa_linea"],
        "effective_date": ["eff_date", "date", "transaction_date", "posting_date", "fecha", "fecha_valor", "fecha_transaccion", "fecha_contable"],
        "fiscal_year": ["year", "fiscalyear", "fy", "accounting_year", "ejercicio", "año_fiscal", "año_contable", "anio"],
        "period_number": ["accounting_period", "month", "period_num", "fiscal_period", "periodo", "mes", "periodo_fiscal"],
        "gl_account_number": ["gl_account", "account_number", "acct_num", "ledger_account", "gl_code", "cuenta_contable", "cuenta", "codigo_cuenta"],
        "amount": ["amount", "amt", "transaction_amount", "value", "importe", "monto", "valor"],
        "debit_amount": ["debit", "debit_amount", "dr", "amt_debit", "debe", "cargo", "monto_debe"],
        "credit_amount": ["credit", "credit_amount", "cr", "amt_credit", "haber", "abono", "monto_haber"],
        "debit_credit_indicator": ["drcr", "debit_credit", "indicator", "amount_type", "dc_indicator", "indicador_dh", "tipo_importe", "debe_haber"],
        "prepared_by": ["user", "entered_by", "created_by", "input_user", "ingresado_por", "usuario", "creado_por"],
        "entry_date": ["input_date", "entered_date", "created_date", "timestamp", "fecha_ingreso", "fecha_creacion", "fecha_registro"],
        "entry_time": ["time", "entered_time", "created_time", "input_time", "hora_ingreso", "hora_creacion", "hora_registro"],
        "gl_account_name": [],
        "vendor_id": [],
              }

    for campo, sinonimos in sinonimos_estaticos.items():
        for sin in sinonimos:
            sinonimos_por_campo[campo].add(sin.lower().strip())

    return dict(sinonimos_por_campo)


ruta_yaml = "config/dynamic_fields_config.yaml"
sinonimos = cargar_sinonimos_desde_yaml_y_estaticos(ruta_yaml)

# Mostrar resultado
for campo, sinonimos_set in sinonimos.items():
    print(f"{campo}: {sorted(sinonimos_set)}")
