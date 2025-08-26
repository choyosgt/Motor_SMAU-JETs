import pandas as pd
from pathlib import Path
from .field_detector import FieldDetector

def analyze_csv_file(file_path: str, detector: FieldDetector, erp_hint: str = None):
    """
    Analiza un archivo CSV usando el detector de campos
    """
    if not Path(file_path).is_file():
        print(f"❌ Archivo no encontrado: {file_path}")
        return None
    
    try:
        df = pd.read_csv(file_path)
        print(f"✓ Archivo cargado: {df.shape}")
    except Exception as e:
        print(f"❌ Error cargando CSV: {e}")
        return None
    
    try:
        result = detector.detect_fields(df, erp_hint=erp_hint)
        summary = detector.get_detection_summary(df, erp_hint=erp_hint)
        
        # Combinar resultados
        result['summary'] = summary
        result['file_path'] = file_path
        result['detected_columns'] = len(summary['detected_fields'])
        result['detection_rate_percent'] = summary['detection_rate_percent']
        
        return result
    except Exception as e:
        print(f"❌ Error analizando CSV: {e}")
        return None
