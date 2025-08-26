# quick_start_spyder.py
"""
Script de inicio rápido para Spyder
Ejecutar este archivo para probar el sistema
"""

import sys
import os
from pathlib import Path
import pandas as pd

# Configurar entorno
project_root = Path(__file__).parent
os.chdir(str(project_root))
sys.path.insert(0, str(project_root))

print("🚀 INICIO RÁPIDO - SISTEMA DINÁMICO")
print("=" * 40)

# Importar sistema
try:
    from core.field_detector import create_detector, test_field_detector
    from core.field_mapper import create_field_mapper, test_field_mapper
    from core.dynamic_field_loader import create_field_loader, test_field_loader
    
    print("✓ Módulos importados correctamente")
    
    # Crear detector
    detector = create_detector()
    print("✓ Detector creado")
    
    # Cargar datos de ejemplo
    df = pd.read_csv('data/ejemplo_contaplus.csv')
    print(f"✓ Datos cargados: {df.shape}")
    
    # Realizar detección
    summary = detector.get_detection_summary(df)
    print(f"✓ Detección completada: {summary['detection_rate_percent']:.1f}%")
    
    # Mostrar resultados
    print("\n📊 RESULTADOS:")
    for field_type, column_name in summary['detected_fields'].items():
        print(f"  ✓ {field_type}: {column_name}")
    
    print("\n💡 VARIABLES DISPONIBLES:")
    print("  - detector: Detector principal")
    print("  - df: DataFrame de ejemplo")
    print("  - summary: Resumen de detección")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\n🔧 CONFIGURACIÓN BÁSICA:")
    print("  1. Ejecutar: python spyder_setup.py")
    print("  2. Verificar que todos los archivos están en su lugar")
    print("  3. Instalar dependencias faltantes")
