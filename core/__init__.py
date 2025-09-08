# core/__init__.py
"""
Sistema Dinámico de Detección de Campos Contables
Módulo principal
"""

__version__ = "2.1.2"
__author__ = "Sistema Dinámico de Campos"

# Importaciones principales para facilitar uso en Spyder
try:
    from .dynamic_field_definition import DynamicFieldDefinition, SynonymData, ValidationRules, create_field_definition
    from .dynamic_field_loader import DynamicFieldLoader, LoaderStatus, create_field_loader
    from .field_mapper import FieldMapper, create_field_mapper
    from .field_detector import FieldDetector, create_detector
    from .csv_utils import analyze_csv_file

    # Exportar clases principales
    __all__ = [
        'DynamicFieldDefinition',
        'SynonymData', 
        'ValidationRules',
        'DynamicFieldLoader',
        'LoaderStatus',
        'FieldMapper',
        'FieldDetector',
        'create_detector',
        'create_field_loader',
        'create_field_mapper',
        'create_field_definition',
        'analyze_csv_file'
    ]
    
    print(f"✓ Core modules loaded successfully (v{__version__})")
    
except ImportError as e:
    print(f"⚠️ Warning: Some core modules could not be imported: {e}")
    print("This may be normal during initial setup.")
    
    # Fallback mínimo
    __all__ = []

# Función de conveniencia para inicio rápido en Spyder
def quick_start():
    """Inicio rápido para usar en Spyder"""
    print("🚀 SISTEMA DINÁMICO - INICIO RÁPIDO")
    print("=" * 40)
    
    try:
        # Crear detector
        detector = create_detector()
        print("✓ Detector creado")
        
        # Verificar estructura de directorios
        from pathlib import Path
        
        data_dir = Path("data")
        if not data_dir.exists():
            data_dir.mkdir(parents=True)
            print("✓ Directorio data creado")
        
        print("\n📚 Para comenzar, ejecuta:")
        print("   from core import create_detector")
        print("   detector = create_detector()")
        print("   # Luego carga tu CSV con pandas")
        
        return detector
        
    except Exception as e:
        print(f"❌ Error en quick_start: {e}")
        return None

# Función para testing completo
def run_complete_test():
    """Ejecuta una batería completa de tests"""
    print("🧪 EJECUTANDO TESTS COMPLETOS")
    print("=" * 35)
    
    try:
        # Test 1: Crear componentes
        print("\n1️⃣ Test de creación de componentes:")
        loader = create_field_loader()
        mapper = create_field_mapper()
        detector = create_detector()
        print("✓ Todos los componentes creados")
        
        # Test 2: Datos de prueba
        print("\n2️⃣ Test con datos de prueba:")
        import pandas as pd
        
        test_data = pd.DataFrame({
            'journal': ['JE001', 'JE002', 'JE003'],
            'asiento': [1, 2, 3],
            'debe': [100.0, 200.0, 300.0],
            'haber': [0.0, 0.0, 0.0],
            'cuenta_contable': ['4300001', '7000001', '5720001'],
            'fecha': ['01/01/2024', '02/01/2024', '03/01/2024']
        })
        
        # Test 3: Detección
        print("\n3️⃣ Test de detección:")
        result = detector.detect_fields(test_data)
        summary = detector.get_detection_summary(test_data)
        
        print(f"✓ ERP detectado: {result['erp_detected']}")
        print(f"✓ Tasa de detección: {summary['detection_rate_percent']:.1f}%")
        print(f"✓ Campos detectados: {len(summary['detected_fields'])}")
        
        # Mostrar campos detectados
        for field_type, column in summary['detected_fields'].items():
            print(f"   - {field_type}: {column}")
        
        print("\n✅ TODOS LOS TESTS COMPLETADOS EXITOSAMENTE")
        return True
        
    except Exception as e:
        print(f"❌ Error en tests: {e}")
        import traceback
        traceback.print_exc()
        return False
