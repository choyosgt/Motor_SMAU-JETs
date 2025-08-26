# analyze_csv_corrected.py
"""
Versión corregida de analyze_csv_mappings
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, '..')

try:
    from core.field_detector import FieldDetector
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False


def analyze_csv_mappings_corrected(file_path: str, erp_hint: str = None, show_stats: bool = True):
    """
    Analiza un CSV y muestra los mapeos de campos de forma clara - VERSIÓN CORREGIDA
    """
    if not CORE_AVAILABLE:
        print("❌ Core modules not available")
        return None
    
    # Verificar que el archivo existe
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        return None
    
    try:
        # Cargar CSV
        df = pd.read_csv(file_path)
        print(f"📄 ANALYZING: {Path(file_path).name}")
        print("=" * 60)
        
        # Crear detector y mapper
        detector = FieldDetector()
        mapper = detector.field_mapper
        
        # Auto-detectar ERP si no se especifica
        if not erp_hint:
            erp_hint = detector.auto_detect_erp(df)
        
        print(f"📊 File info:")
        print(f"  • Rows: {len(df)}")
        print(f"  • Columns: {len(df.columns)}")
        print(f"  • ERP detected: {erp_hint}")
        print()
        
        # Analizar cada columna
        field_mappings = {}
        unmapped_columns = []
        
        for column in df.columns:
            mapping_result = mapper.find_field_mapping(column, erp_hint)
            
            if mapping_result:
                field_type, confidence = mapping_result
                field_def = mapper.field_loader.get_field_definition(field_type)
                field_name = field_def.name if field_def else field_type
                
                field_mappings[column] = {
                    'field_type': field_type,
                    'field_name': field_name,
                    'confidence': confidence
                }
            else:
                unmapped_columns.append(column)
        
        # Mostrar resultados
        print("🎯 FIELD MAPPINGS:")
        print("-" * 20)
        
        # Mapeos exitosos (ordenados por confianza)
        sorted_mappings = sorted(
            field_mappings.items(), 
            key=lambda x: x[1]['confidence'], 
            reverse=True
        )
        
        for column, mapping_info in sorted_mappings:
            confidence = mapping_info['confidence']
            field_type = mapping_info['field_type']
            field_name = mapping_info['field_name']
            
            print(f"  ✓ {column} -> {field_type} (confidence: {confidence:.3f})")
            if show_stats:
                print(f"    └─ Field: {field_name}")
        
        # Columnas sin mapeo
        if unmapped_columns:
            print()
            for column in unmapped_columns:
                print(f"  ❌ {column} -> No mapping found")
        
        # Estadísticas generales - CORREGIDO
        total_columns = len(df.columns)
        mapped_columns = len(field_mappings)
        mapping_rate = (mapped_columns / total_columns * 100) if total_columns > 0 else 0
        
        if show_stats:
            print()
            print("📈 STATISTICS:")
            print("-" * 15)
            print(f"  • Total columns: {total_columns}")
            print(f"  • Mapped columns: {mapped_columns}")
            print(f"  • Unmapped columns: {len(unmapped_columns)}")
            print(f"  • Mapping rate: {mapping_rate:.1f}%")
            
            if field_mappings:
                avg_confidence = sum(m['confidence'] for m in field_mappings.values()) / len(field_mappings)
                print(f"  • Average confidence: {avg_confidence:.3f}")
                
                # Top field types detected
                field_types = [m['field_type'] for m in field_mappings.values()]
                unique_types = len(set(field_types))
                print(f"  • Unique field types: {unique_types}")
            else:
                avg_confidence = 0.0
        else:
            avg_confidence = sum(m['confidence'] for m in field_mappings.values()) / len(field_mappings) if field_mappings else 0.0
        
        return {
            'file_path': file_path,
            'erp_detected': erp_hint,
            'total_columns': total_columns,
            'mapped_columns': mapped_columns,
            'mapping_rate': mapping_rate,
            'field_mappings': field_mappings,
            'unmapped_columns': unmapped_columns,
            'average_confidence': avg_confidence
        }
        
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_corrected_function():
    """Test de la función corregida"""
    print("🧪 PROBANDO FUNCIÓN CORREGIDA")
    print("=" * 35)
    
    # Crear datos de test
    import pandas as pd
    test_data = pd.DataFrame({
        'TestCol1': ['A', 'B', 'C'],
        'TestCol2': [1, 2, 3],
        'TestCol3': ['X', 'Y', 'Z']
    })
    
    test_file = Path("test_corrected.csv")
    test_data.to_csv(test_file, index=False)
    
    # Probar función corregida
    result = analyze_csv_mappings_corrected(str(test_file), "TestERP", show_stats=True)
    
    if result:
        print("✅ Función corregida funciona correctamente")
        mapping_rate = result['mapping_rate']
        print(f"📊 Resultado: {mapping_rate:.1f}% mapping rate")
    else:
        print("❌ Función corregida falló")
    
    # Limpiar
    if test_file.exists():
        test_file.unlink()
    
    return result is not None

if __name__ == "__main__":
    test_corrected_function()
