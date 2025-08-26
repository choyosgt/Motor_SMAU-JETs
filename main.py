# main_simple.py
"""
Main simplificado para mostrar mapeos de campos
Optimizado para mostrar resultados claros y concisos
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime

# Configurar entorno
def setup_environment():
    """Configura el entorno para ejecución"""
    project_root = Path(__file__).parent
    os.chdir(str(project_root))
    
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    return project_root

# Configurar entorno
setup_environment()

# Importar módulos del sistema
try:
    from core.field_detector import FieldDetector
    from core.field_mapper import FieldMapper
    CORE_AVAILABLE = True
except ImportError as e:
    print(f"❌ Error importing core modules: {e}")
    CORE_AVAILABLE = False

def analyze_csv_mappings(file_path: str, erp_hint: str = None, show_stats: bool = True):
    """
    Analiza un CSV y muestra los mapeos de campos de forma clara
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
        
        # Estadísticas generales
        if show_stats:
            print()
            print("📈 STATISTICS:")
            print("-" * 15)
            total_columns = len(df.columns)
            mapped_columns = len(field_mappings)
            mapping_rate = (mapped_columns / total_columns * 100) if total_columns > 0 else 0
            
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
        
        return {
            'file_path': file_path,
            'erp_detected': erp_hint,
            'total_columns': len(df.columns),
            'mapped_columns': len(field_mappings),
            'mapping_rate': mapping_rate,
            'field_mappings': field_mappings,
            'unmapped_columns': unmapped_columns,
            'average_confidence': avg_confidence if field_mappings else 0.0
        }
        
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        return None

def compare_multiple_files(file_paths: list, show_details: bool = False):
    """
    Compara mapeos entre múltiples archivos
    """
    print("🔍 COMPARING MULTIPLE FILES")
    print("=" * 40)
    
    results = []
    
    for file_path in file_paths:
        if Path(file_path).exists():
            print(f"\n📄 {Path(file_path).name}:")
            print("-" * 30)
            
            result = analyze_csv_mappings(file_path, show_stats=show_details)
            if result:
                results.append(result)
            
            # Resumen rápido si no se muestran detalles
            if not show_details and result:
                mapped = result['mapped_columns']
                total = result['total_columns']
                rate = result['mapping_rate']
                print(f"  Mapping rate: {rate:.1f}% ({mapped}/{total})")
        else:
            print(f"❌ File not found: {file_path}")
    
    # Resumen comparativo
    if len(results) > 1:
        print(f"\n📊 COMPARISON SUMMARY:")
        print("-" * 25)
        
        for result in results:
            file_name = Path(result['file_path']).name
            rate = result['mapping_rate']
            erp = result['erp_detected']
            avg_conf = result['average_confidence']
            
            print(f"  {file_name:20} | {rate:5.1f}% | ERP: {erp:10} | Avg conf: {avg_conf:.3f}")
        
        # Estadísticas globales
        total_files = len(results)
        avg_mapping_rate = sum(r['mapping_rate'] for r in results) / total_files
        avg_confidence = sum(r['average_confidence'] for r in results) / total_files
        
        print(f"\n  Overall avg mapping rate: {avg_mapping_rate:.1f}%")
        print(f"  Overall avg confidence: {avg_confidence:.3f}")
    
    return results

def test_with_sample_data():
    """
    Prueba el sistema con los datos de ejemplo disponibles
    """
    print("🧪 TESTING WITH SAMPLE DATA")
    print("=" * 35)
    
    # Buscar archivos de ejemplo
    data_dir = Path('data')
    sample_files = []
    
    if data_dir.exists():
        for pattern in ['sample_*.csv', 'ejemplo_*.csv']:
            sample_files.extend(data_dir.glob(pattern))
    
    if not sample_files:
        print("❌ No sample files found in data/ directory")
        print("💡 Run 'python spyder_setup.py' to create sample data")
        return None
    
    print(f"Found {len(sample_files)} sample files:")
    for file in sample_files:
        print(f"  • {file.name}")
    
    print("\n" + "="*60)
    
    # Analizar cada archivo de ejemplo
    results = []
    for sample_file in sample_files:
        print(f"\n{'='*60}")
        result = analyze_csv_mappings(str(sample_file), show_stats=True)
        if result:
            results.append(result)
        print("="*60)
    
    # Resumen final si hay múltiples archivos
    if len(results) > 1:
        print(f"\n🏆 FINAL COMPARISON:")
        print("=" * 25)
        
        best_file = max(results, key=lambda x: x['mapping_rate'])
        worst_file = min(results, key=lambda x: x['mapping_rate'])
        
        print(f"Best mapping: {Path(best_file['file_path']).name} ({best_file['mapping_rate']:.1f}%)")
        print(f"Worst mapping: {Path(worst_file['file_path']).name} ({worst_file['mapping_rate']:.1f}%)")
        
        # ERP distribution
        erp_counts = {}
        for result in results:
            erp = result['erp_detected']
            erp_counts[erp] = erp_counts.get(erp, 0) + 1
        
        print(f"\nERP distribution:")
        for erp, count in erp_counts.items():
            print(f"  {erp}: {count} files")
    
    return results

def main():
    """
    Función principal simplificada
    """
    print("🚀 SISTEMA DE MAPEO DE CAMPOS")
    print("=" * 40)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    if not CORE_AVAILABLE:
        print("❌ Core modules not available. Please check your installation.")
        return
    
    # Verificar argumentos de línea de comandos
    if len(sys.argv) > 1:
        # Modo: analizar archivos específicos
        file_paths = sys.argv[1:]
        
        if len(file_paths) == 1:
            # Analizar un solo archivo
            analyze_csv_mappings(file_paths[0], show_stats=True)
        else:
            # Comparar múltiples archivos
            compare_multiple_files(file_paths, show_details=False)
    
    else:
        # Modo: usar datos de ejemplo
        test_with_sample_data()

if __name__ == "__main__":
    main()