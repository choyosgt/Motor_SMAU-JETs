# yaml_config_debugger.py
"""
Debug tool para verificar la carga del YAML de configuración
Identifica por qué no se cargan los sinónimos
"""

import yaml
import json
from pathlib import Path
from typing import Dict, Any

def debug_yaml_config(yaml_path: str = "config/dynamic_fields_config.yaml"):
    """Debuggea la configuración YAML paso a paso"""
    print("🔍 DEBUGGING YAML CONFIG")
    print("=" * 50)
    
    yaml_path = Path(yaml_path)
    
    # 1. Verificar que el archivo existe
    print(f"📁 Verificando archivo: {yaml_path}")
    if not yaml_path.exists():
        print(f"❌ ERROR: Archivo no encontrado: {yaml_path}")
        print(f"📍 Directorio actual: {Path.cwd()}")
        print(f"📍 Archivos en config/:")
        config_dir = Path("config")
        if config_dir.exists():
            for file in config_dir.iterdir():
                print(f"  • {file.name}")
        return False
    
    print(f"✅ Archivo encontrado: {yaml_path}")
    
    # 2. Verificar contenido del archivo
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"📄 Tamaño del archivo: {len(content)} caracteres")
        print(f"📄 Primeras 500 caracteres:")
        print("-" * 30)
        print(content[:500])
        print("-" * 30)
        
    except Exception as e:
        print(f"❌ ERROR leyendo archivo: {e}")
        return False
    
    # 3. Parsear YAML
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f)
        
        print(f"✅ YAML parseado correctamente")
        print(f"📊 Claves principales: {list(yaml_data.keys()) if yaml_data else 'None'}")
        
    except Exception as e:
        print(f"❌ ERROR parseando YAML: {e}")
        return False
    
    # 4. Verificar estructura de fields
    try:
        fields = yaml_data['field_definitions']['dynamic_fields']
        print(f"📊 Total de campos definidos: {len(fields)}")
    except KeyError:
        print(f"❌ ERROR: No se encontró 'field_definitions.dynamic_fields' en el YAML")
        return False
    



def debug_field_loader(config_path: str = "config/dynamic_fields_config.yaml"):
    """Debug del DynamicFieldLoader específicamente"""
    print(f"\n🔍 DEBUGGING DYNAMIC FIELD LOADER")
    print("=" * 50)
    
    try:
        # Importar el loader
        from core.dynamic_field_loader import DynamicFieldLoader
        
        # Crear instancia
        loader = DynamicFieldLoader(config_path)

        # Mostrar config cruda (debug profundo)
        print(f"✅ DynamicFieldLoader creado")
        print("🛠 Dump crudo del YAML cargado por el loader:")
        print(json.dumps(loader.get_raw_config(), indent=2, ensure_ascii=False))
        
        # Obtener definiciones
        definitions = loader.get_field_definitions()
        print(f"📊 Definiciones cargadas: {len(definitions)}")
        
        
        # Estadísticas del loader
        stats = loader.get_statistics()
        print(f"\n📊 Estadísticas del loader:")
        for key, value in stats.items():
            print(f"  • {key}: {value}")
        
        return loader
        
    except Exception as e:
        print(f"❌ ERROR en DynamicFieldLoader: {e}")
        import traceback
        traceback.print_exc()
        return None

def debug_field_mapper_integration():
    """Debug de la integración completa"""
    print(f"\n🔍 DEBUGGING FIELD MAPPER INTEGRATION")
    print("=" * 50)
    
    try:
        # Importar y crear field mapper
        from core.field_mapper import create_field_mapper
        
        mapper = create_field_mapper()
        print(f"✅ FieldMapper creado")
        

        
        # Validación del mapper
        validation = mapper.validate_mappings()
        print(f"\n📊 Validación del mapper:")
        print(f"  • Válido: {validation['valid']}")
        print(f"  • Errores: {len(validation['errors'])}")
        print(f"  • Advertencias: {len(validation['warnings'])}")
        
        if validation['warnings']:
            print(f"  ⚠️ Advertencias:")
            for warning in validation['warnings'][:5]:  # Primeras 5
                print(f"    - {warning}")
        
        return mapper
        
    except Exception as e:
        print(f"❌ ERROR en FieldMapper: {e}")
        import traceback
        traceback.print_exc()
        return None

def full_debug_session():
    """Sesión completa de debug"""
    print("🚀 FULL DEBUG SESSION - FIELD MAPPER")
    print("=" * 60)
    
    # 1. Debug YAML
    yaml_ok = debug_yaml_config()
    
    if not yaml_ok:
        print("❌ YAML config falló. No se puede continuar.")
        return False
    
    # 2. Debug Field Loader
    loader = debug_field_loader()
    
    if not loader:
        print("❌ Field Loader falló. No se puede continuar.")
        return False
    
    # 3. Debug Field Mapper
    mapper = debug_field_mapper_integration()
    
    if not mapper:
        print("❌ Field Mapper falló.")
        return False
    
    print(f"\n✅ DEBUG COMPLETADO")
    print("=" * 30)
    print("Si los sinónimos están en el YAML pero no se cargan,")
    print("revisa la estructura del YAML o el código del loader.")
    
    return True

if __name__ == "__main__":
    full_debug_session()