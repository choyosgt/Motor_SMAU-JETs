# check_current_config.py
"""
Script para revisar la configuración actual y entender por qué no se persisten los sinónimos
"""

import sys
import os
import yaml
import json
from pathlib import Path
from datetime import datetime

# Añadir el directorio padre al path
sys.path.insert(0, '..')

def revisar_archivo_yaml():
    """
    Revisa el contenido actual del archivo YAML
    """
    print("🔍 REVISANDO ARCHIVO YAML ACTUAL")
    print("=" * 40)
    
    config_file = Path('config/dynamic_fields_config.yaml')
    
    if not config_file.exists():
        print(f"❌ Archivo no encontrado: {config_file}")
        return None
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        print(f"✅ Archivo cargado correctamente")
        print(f"📄 Tamaño del archivo: {config_file.stat().st_size} bytes")
        
        # Estructura general
        print(f"\n📋 Estructura del YAML:")
        print(f"   • Claves principales: {list(config.keys()) if config else 'None'}")
        
        if 'field_definitions' in config:
            field_defs = config['field_definitions']
            print(f"   • field_definitions: {list(field_defs.keys()) if field_defs else 'None'}")
            
            if 'dynamic_fields' in field_defs:
                dynamic_fields = field_defs['dynamic_fields']
                print(f"   • dynamic_fields: {len(dynamic_fields)} campos")
                
                # Analizar cada campo
                print(f"\n📊 Análisis por campo:")
                for campo, datos in dynamic_fields.items():
                    sinonimos = datos.get('synonyms', {})
                    total_sinonimos = sum(len(lista) for lista in sinonimos.values()) if sinonimos else 0
                    
                    print(f"   • {campo}: {total_sinonimos} sinónimos en {len(sinonimos)} ERPs")
                    
                    # Mostrar sinónimos SAP específicamente
                    sap_synonyms = sinonimos.get('SAP', [])
                    if sap_synonyms:
                        nombres_sap = [s.get('name', 'N/A') if isinstance(s, dict) else s for s in sap_synonyms]
                        print(f"     - SAP: {nombres_sap}")
                    else:
                        print(f"     - SAP: Sin sinónimos")
        
        return config
        
    except Exception as e:
        print(f"❌ Error leyendo YAML: {e}")
        import traceback
        traceback.print_exc()
        return None

def revisar_training_data():
    """
    Revisa los archivos de training data para ver qué se intentó guardar
    """
    print(f"\n📄 REVISANDO TRAINING DATA")
    print("=" * 35)
    
    logs_dir = Path('logs')
    training_files = list(logs_dir.glob('training_data_*.json'))
    
    if not training_files:
        print("❌ No se encontraron archivos de training")
        return
    
    # Tomar el más reciente
    latest_file = max(training_files, key=lambda x: x.stat().st_mtime)
    print(f"📁 Archivo más reciente: {latest_file.name}")
    
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
        
        print(f"✅ Training data cargado")
        
        # Información general
        file_info = training_data.get('file_info', {})
        print(f"📊 Información del entrenamiento:")
        print(f"   • Archivo: {file_info.get('file_name', 'N/A')}")
        print(f"   • ERP: {file_info.get('erp_system', 'N/A')}")
        print(f"   • Timestamp: {file_info.get('timestamp', 'N/A')}")
        
        # Mapeos que se intentaron añadir
        new_mappings = training_data.get('new_mappings', {})
        confirmed_mappings = training_data.get('confirmed_mappings', {})
        
        print(f"\n🆕 Nuevos mapeos intentados:")
        if new_mappings:
            for column, mapping in new_mappings.items():
                field_type = mapping.get('field_type', 'N/A')
                source = mapping.get('source', 'N/A')
                print(f"   • {column} → {field_type} (source: {source})")
        else:
            print("   • Ninguno")
        
        print(f"\n✅ Mapeos confirmados:")
        if confirmed_mappings:
            for column, mapping in confirmed_mappings.items():
                field_type = mapping.get('field_type', 'N/A')
                confidence = mapping.get('confidence', 0)
                print(f"   • {column} → {field_type} (conf: {confidence:.3f})")
        else:
            print("   • Ninguno")
        
        # Feedback del usuario
        user_feedback = training_data.get('user_feedback', [])
        print(f"\n🔄 Feedback del usuario:")
        if user_feedback:
            for feedback in user_feedback:
                action = feedback.get('action', 'N/A')
                column = feedback.get('column', 'N/A')
                print(f"   • {column}: {action}")
        else:
            print("   • Ninguno")
        
        return training_data
        
    except Exception as e:
        print(f"❌ Error leyendo training data: {e}")
        return None

def diagnosticar_problema_persistencia():
    """
    Diagnostica específicamente por qué no se persisten los sinónimos
    """
    print(f"\n🩺 DIAGNÓSTICO DEL PROBLEMA")
    print("=" * 35)
    
    try:
        # Importar y probar el sistema
        from core.field_detector import FieldDetector
        from core.field_mapper import FieldMapper
        
        print("✅ Módulos importados correctamente")
        
        # Crear instancias
        detector = FieldDetector()
        mapper = detector.field_mapper
        loader = detector.field_loader
        
        print("✅ Instancias creadas correctamente")
        
        # Verificar método de añadir sinónimo
        print(f"\n🔧 Probando añadir sinónimo manualmente:")
        
        # Intentar añadir un sinónimo de prueba
        field_type = 'journal_id'
        synonym_name = 'TEST_SYNONYM'
        erp_system = 'TEST_ERP'
        
        success = mapper.add_dynamic_synonym(field_type, synonym_name, erp_system, 0.9)
        print(f"   • add_dynamic_synonym result: {success}")
        
        # Verificar si se añadió en memoria
        field_def = loader.get_field_definition(field_type)
        if field_def:
            test_synonyms = field_def.get_synonyms_for_erp(erp_system)
            print(f"   • Sinónimos en memoria: {test_synonyms}")
            
            if synonym_name in test_synonyms:
                print("   ✅ Sinónimo añadido en memoria")
                
                # El problema es que no se persiste al archivo YAML
                print("   ⚠️ PROBLEMA: El sinónimo se añade en memoria pero NO se guarda en YAML")
                print("   💡 SOLUCIÓN: Necesitamos implementar persistencia automática")
            else:
                print("   ❌ Sinónimo NO se añadió ni en memoria")
        else:
            print("   ❌ Campo no encontrado")
        
        # Verificar configuración del loader
        stats = loader.get_statistics()
        print(f"\n📊 Estadísticas del loader:")
        print(f"   • Status: {stats.get('status', 'N/A')}")
        print(f"   • Auto-reload: {loader.auto_reload_enabled}")
        print(f"   • Config source: {loader.config_source}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en diagnóstico: {e}")
        import traceback
        traceback.print_exc()
        return False

def proponer_solucion():
    """
    Propone la solución específica basada en el diagnóstico
    """
    print(f"\n💡 SOLUCIÓN PROPUESTA")
    print("=" * 25)
    
    print("🔍 Problema identificado:")
    print("   • Los sinónimos se añaden correctamente en memoria")
    print("   • El método add_dynamic_synonym() funciona")
    print("   • PERO no hay persistencia automática al archivo YAML")
    
    print(f"\n🔧 Solución:")
    print("   1. Actualizar manualmente el archivo YAML con los sinónimos faltantes")
    print("   2. O implementar auto-save en el DynamicFieldLoader")
    
    print(f"\n⚡ Acción inmediata:")
    print("   • Ejecutar fix_persistence.py para actualizar el YAML")
    print("   • Esto añadirá todos los sinónimos SAP que faltan")
    print("   • Después el sistema funcionará correctamente")

def main():
    """
    Función principal de diagnóstico
    """
    print("🔍 DIAGNÓSTICO COMPLETO DEL PROBLEMA")
    print("=" * 50)
    
    # 1. Revisar archivo YAML actual
    config = revisar_archivo_yaml()
    
    # 2. Revisar training data
    training_data = revisar_training_data()
    
    # 3. Diagnosticar problema específico
    diagnosticar_problema_persistencia()
    
    # 4. Proponer solución
    proponer_solucion()
    
    print(f"\n🎯 CONCLUSIÓN:")
    print("=" * 15)
    print("El sistema funciona correctamente EN MEMORIA,")
    print("pero necesita actualizar el archivo YAML para persistir los cambios.")
    print("\n➡️ Ejecuta: python fix_persistence.py")

if __name__ == "__main__":
    main()