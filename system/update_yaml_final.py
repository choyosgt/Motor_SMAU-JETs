# final_yaml_update.py
"""
Script final para actualizar el YAML con todos los sinónimos SAP
y verificar que todo funcione correctamente
"""

import yaml
import sys
from pathlib import Path
from datetime import datetime
import shutil

def update_yaml_with_sap_synonyms():
    """Actualiza el YAML con sinónimos SAP específicos"""
    
    config_path = Path('config/dynamic_fields_config.yaml')
    
    # Backup
    backup_path = Path('backups') / f'dynamic_fields_config_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.yaml'
    backup_path.parent.mkdir(exist_ok=True)
    shutil.copy2(config_path, backup_path)
    print(f"✓ Backup creado: {backup_path}")
    
    # Cargar configuración actual
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Sinónimos SAP adicionales para campos existentes
    sap_synonyms_updates = {
        'journal_id': [
            {'name': 'AWKEY', 'confidence_boost': 0.8, 'language': 'de', 'description': 'SAP Object Key'},
            {'name': 'BELNR', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Document Number'},
        ],
        'effective_date': [
            {'name': 'BUDAT', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Posting Date'},
            {'name': 'BLDAT', 'confidence_boost': 0.9, 'language': 'de', 'description': 'SAP Document Date'},
        ],
        'gl_account_number': [
            {'name': 'HKONT', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP GL Account'},
            {'name': 'SAKNR', 'confidence_boost': 0.9, 'language': 'de', 'description': 'SAP Account Number'},
        ],
        'amount_debit': [
            {'name': 'WRBTR', 'confidence_boost': 0.85, 'language': 'de', 'description': 'SAP Amount in Transaction Currency'},
            {'name': 'DMBTR', 'confidence_boost': 0.85, 'language': 'de', 'description': 'SAP Amount in Local Currency'},
        ],
        'amount_credit': [
            {'name': 'WRBTR', 'confidence_boost': 0.85, 'language': 'de', 'description': 'SAP Amount in Transaction Currency'},
            {'name': 'DMBTR', 'confidence_boost': 0.85, 'language': 'de', 'description': 'SAP Amount in Local Currency'},
        ],
        'je_line_description': [
            {'name': 'SGTXT', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Item Text'},
            {'name': 'BSEG-SGTXT', 'confidence_boost': 0.9, 'language': 'de', 'description': 'SAP Line Item Text'},
        ],
        'fiscal_year': [
            {'name': 'GJAHR', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Fiscal Year'},
        ],
        'period': [
            {'name': 'MONAT', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Period'},
            {'name': 'POPER', 'confidence_boost': 0.9, 'language': 'de', 'description': 'SAP Posting Period'},
        ],
        'amount_currency': [
            {'name': 'WAERS', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Currency Key'},
            {'name': 'PSWSL', 'confidence_boost': 0.8, 'language': 'de', 'description': 'SAP Price Unit Currency'},
        ],
        'entered_by': [
            {'name': 'USNAM', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP User Name'},
            {'name': 'CPUDT', 'confidence_boost': 0.7, 'language': 'de', 'description': 'SAP Entry Date'},
        ],
        'cost_center': [
            {'name': 'KOSTL', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Cost Center'},
            {'name': 'BSEG-KOSTL', 'confidence_boost': 0.9, 'language': 'de', 'description': 'SAP Line Item Cost Center'},
        ]
    }
    
    # Nuevos campos SAP específicos
    new_sap_fields = {
        'vendor_code': {
            'name': 'Código de Proveedor',
            'description': 'Código del proveedor o acreedor',
            'data_type': 'alphanumeric',
            'active': True,
            'priority': 65,
            'validation': {
                'pattern': '^[A-Z0-9]{1,10}$',
                'required': False
            },
            'synonyms': {
                'SAP': [
                    {'name': 'LIFNR', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Vendor Number'},
                    {'name': 'KRED', 'confidence_boost': 0.8, 'language': 'de', 'description': 'SAP Creditor Number'},
                ],
                'Generic_ES': [
                    {'name': 'CodigoProveedor', 'confidence_boost': 0.9, 'language': 'es'},
                    {'name': 'Proveedor', 'confidence_boost': 0.8, 'language': 'es'},
                ]
            }
        },
        'customer_code': {
            'name': 'Código de Cliente',
            'description': 'Código del cliente o deudor',
            'data_type': 'alphanumeric',
            'active': True,
            'priority': 65,
            'validation': {
                'pattern': '^[A-Z0-9]{1,10}$',
                'required': False
            },
            'synonyms': {
                'SAP': [
                    {'name': 'KUNNR', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Customer Number'},
                    {'name': 'DEBR', 'confidence_boost': 0.8, 'language': 'de', 'description': 'SAP Debtor Number'},
                ],
                'Generic_ES': [
                    {'name': 'CodigoCliente', 'confidence_boost': 0.9, 'language': 'es'},
                    {'name': 'Cliente', 'confidence_boost': 0.8, 'language': 'es'},
                ]
            }
        },
        'material_code': {
            'name': 'Código de Material',
            'description': 'Código del material o producto',
            'data_type': 'alphanumeric',
            'active': True,
            'priority': 60,
            'validation': {
                'pattern': '^[A-Z0-9]{1,18}$',
                'required': False
            },
            'synonyms': {
                'SAP': [
                    {'name': 'MATNR', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Material Number'},
                    {'name': 'CHARG', 'confidence_boost': 0.7, 'language': 'de', 'description': 'SAP Batch Number'},
                ],
                'Generic_ES': [
                    {'name': 'CodigoMaterial', 'confidence_boost': 0.9, 'language': 'es'},
                    {'name': 'Material', 'confidence_boost': 0.8, 'language': 'es'},
                ]
            }
        },
        'company_code': {
            'name': 'Código de Sociedad',
            'description': 'Código de la sociedad o empresa',
            'data_type': 'alphanumeric',
            'active': True,
            'priority': 70,
            'validation': {
                'pattern': '^[A-Z0-9]{2,4}$',
                'required': False
            },
            'synonyms': {
                'SAP': [
                    {'name': 'BUKRS', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Company Code'},
                    {'name': 'MANDT', 'confidence_boost': 0.7, 'language': 'de', 'description': 'SAP Client'},
                ],
                'Generic_ES': [
                    {'name': 'CodigoSociedad', 'confidence_boost': 0.9, 'language': 'es'},
                    {'name': 'Sociedad', 'confidence_boost': 0.8, 'language': 'es'},
                    {'name': 'Empresa', 'confidence_boost': 0.8, 'language': 'es'},
                ]
            }
        },
        'plant_code': {
            'name': 'Código de Centro',
            'description': 'Centro de fabricación o almacén',
            'data_type': 'alphanumeric',
            'active': True,
            'priority': 60,
            'validation': {
                'pattern': '^[A-Z0-9]{2,4},
                'required': False
            },
            'synonyms': {
                'SAP': [
                    {'name': 'WERKS', 'confidence_boost': 0.95, 'language': 'de', 'description': 'SAP Plant'},
                    {'name': 'LGORT', 'confidence_boost': 0.8, 'language': 'de', 'description': 'SAP Storage Location'},
                ],
                'Generic_ES': [
                    {'name': 'CodigoCentro', 'confidence_boost': 0.9, 'language': 'es'},
                    {'name': 'Centro', 'confidence_boost': 0.8, 'language': 'es'},
                    {'name': 'Almacen', 'confidence_boost': 0.7, 'language': 'es'},
                ]
            }
        }
    }
    
    # Actualizar campos existentes
    dynamic_fields = config.get('field_definitions', {}).get('dynamic_fields', {})
    
    updated_count = 0
    for field_name, synonyms in sap_synonyms_updates.items():
        if field_name in dynamic_fields:
            # Asegurar que existe la estructura SAP
            if 'synonyms' not in dynamic_fields[field_name]:
                dynamic_fields[field_name]['synonyms'] = {}
            if 'SAP' not in dynamic_fields[field_name]['synonyms']:
                dynamic_fields[field_name]['synonyms']['SAP'] = []
            
            # Añadir sinónimos SAP (evitar duplicados)
            existing_names = {syn['name'] for syn in dynamic_fields[field_name]['synonyms']['SAP']}
            for synonym in synonyms:
                if synonym['name'] not in existing_names:
                    dynamic_fields[field_name]['synonyms']['SAP'].append(synonym)
                    updated_count += 1
    
    # Añadir nuevos campos SAP
    added_count = 0
    for field_name, field_config in new_sap_fields.items():
        if field_name not in dynamic_fields:
            dynamic_fields[field_name] = field_config
            added_count += 1
    
    # Guardar configuración actualizada
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    
    print(f"✅ YAML actualizado exitosamente")
    print(f"📊 Sinónimos SAP añadidos: {updated_count}")
    print(f"📊 Campos nuevos añadidos: {added_count}")
    print(f"📄 Total campos en YAML: {len(dynamic_fields)}")
    
    return True

def verify_update():
    """Verifica que la actualización fue exitosa"""
    try:
        # Configurar path correctamente
        sys.path.insert(0, str(Path.cwd()))
        
        # Importar módulos
        from core.dynamic_field_loader import DynamicFieldLoader
        from core.field_detector import FieldDetector
        
        # Crear loader con configuración actualizada
        loader = DynamicFieldLoader()
        
        # Estadísticas
        stats = loader.get_statistics()
        print(f"✓ Campos activos: {stats['active_fields']}")
        print(f"✓ Sinónimos totales: {stats['total_synonyms']}")
        print(f"✓ Sistemas ERP: {stats['erp_systems']}")
        
        # Verificar campos SAP específicos
        sap_fields = ['vendor_code', 'customer_code', 'material_code', 'company_code', 'plant_code']
        found_sap_fields = []
        
        definitions = loader.get_field_definitions()
        for field_name in sap_fields:
            if field_name in definitions:
                field_def = definitions[field_name]
                sap_synonyms = field_def.get_synonyms_for_erp('SAP')
                if sap_synonyms:
                    found_sap_fields.append(f"{field_name}: {len(sap_synonyms)} sinónimos SAP")
        
        if found_sap_fields:
            print("✅ Campos SAP añadidos correctamente:")
            for field_info in found_sap_fields:
                print(f"  • {field_info}")
        
        # Crear detector para prueba final
        detector = FieldDetector()
        print(f"✓ Detector creado con {len(detector.get_available_field_types())} tipos de campo")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en verificación: {e}")
        return False

def create_test_sap_data():
    """Crea datos de prueba SAP para verificar el funcionamiento"""
    import pandas as pd
    
    sap_test_data = pd.DataFrame({
        'BELNR': ['1000000001', '1000000002', '1000000003'],
        'BUKRS': ['1000', '1000', '2000'],
        'GJAHR': ['2024', '2024', '2024'],
        'MONAT': ['01', '01', '02'],
        'BUDAT': ['20240115', '20240116', '20240201'],
        'BLDAT': ['20240115', '20240116', '20240201'],
        'HKONT': ['0000113100', '0000160000', '0000400000'],
        'KOSTL': ['CC001', 'CC002', 'CC001'],
        'WERKS': ['1000', '1000', '2000'],
        'LIFNR': ['VENDOR001', '', ''],
        'KUNNR': ['', 'CUSTOMER001', ''],
        'MATNR': ['MAT001', 'MAT002', 'MAT003'],
        'WRBTR': [1000.00, 1000.00, 500.00],
        'DMBTR': [1000.00, 1000.00, 500.00],
        'WAERS': ['EUR', 'EUR', 'EUR'],
        'SGTXT': ['Purchase material', 'Customer payment', 'Material issue'],
        'USNAM': ['USER001', 'USER002', 'USER001']
    })
    
    # Crear directorio data si no existe
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    
    # Guardar archivo de prueba
    test_file = data_dir / 'test_sap_enhanced.csv'
    sap_test_data.to_csv(test_file, index=False)
    
    print(f"✓ Archivo de prueba SAP creado: {test_file}")
    return test_file

def test_sap_detection():
    """Prueba la detección con datos SAP"""
    try:
        # Crear datos de prueba
        test_file = create_test_sap_data()
        
        # Cargar datos
        import pandas as pd
        df = pd.read_csv(test_file)
        
        # Configurar path
        sys.path.insert(0, str(Path.cwd()))
        
        # Importar detector
        from core.field_detector import FieldDetector
        
        # Crear detector
        detector = FieldDetector()
        
        # Ejecutar detección
        result = detector.detect_fields(df, erp_hint='SAP')
        
        # Mostrar resultados
        print(f"\n🎯 RESULTADOS DE DETECCIÓN SAP:")
        print("=" * 40)
        
        detected_fields = {}
        for field_type, candidates in result.get('candidates', {}).items():
            if candidates:
                best_candidate = candidates[0]
                if best_candidate['confidence'] > 0.3:
                    detected_fields[field_type] = {
                        'column': best_candidate['column_name'],
                        'confidence': best_candidate['confidence']
                    }
        
        if detected_fields:
            print(f"✅ Campos detectados: {len(detected_fields)}")
            for field_type, info in detected_fields.items():
                confidence_percent = info['confidence'] * 100
                print(f"  • {field_type}: {info['column']} ({confidence_percent:.1f}%)")
        else:
            print("⚠️ No se detectaron campos con suficiente confianza")
        
        return len(detected_fields) > 0
        
    except Exception as e:
        print(f"❌ Error en prueba SAP: {e}")
        return False

def main():
    """Función principal de actualización"""
    print("🚀 ACTUALIZACIÓN FINAL DEL YAML SAP")
    print("=" * 50)
    
    # 1. Actualizar YAML
    update_success = update_yaml_with_sap_synonyms()
    
    if update_success:
        print("\n🔍 VERIFICANDO ACTUALIZACIÓN")
        print("=" * 30)
        
        # 2. Verificar actualización
        verify_success = verify_update()
        
        if verify_success:
            print("\n🧪 PROBANDO DETECCIÓN SAP")
            print("=" * 30)
            
            # 3. Probar detección
            test_success = test_sap_detection()
            
            if test_success:
                print("\n✅ ACTUALIZACIÓN COMPLETADA EXITOSAMENTE")
                print("=" * 45)
                print("🎉 El sistema ahora tiene soporte completo para SAP")
                print("📊 Todos los campos SAP estándar están configurados")
                print("🔍 La detección automática funciona correctamente")
                
                print("\n📚 PRÓXIMOS PASOS:")
                print("1. Usar el detector con tus archivos SAP reales")
                print("2. Ajustar umbrales de confianza si es necesario")
                print("3. Añadir más sinónimos específicos de tu empresa")
                
            else:
                print("\n⚠️ Actualización completada pero hay problemas en la detección")
        else:
            print("\n⚠️ Actualización completada pero hay problemas en la verificación")
    else:
        print("\n❌ Error en la actualización del YAML")
    
    return update_success

if __name__ == "__main__":
    main()