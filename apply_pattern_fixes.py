#!/usr/bin/env python3
# apply_pattern_fixes.py - Script automatizado para aplicar las correcciones

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

def create_backup(file_path):
    """Crea un respaldo del archivo original"""
    if os.path.exists(file_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{file_path}.backup_{timestamp}"
        shutil.copy2(file_path, backup_path)
        print(f"✅ Backup created: {backup_path}")
        return backup_path
    return None

def fix_field_mapper():
    """Corrige core/field_mapper.py"""
    file_path = "core/field_mapper.py"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    # Crear backup
    create_backup(file_path)
    
    # Leer contenido actual
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Añadir import de YAMLPatternManager
    import_pattern = r'from \.dynamic_field_definition import DynamicFieldDefinition'
    if 'YAMLPatternManager' not in content:
        replacement = '''from .dynamic_field_definition import DynamicFieldDefinition

# Import para gestión de patrones YAML
try:
    from fix_pattern_learning_persistence import YAMLPatternManager
except ImportError:
    print("⚠️ YAMLPatternManager not found. Please ensure fix_pattern_learning_persistence.py is available.")
    YAMLPatternManager = None'''
        
        content = re.sub(import_pattern, replacement, content)
        print("✅ Added YAMLPatternManager import")
    
    # 2. Reemplazar __init__ de ContentPatternLearner
    old_init_pattern = r'def __init__\(self, patterns_file: str = "config/learned_patterns\.json"\):\s*self\.patterns_file = patterns_file\s*self\.learned_patterns = self\._load_learned_patterns\(\)'
    
    new_init = '''def __init__(self, patterns_file: str = "config/pattern_learning_config.yaml"):
        self.patterns_file = patterns_file
        self.yaml_manager = YAMLPatternManager(patterns_file) if YAMLPatternManager else None
        self.learned_patterns = self._load_learned_patterns()'''
    
    if re.search(old_init_pattern, content, re.DOTALL):
        content = re.sub(old_init_pattern, new_init, content, flags=re.DOTALL)
        print("✅ Updated ContentPatternLearner.__init__")
    
    # 3. Reemplazar método _load_learned_patterns
    old_load_pattern = r'def _load_learned_patterns\(self\) -> Dict:.*?return \{[^}]*\}'
    
    new_load_method = '''def _load_learned_patterns(self) -> Dict:
        """Carga patrones aprendidos desde archivo YAML"""
        try:
            if self.yaml_manager:
                yaml_config = self.yaml_manager.config
                
                # Extraer patrones de la configuración YAML
                learned_data = {
                    "field_patterns": {},
                    "data_type_patterns": {},
                    "validation_stats": {},
                    "pattern_confidence": {}
                }
                
                # Convertir formato YAML a formato interno
                if 'learned_patterns' in yaml_config:
                    for field_type, field_config in yaml_config['learned_patterns'].items():
                        learned_data["field_patterns"][field_type] = {
                            "regex_patterns": [p.get('regex', '') for p in field_config.get('priority_patterns', []) if 'regex' in p],
                            "format_patterns": [],
                            "value_patterns": [],
                            "statistical_patterns": field_config.get('statistical_patterns', {})
                        }
                        
                        learned_data["pattern_confidence"][f"{field_type}_confidence"] = field_config.get('confidence_score', 0.5)
                
                # También cargar patrones de configuración predefinidos
                for field_type in ['journal_id', 'effective_date', 'amount', 'gl_account_number', 'amount_credit_debit_indicator']:
                    if 'priority_patterns' in yaml_config and field_type in yaml_config['priority_patterns']:
                        field_config = yaml_config['priority_patterns'][field_type]
                        
                        if field_type not in learned_data["field_patterns"]:
                            learned_data["field_patterns"][field_type] = {
                                "regex_patterns": [],
                                "format_patterns": [],
                                "value_patterns": [],
                                "statistical_patterns": {}
                            }
                        
                        # Añadir patrones predefinidos
                        for pattern_entry in field_config.get('priority_patterns', []):
                            if 'regex' in pattern_entry:
                                learned_data["field_patterns"][field_type]["regex_patterns"].append(pattern_entry['regex'])
                            elif 'values' in pattern_entry:
                                learned_data["field_patterns"][field_type]["value_patterns"].extend(pattern_entry['values'])
                
                return learned_data
                
        except Exception as e:
            logger.warning(f"Error loading learned patterns from YAML: {e}")
        
        # Fallback al método original si YAML no funciona
        try:
            if os.path.exists(self.patterns_file):
                with open(self.patterns_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading learned patterns: {e}")
        
        return {
            "field_patterns": {},
            "data_type_patterns": {},
            "validation_stats": {},
            "pattern_confidence": {}
        }'''
    
    if re.search(old_load_pattern, content, re.DOTALL):
        content = re.sub(old_load_pattern, new_load_method, content, flags=re.DOTALL)
        print("✅ Updated _load_learned_patterns method")
    
    # 4. Reemplazar método save_learned_patterns
    old_save_pattern = r'def save_learned_patterns\(self\):.*?except Exception as e:\s*logger\.error\(f"Error saving learned patterns: \{e\}"\)'
    
    new_save_method = '''def save_learned_patterns(self):
        """Guarda patrones aprendidos en archivo YAML"""
        try:
            if self.yaml_manager:
                # Actualizar YAML con patrones aprendidos
                for field_type, patterns in self.learned_patterns["field_patterns"].items():
                    if patterns and any(patterns.values()):  # Solo si hay patrones
                        pattern_data = {
                            'regex_patterns': patterns.get('regex_patterns', []),
                            'statistical_patterns': patterns.get('statistical_patterns', {})
                        }
                        self.yaml_manager.add_learned_pattern(field_type, pattern_data)
                
                # Guardar el archivo YAML actualizado
                self.yaml_manager.save_yaml_config()
                print(f"💾 Patterns saved to YAML: {self.patterns_file}")
            
            # También mantener backup en JSON para compatibilidad
            os.makedirs(os.path.dirname(self.patterns_file), exist_ok=True)
            json_file = self.patterns_file.replace('.yaml', '.json').replace('.yml', '.json')
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.learned_patterns, f, indent=2, ensure_ascii=False)
            
            # Crear backup con timestamp
            os.makedirs('results/learned_patterns', exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_backup = f"results/learned_patterns/patterns_{timestamp}.json"
            
            with open(json_backup, 'w', encoding='utf-8') as f:
                json.dump(self.learned_patterns, f, indent=2, ensure_ascii=False)
            
            print(f"💾 JSON backup saved: {json_backup}")
            
        except Exception as e:
            logger.error(f"Error saving learned patterns: {e}")'''
    
    if re.search(old_save_pattern, content, re.DOTALL):
        content = re.sub(old_save_pattern, new_save_method, content, flags=re.DOTALL)
        print("✅ Updated save_learned_patterns method")
    
    # Guardar archivo modificado
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ Successfully updated: {file_path}")
    return True

def fix_enhanced_trainer():
    """Corrige complete_enhanced_trainer.py"""
    file_path = "complete_enhanced_trainer.py"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    # Crear backup
    create_backup(file_path)
    
    # Leer contenido actual
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Añadir import al inicio del archivo
    if 'YAMLPatternManager' not in content:
        # Buscar la primera línea de import
        import_lines = re.findall(r'^(import|from) .*$', content, re.MULTILINE)
        if import_lines:
            first_import = import_lines[0]
            import_addition = '''# Import para gestión de patrones YAML
try:
    from fix_pattern_learning_persistence import YAMLPatternManager
except ImportError:
    print("⚠️ YAMLPatternManager not found. Please ensure fix_pattern_learning_persistence.py is available.")
    YAMLPatternManager = None

'''
            content = content.replace(first_import, import_addition + first_import)
            print("✅ Added YAMLPatternManager import to trainer")
    
    # 2. Añadir yaml_manager al __init__ del EnhancedTrainer
    init_pattern = r'(self\.training_stats = \{[^}]*\})'
    replacement = r'\1\n        \n        # Gestor de patrones YAML\n        self.yaml_manager = YAMLPatternManager("config/pattern_learning_config.yaml") if YAMLPatternManager else None'
    
    if re.search(init_pattern, content, re.DOTALL):
        content = re.sub(init_pattern, replacement, content, flags=re.DOTALL)
        print("✅ Added yaml_manager to EnhancedTrainer.__init__")
    
    # 3. Reemplazar método _save_learned_patterns
    old_save_pattern = r'def _save_learned_patterns\(self\):.*?except Exception as e:\s*logger\.error\(f"Error saving learned patterns: \{e\}"\)'
    
    new_save_method = '''def _save_learned_patterns(self):
        """Guarda patrones aprendidos en archivo YAML y JSON de respaldo"""
        try:
            if self.yaml_manager:
                # 1. Actualizar el archivo YAML principal
                for field_type, pattern_info in self.learned_patterns.items():
                    if pattern_info and 'patterns' in pattern_info and pattern_info['patterns']:
                        pattern_data = {
                            'regex_patterns': pattern_info['patterns'],
                            'statistical_patterns': {
                                'sample_count': len(pattern_info.get('examples', [])),
                                'column_names_seen': list(set(pattern_info.get('column_names', [])))
                            }
                        }
                        self.yaml_manager.add_learned_pattern(field_type, pattern_data)
                
                # Guardar YAML actualizado
                self.yaml_manager.save_yaml_config()
                print(f"💾 Learned patterns saved to YAML: config/pattern_learning_config.yaml")
            
            # 2. Crear respaldo JSON con timestamp
            os.makedirs('results/learned_patterns', exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            patterns_file = f"results/learned_patterns/patterns_{timestamp}.json"
            
            with open(patterns_file, 'w', encoding='utf-8') as f:
                json.dump(self.learned_patterns, f, indent=2, ensure_ascii=False)
            
            print(f"💾 JSON backup saved: {patterns_file}")
            
        except Exception as e:
            logger.error(f"Error saving learned patterns: {e}")
            print(f"❌ Error saving patterns: {e}")'''
    
    if re.search(old_save_pattern, content, re.DOTALL):
        content = re.sub(old_save_pattern, new_save_method, content, flags=re.DOTALL)
        print("✅ Updated _save_learned_patterns method in trainer")
    
    # Guardar archivo modificado
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ Successfully updated: {file_path}")
    return True

def verify_yaml_config():
    """Verifica que el archivo YAML tenga la estructura correcta"""
    yaml_file = "config/pattern_learning_config.yaml"
    
    if not os.path.exists(yaml_file):
        print(f"⚠️ YAML config file not found: {yaml_file}")
        return False
    
    try:
        import yaml
        with open(yaml_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Verificar estructura básica
        required_sections = ['pattern_learning', 'learning_thresholds', 'priority_patterns']
        missing_sections = [section for section in required_sections if section not in config]
        
        if missing_sections:
            print(f"⚠️ Missing sections in YAML: {missing_sections}")
        else:
            print("✅ YAML config structure is valid")
        
        return len(missing_sections) == 0
        
    except Exception as e:
        print(f"❌ Error reading YAML config: {e}")
        return False

def main():
    """Función principal para aplicar todas las correcciones"""
    print("🔧 APPLYING PATTERN PERSISTENCE FIXES")
    print("=" * 40)
    
    success_count = 0
    total_fixes = 3
    
    # 1. Verificar archivo YAML
    print("\n📋 Step 1: Verifying YAML configuration...")
    if verify_yaml_config():
        success_count += 1
    
    # 2. Corregir field_mapper.py
    print("\n🔧 Step 2: Fixing core/field_mapper.py...")
    if fix_field_mapper():
        success_count += 1
    
    # 3. Corregir complete_enhanced_trainer.py
    print("\n🔧 Step 3: Fixing complete_enhanced_trainer.py...")
    if fix_enhanced_trainer():
        success_count += 1
    
    # Resumen final
    print(f"\n📊 RESULTS: {success_count}/{total_fixes} fixes applied successfully")
    print("=" * 40)
    
    if success_count == total_fixes:
        print("✅ ALL FIXES APPLIED SUCCESSFULLY!")
        print("\n🎯 Next steps:")
        print("1. Test the system: python complete_enhanced_trainer.py ejemplo_sap_02.csv")
        print("2. Check that patterns are saved to: config/pattern_learning_config.yaml")
        print("3. Verify that learned patterns persist between training sessions")
        print("\n💡 The system will now:")
        print("• Save patterns to YAML instead of temporary JSON files")
        print("• Load previously learned patterns from YAML")
        print("• Maintain JSON backups for compatibility")
    else:
        print("❌ Some fixes failed. Please check the error messages above.")
        print("You may need to apply the changes manually.")
    
    print(f"\n📁 Backup files created with timestamp in their names")
    print("🔄 You can restore from backups if needed")

if __name__ == "__main__":
    main()
