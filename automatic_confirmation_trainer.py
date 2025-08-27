# automatic_confirmation_trainer.py - TRAINER AUTOMÁTICO REFACTORIZADO
# BASADO EN: manual_confirmation_trainer.py pero con decisiones automáticas
# ENHANCED: Usa módulos externos reutilizables para procesamiento, validación y reportes
# VERSIÓN: Modular y reutilizable para futuras versiones

import pandas as pd
import os
import sys
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import yaml
import numpy as np

# Importar módulos reutilizables
from accounting_data_processor import AccountingDataProcessor
from balance_validator import BalanceValidator
from csv_transformer import CSVTransformer
from training_reporter import TrainingReporter
from config.custom_field_validators import check_single_date_same_year_pattern

# Importar módulos existentes del core
try:
    from core.field_mapper import FieldMapper
    from core.field_detector import FieldDetector
except ImportError:
    print("⚠️ Core modules not found - please ensure core/ directory exists")
    sys.exit(1)

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutomaticConfirmationTrainingSession:
    """Sesión de entrenamiento AUTOMÁTICO modular - sin confirmación manual"""
    
    def __init__(self, csv_file: str, erp_hint: str = None):
        self.csv_file = csv_file
        self.erp_hint = erp_hint
        self.df = None
        self.mapper = None
        self.detector = None
        
        # MISMOS CAMPOS ESTÁNDAR que manual trainer
        self.standard_fields = [
            'journal_entry_id', 'line_number', 'description', 'line_description',
            'posting_date', 'fiscal_year', 'period_number', 'gl_account_number',
            'amount', 'debit_amount', 'credit_amount', 'debit_credit_indicator',
            'prepared_by', 'entry_date', 'entry_time', 'gl_account_name', 'vendor_id'
        ]
        
        # Estadísticas de entrenamiento automático
        self.training_stats = {
            'columns_processed': 0,
            'automatic_mappings': 0,
            'conflicts_resolved': 0,
            'amount_conflicts_resolved': 0,
            'journal_id_balance_resolutions': 0,
            'high_confidence_mappings': 0,
            'low_confidence_mappings': 0,
            'rejected_low_confidence': 0,
            'unmapped_columns': 0,
            'synonyms_added': 0,
            'regex_patterns_added': 0
        }
        
        # Umbral de confianza mínimo
        self.confidence_threshold = 0.75
        
        # Decisiones automáticas registradas (compatible con manual trainer)
        self.user_decisions = {}
        self.learned_patterns = {}
        self.new_synonyms = {}
        self.new_regex_patterns = {}
        self.conflict_resolutions = {}
        
        # Archivos de configuración
        self.yaml_config_file = "config/pattern_learning_config.yaml"
        self.dynamic_fields_file = "config/dynamic_fields_config.yaml"
        
        # Inicializar módulos reutilizables
        self.data_processor = AccountingDataProcessor()
        self.balance_validator = BalanceValidator()
        self.csv_transformer = CSVTransformer(output_prefix="automatic_training")
        self.reporter = TrainingReporter(report_prefix="automatic_training_report")
        
    def initialize(self) -> bool:
        """Inicializa la sesión de entrenamiento automático"""
        try:
            print(f"Initializing AUTOMATIC TRAINING Session...")
            print(f"File: {self.csv_file}")
            print(f"ERP Hint: {self.erp_hint or 'Auto-detect'}")
            
            # Verificar archivo
            if not os.path.exists(self.csv_file):
                print(f"❌ File not found: {self.csv_file}")
                return False
            
            # Cargar CSV
            self.df = pd.read_csv(self.csv_file)
            print(f"✅ CSV loaded: {len(self.df)} rows, {len(self.df.columns)} columns")
            
            # Inicializar detector y mapper
            self.detector = FieldDetector()
            self.mapper = FieldMapper()
            
            # Cargar patrones aprendidos
            self._load_learned_patterns()
            
            return True
            
        except Exception as e:
            logger.error(f"Error initializing session: {e}")
            print(f"Initialization failed: {e}")
            return False
    
    def _load_learned_patterns(self):
        """Carga patrones previamente aprendidos"""
        try:
            if os.path.exists(self.yaml_config_file):
                with open(self.yaml_config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    self.learned_patterns = config.get('learned_patterns', {})
                    print(f"Loaded {len(self.learned_patterns)} learned patterns")
            else:
                print("No previous learned patterns found")
        except Exception as e:
            print(f"Could not load learned patterns: {e}")
            self.learned_patterns = {}

    def _apply_additional_validations(self):
        """Aplica validaciones adicionales después del mapeo automático"""
        try:

            
            # Aplicar validación de patrón de fechas del mismo año
            print("📅 Checking single date same year pattern...")
            original_decisions = self.user_decisions.copy()
            
            # Aplicar la validación
            self.user_decisions = check_single_date_same_year_pattern(
                self.user_decisions, 
                self.df
            )
            
            # Contar si hubo cambios
            changes_count = 0
            for column_name, decision in self.user_decisions.items():
                original_decision = original_decisions.get(column_name, {})
                if decision.get('field_type') != original_decision.get('field_type'):
                    changes_count += 1
                    print(f"   ✅ Updated: {column_name} -> {decision['field_type']}")
            
            if changes_count == 0:
                print("   ℹ️ No date pattern changes needed")
            else:
                print(f"   🔄 Applied {changes_count} date pattern updates")
                self.training_stats['date_pattern_updates'] = changes_count
                
        except Exception as e:
            print(f"   ⚠️ Error applying additional validations: {e}")
            # No fallar el proceso completo por esto
            pass
        
    def run_automatic_training(self) -> Dict:
        """Ejecuta el entrenamiento automático SIN confirmación manual"""
        try:
            print(f"\nStarting AUTOMATIC FIELD TRAINING...")
            print(f"=" * 55)
            print(f"All decisions will be made automatically based on confidence")
            print(f"Special rule for 'amount': prioritize 'local' columns ALWAYS")
            print(f"Confidence threshold: {self.confidence_threshold}")
            
            # Análisis inicial del DataFrame
            self._show_initial_analysis()
            
            # 1. Detectar campos automáticamente
            field_analysis = self._perform_automatic_field_detection()
            
            if not field_analysis['success']:
                return {'success': False, 'error': 'Field detection failed'}
            
            # 2. Resolver conflictos automáticamente
            final_mappings = self._resolve_conflicts_automatically(field_analysis['mappings'])
            
            # 3. Aplicar filtro de confianza
            filtered_mappings = self._apply_confidence_filter(final_mappings)
            
            # 4. Actualizar decisiones de usuario
            self._update_user_decisions_from_mappings(filtered_mappings)

            # 4.5. APLICAR VALIDACIONES ADICIONALES DE FECHAS
            self._apply_additional_validations()
            
            # 5. Finalizar entrenamiento y generar outputs
            result = self._finalize_automatic_training()
            
            return result
            
        except Exception as e:
            logger.error(f"Error in automatic training: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}
    
    def _show_initial_analysis(self):
        """Muestra análisis inicial del CSV"""
        print(f"CSV Columns ({len(self.df.columns)}):")
        for i, col in enumerate(self.df.columns, 1):
            sample_data = self.df[col].dropna().head(3).tolist()
            print(f"  {i:2d}. {col} → {sample_data}")
        print()
    
    def _perform_automatic_field_detection(self) -> Dict:
        """Realiza detección automática de campos usando el field_mapper"""
        try:
            print(f"🔍 AUTOMATIC FIELD DETECTION")
            print(f"-" * 30)
            
            mappings = {}
            self.training_stats['columns_processed'] = len(self.df.columns)
            
            for column in self.df.columns:
                print(f"\nAnalyzing column: '{column}'")
                
                # Obtener análisis del mapper
                # Obtener datos de muestra para el análisis
                sample_data = self.df[column].dropna().head(100)

                # CORRECCIÓN: Usar find_field_mapping en lugar de analyze_column
                mapping_result = self.mapper.find_field_mapping(
                    field_name=column,
                    erp_system=self.erp_hint,
                    sample_data=sample_data
)
                
                # Encontrar mejor match
                if mapping_result:
                    field_type, confidence = mapping_result
                    print(f"   Best match: {field_type} (confidence: {confidence:.3f})")
                    
                    mappings[column] = {
                        'field_type': field_type,
                        'confidence': confidence
                    }
                else:
                    print(f"   No matches found")
            
            return {'success': True, 'mappings': mappings}
            
        except Exception as e:
            logger.error(f"Error in field detection: {e}")
            return {'success': False, 'error': str(e)}
    
    def _resolve_conflicts_automatically(self, mappings: Dict) -> Dict:
        """Resuelve conflictos automáticamente usando reglas predefinidas"""
        print(f"\n⚖️ AUTOMATIC CONFLICT RESOLUTION")
        print(f"-" * 35)
        
        # Agrupar por field_type para detectar conflictos
        field_type_groups = {}
        for column, mapping in mappings.items():
            field_type = mapping['field_type']
            if field_type not in field_type_groups:
                field_type_groups[field_type] = []
            field_type_groups[field_type].append((column, mapping['confidence']))
        
        final_mappings = {}
        
        for field_type, candidates in field_type_groups.items():
            if len(candidates) == 1:
                # Sin conflicto
                column, confidence = candidates[0]
                final_mappings[column] = {
                    'field_type': field_type,
                    'confidence': confidence,
                    'resolution_type': 'no_conflict'
                }
                print(f"   {field_type}: {column} (no conflict)")
            
            else:
                # Conflicto detectado - resolver automáticamente
                winner_column, winner_confidence, resolution_type = self._resolve_field_conflict(
                    field_type, candidates
                )
                
                final_mappings[winner_column] = {
                    'field_type': field_type,
                    'confidence': winner_confidence,
                    'resolution_type': resolution_type
                }
                
                # Registrar resolución para el reporte
                self.conflict_resolutions[field_type] = {
                    'winner': winner_column,
                    'resolution_type': resolution_type,
                    'all_candidates': [f"{col}({conf:.3f})" for col, conf in candidates]
                }
                
                self.training_stats['conflicts_resolved'] += 1
                if field_type == 'journal_entry_id' and resolution_type == 'journal_id_balance_tested':
                    self.training_stats['journal_id_balance_resolutions'] += 1

                if field_type == 'amount':
                    self.training_stats['amount_conflicts_resolved'] += 1
        
        return final_mappings
    
    def _resolve_field_conflict(self, field_type: str, candidates: List[Tuple[str, float]]) -> Tuple[str, float, str]:
        """Resuelve conflicto para un field_type específico usando reglas automáticas"""
        print(f"   Resolving conflict for '{field_type}':")
        for col, conf in candidates:
            print(f"     - {col}: {conf:.3f}")
        # NUEVA REGLA ESPECIAL para 'journal_entry_id': usar balance testing
        if field_type == 'journal_entry_id':
            balance_fields = self._identify_balance_fields()
            if balance_fields['found']:
                balance_result = self._resolve_journal_entry_id_with_balance(candidates, balance_fields)
                if balance_result:
                    print(f"    JOURNAL_ID BALANCE RULE: '{balance_result[0]}' selected (balance rate: {balance_result[3]*100:.1f}%)")
                    return balance_result[:3]  # column, confidence, resolution_type
        
        # REGLA ESPECIAL para 'amount': priorizar columnas 'local'
        if field_type == 'amount':
            for column, confidence in candidates:
                if 'local' in column.lower():
                    print(f"    AMOUNT SPECIAL RULE: '{column}' selected (contains 'local')")
                    return (column, confidence, 'amount_local_priority')
        
        # REGLA GENERAL: mayor confianza gana
        candidates_sorted = sorted(candidates, key=lambda x: x[1], reverse=True)
        winner_column, winner_confidence = candidates_sorted[0]
        
        print(f"    GENERAL RULE: '{winner_column}' has highest confidence ({winner_confidence:.3f})")
        return (winner_column, winner_confidence, 'highest_confidence')
    
    def _identify_balance_fields(self) -> Dict[str, Any]:
        """Identifica campos de balance usando resultados de mapeo existentes"""
        print(f"    Identifying balance fields...")
        
        # Buscar en los mapeos ya realizados durante field detection
        amount_column = None
        debit_column = None  
        credit_column = None
        
        # Revisar los mapeos que ya se hicieron en field_analysis['mappings']
        # Los resultados ya están en self, necesitamos acceder a ellos
        
        # Buscar entre las columnas ya procesadas cuáles fueron mapeadas a balance
        for column in self.df.columns:
            # Hacer una consulta rápida sin procesar de nuevo
            sample_data = self.df[column].dropna().head(5)
            
            # Solo verificar si contiene palabras clave para evitar remapear
            col_lower = column.lower()
            
            # Buscar amount
            if not amount_column and ('importe' in col_lower and 'debe' not in col_lower and 'haber' not in col_lower):
                amount_column = column
                print(f"      Found amount: '{column}'")
                
            # Buscar debit
            elif not debit_column and ('debe' in col_lower or 'debit' in col_lower):
                debit_column = column 
                print(f"      Found debit: '{column}'")
                
            # Buscar credit  
            elif not credit_column and ('haber' in col_lower or 'credit' in col_lower):
                credit_column = column
                print(f"      Found credit: '{column}'")
        
        # Determinar tipo de balance
        has_debit_credit = debit_column and credit_column
        has_amount_only = amount_column and not has_debit_credit
        
        if has_debit_credit:
            balance_type = 'debit_credit'
        elif has_amount_only:
            balance_type = 'amount_only'
        else:
            balance_type = 'none'
        
        print(f"      Balance type: {balance_type}")
        
        return {
            'found': has_debit_credit or has_amount_only,
            'balance_type': balance_type,
            'amount_column': amount_column,
            'debit_column': debit_column,
            'credit_column': credit_column
        }

    def _resolve_journal_entry_id_with_balance(self, candidates: List[Tuple[str, float]], 
                                            balance_fields: Dict[str, Any]) -> Optional[Tuple[str, float, str, float]]:
        """Resuelve conflictos de journal_entry_id usando balance testing"""
        print(f"    BALANCE TESTING: Testing {len(candidates)} candidates...")
        
        candidate_columns = [col for col, conf in candidates]
        balance_results = {}
        
        try:
            for column_name in candidate_columns:
                if balance_fields['balance_type'] == 'debit_credit':
                    balance_score = self._test_with_debit_credit(
                        column_name, balance_fields['debit_column'], balance_fields['credit_column']
                    )
                elif balance_fields['balance_type'] == 'amount_only':
                    balance_score = self._test_with_amount_only(
                        column_name, balance_fields['amount_column']
                    )
                else:
                    balance_score = 0.0
                
                balance_results[column_name] = balance_score
                print(f"      {column_name}: {balance_score*100:.1f}%")
            
            if not balance_results or max(balance_results.values()) == 0:
                return None
                
            best_column = max(balance_results.keys(), key=lambda x: balance_results[x])
            best_balance_rate = balance_results[best_column]
            best_original_confidence = next(conf for col, conf in candidates if col == best_column)
            
            if best_balance_rate > 0.6:
                return (best_column, best_original_confidence, 'journal_id_balance_tested', best_balance_rate)
            else:
                return None
                
        except Exception as e:
            print(f"    ⚠️ Balance testing error: {e}")
            return None    

    def _test_with_debit_credit(self, candidate_column: str, debit_column: str, credit_column: str) -> float:
        """Prueba candidato usando campos debit/credit"""
        try:
            unique_entries = self.df[candidate_column].dropna().unique()
            if len(unique_entries) == 0:
                return 0.0
            
            # Tomar muestra
            sample_size = min(20, len(unique_entries))
            if len(unique_entries) > sample_size:
                import numpy as np
                np.random.seed(42)
                sample_entries = np.random.choice(unique_entries, size=sample_size, replace=False)
            else:
                sample_entries = unique_entries
            
            balanced_count = 0
            valid_entries = 0
            
            for entry_id in sample_entries:
                entry_lines = self.df[self.df[candidate_column] == entry_id]
                
                if len(entry_lines) < 2:
                    continue
                    
                total_debit = self._sum_column(entry_lines[debit_column])
                total_credit = self._sum_column(entry_lines[credit_column])
                difference = abs(total_debit - total_credit)
                
                valid_entries += 1
                
                if difference < max(0.01, max(total_debit, total_credit) * 0.01):
                    balanced_count += 1
            
            return balanced_count / valid_entries if valid_entries > 0 else 0.0
            
        except Exception as e:
            return 0.0

    
    def _test_with_amount_only(self, candidate_column: str, amount_column: str) -> float:
        """Prueba candidato usando solo campo amount"""
        try:
            unique_entries = self.df[candidate_column].dropna().unique()
            if len(unique_entries) == 0:
                return 0.0
            
            # Tomar muestra
            sample_size = min(3, len(unique_entries))
            if len(unique_entries) > sample_size:
                import numpy as np
                np.random.seed(42)
                sample_entries = np.random.choice(unique_entries, size=sample_size, replace=False)
            else:
                sample_entries = unique_entries
            
            balanced_count = 0
            valid_entries = 0
            
            for entry_id in sample_entries:
                entry_lines = self.df[self.df[candidate_column] == entry_id]
                
                if len(entry_lines) < 2:
                    continue
                    
                total_amount = self._sum_column(entry_lines[amount_column])
                valid_entries += 1
                
                if abs(total_amount) < 0.01:
                    balanced_count += 1
            
            return balanced_count / valid_entries if valid_entries > 0 else 0.0
            
        except Exception as e:
            return 0.0

    
    def _sum_column(self, series) -> float:
        """Suma valores numéricos de una serie"""
        total = 0.0
        
        for value in series.dropna():
            try:
                if isinstance(value, (int, float)):
                    total += float(value)
                    continue
                    
                clean_value = str(value).strip().replace(',', '.').replace(' ', '')
                clean_value = clean_value.replace('€', '').replace('$', '').replace('£', '')
                
                if clean_value.startswith('(') and clean_value.endswith(')'):
                    clean_value = '-' + clean_value[1:-1]
                
                if clean_value and clean_value not in ['', '-', 'nan']:
                    total += float(clean_value)
                    
            except (ValueError, TypeError):
                continue
        
        return total
    def _test_journal_id_candidate_balance(self, candidate_column: str, sample_size: int = 3, min_entries_per_sample: int = 2) -> float:
        """
        Prueba un candidato individual para journal_entry_id usando balance de asientos.
        
        Args:
            candidate_column: Columna candidata a probar
            sample_size: Número de asientos únicos a probar
            min_entries_per_sample: Mínimo líneas por asiento
            
        Returns:
            float: Tasa de balance (0.0 a 1.0)
        """
        try:
            # Obtener valores únicos del candidato
            unique_entries = self.df[candidate_column].dropna().unique()
            
            if len(unique_entries) == 0:
                return 0.0
            
            # Tomar muestra si hay muchos valores
            if len(unique_entries) > sample_size:
                import numpy as np
                np.random.seed(42)  # Para reproducibilidad
                sample_entries = np.random.choice(unique_entries, size=sample_size, replace=False)
            else:
                sample_entries = unique_entries
            
            balanced_count = 0
            valid_entries_tested = 0
            
            # Probar cada asiento en la muestra
            for entry_id in sample_entries:
                entry_lines = self.df[self.df[candidate_column] == entry_id]
                
                # Verificar suficientes líneas
                if len(entry_lines) < min_entries_per_sample:
                    continue
                    
                # Calcular balance
                total_debit = entry_lines['debit_amount'].sum()
                total_credit = entry_lines['credit_amount'].sum()
                balance_difference = abs(total_debit - total_credit)
                
                valid_entries_tested += 1
                
                # Considerar balanceado si diferencia < 0.01
                if balance_difference < 0.01:
                    balanced_count += 1
            
            if valid_entries_tested == 0:
                return 0.0
            
            balanced_rate = balanced_count / valid_entries_tested
            return balanced_rate
            
        except Exception as e:
            print(f"    Error testing {candidate_column}: {e}")
            return 0.0
    
    def _apply_confidence_filter(self, mappings: Dict) -> Dict:
        """Aplica filtro de confianza mínima"""
        print(f"\n🔍 APPLYING CONFIDENCE FILTER (threshold: {self.confidence_threshold})")
        print(f"-" * 40)
        
        filtered_mappings = {}
        rejected_count = 0
        
        for column, mapping in mappings.items():
            confidence = mapping['confidence']
            
            if confidence >= self.confidence_threshold:
                filtered_mappings[column] = mapping
                print(f"   ✅ {column}: {mapping['field_type']} ({confidence:.3f}) - ACCEPTED")
            else:
                rejected_count += 1
                print(f"   ❌ {column}: {mapping['field_type']} ({confidence:.3f}) - REJECTED (low confidence)")
        
        self.training_stats['rejected_low_confidence'] = rejected_count
        print(f"\n   Final: {len(filtered_mappings)} accepted, {rejected_count} rejected")
        
        return filtered_mappings
    
    def _update_user_decisions_from_mappings(self, final_mappings: Dict):
        """Actualiza user_decisions basado en mapeos finales"""
        for column_name, mapping_info in final_mappings.items():
            field_type = mapping_info['field_type']
            confidence = mapping_info['confidence']
            resolution_type = mapping_info['resolution_type']
            
            # Determinar tipo de decisión automática
            if resolution_type == 'no_conflict':
                decision_type = 'automatic_no_conflict'
            else:
                decision_type = f'automatic_{resolution_type}'
            
            self.user_decisions[column_name] = {
                'field_type': field_type,
                'confidence': confidence,
                'decision_type': decision_type,
                'resolution_type': resolution_type
            }
            
            # Actualizar estadísticas
            if confidence > 0.8:
                self.training_stats['high_confidence_mappings'] += 1
            else:
                self.training_stats['low_confidence_mappings'] += 1
                
            self.training_stats['automatic_mappings'] += 1
    
    def _finalize_automatic_training(self) -> Dict:
        """Finaliza el entrenamiento automático usando módulos reutilizables"""
        try:
            print(f"\n🏁 AUTOMATIC TRAINING FINALIZATION")
            print(f"=" * 40)
            
            # 1. Crear DataFrame transformado con mapeos
            transformed_df = self.df.copy()
            column_mapping = {col: decision['field_type'] for col, decision in self.user_decisions.items()}
            transformed_df = transformed_df.rename(columns=column_mapping)
            
            # 2. USAR MÓDULO: Procesar campos numéricos y calcular amounts
            transformed_df, processing_stats = self.data_processor.process_numeric_fields_and_calculate_amounts(
                transformed_df
            )
            
            # Integrar estadísticas de procesamiento
            self.training_stats.update(processing_stats)
            
            # 3. USAR MÓDULO: Realizar validaciones de balance
            balance_report = self.balance_validator.perform_comprehensive_balance_validation(
                transformed_df
            )
            
            # Integrar estadísticas de validación
            self.training_stats.update(balance_report.get('validation_stats', {}))
            
            # 4. USAR MÓDULO: Crear CSV de cabecera y detalle
            csv_result = self.csv_transformer.create_header_detail_csvs(
                self.df, self.user_decisions, self.standard_fields
            )
            
            # 5. USAR MÓDULO: Generar reporte de entrenamiento
            training_data = {
                'csv_file': self.csv_file,
                'erp_hint': self.erp_hint,
                'training_stats': self.training_stats,
                'user_decisions': self.user_decisions,
                'conflict_resolutions': self.conflict_resolutions,
                'balance_report': balance_report,
                'training_mode': 'automatic',
                'standard_fields': self.standard_fields,
                **csv_result
            }
            
            report_file = self.reporter.generate_comprehensive_training_report(training_data)
            
            # 6. Preparar resultado final
            result = {
                'success': True,
                'training_stats': self.training_stats,
                'user_decisions': self.user_decisions,
                'conflict_resolutions': self.conflict_resolutions,
                'balance_report': balance_report,
                'report_file': report_file,
                **csv_result
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error finalizing training: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}

def run_automatic_training(csv_file: str, erp_hint: str = None) -> Dict:
    """Función principal para ejecutar entrenamiento automático"""
    try:
        print(f"🤖 AUTOMATIC CONFIRMATION TRAINER - MODULAR VERSION")
        print(f"=" * 55)
        print(f"Starting automatic training session...")
        print(f"File: {csv_file}")
        print(f"ERP: {erp_hint or 'Auto-detect'}")
        print(f"Decision mode: AUTOMATIC (no confirmation required)")
        print(f"Quality filter: Only confidence > 0.75 accepted")
        print(f"Special rule: AMOUNT field prioritizes 'local' ALWAYS")
        print(f"Enhancement: Modular processing with reusable components")
        print()
        
        # Crear sesión de entrenamiento automático
        session = AutomaticConfirmationTrainingSession(csv_file, erp_hint)
        
        # Inicializar
        if not session.initialize():
            return {'success': False, 'error': 'Initialization failed'}
        
        # Ejecutar entrenamiento automático
        result = session.run_automatic_training()
        
        if result['success']:
            print(f"\n✅ AUTOMATIC TRAINING COMPLETED SUCCESSFULLY!")
            
            # Mostrar resumen de resultados usando módulos
            print(f"\n📊 RESULTS SUMMARY:")
            print(f"   • Automatic mappings: {result['training_stats']['automatic_mappings']}")
            print(f"   • Conflicts resolved: {result['training_stats']['conflicts_resolved']}")
            print(f"   • High confidence decisions: {result['training_stats']['high_confidence_mappings']}")
            print(f"   • Low confidence rejected: {result['training_stats']['rejected_low_confidence']}")
            
            if 'zero_filled_fields' in result['training_stats']:
                print(f"   • Numeric fields processed: {result['training_stats']['fields_cleaned']}")
                print(f"   • Zero-filled values: {result['training_stats']['zero_filled_fields']}")
            
            # Mostrar información de balance
            if result.get('balance_report'):
                balance = result['balance_report']
                print(f"\n⚖️ BALANCE VALIDATION:")
                print(f"   • Total Balance: {'✅ BALANCED' if balance['is_balanced'] else '❌ UNBALANCED'}")
                print(f"   • Total Debit: {balance['total_debit_sum']:,.2f}")
                print(f"   • Total Credit: {balance['total_credit_sum']:,.2f}")
                
                if balance['entries_count'] > 0:
                    balanced_pct = balance['balanced_entries_count'] / balance['entries_count'] * 100
                    print(f"   • Entry Balance Rate: {balanced_pct:.1f}%")
            
            # Mostrar archivos generados
            if result.get('header_file') and result.get('detail_file'):
                print(f"\n📄 FILES CREATED:")
                print(f"   • Header CSV: {result['header_file']}")
                print(f"   • Detail CSV: {result['detail_file']}")
                print(f"   • Training Report: {result['report_file']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Automatic training failed: {e}")
        print(f"Automatic training failed: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}

def main():
    """Función principal - COMPATIBLE con manual_confirmation_trainer.py"""
    if len(sys.argv) < 2:
        print("AUTOMATIC CONFIRMATION TRAINER - ENHANCED MODULAR VERSION")
        print("=" * 60)
        print("Training with AUTOMATIC DECISIONS - no manual confirmation required")
        print()
        print("🔧 NEW MODULAR FEATURES:")
        print("  • Reusable accounting data processor")
        print("  • Comprehensive balance validator")
        print("  • Advanced CSV transformer (header/detail separation)")
        print("  • Professional training reporter")
        print("  • All decisions made automatically based on confidence")
        print("  • High confidence: automatic assignment")
        print("  • Confidence filter: Only mappings > 0.75 are included")
        print("  • Conflicts resolved by highest confidence")
        print("  • Special rule for 'amount': prioritizes 'local' ALWAYS")
        print("  • Automatic numeric field cleaning and amount calculation")
        print("  • Zero-fill empty numeric fields (debit, credit, amount)")
        print("  • Balance validation by entry and total")
        print("  • Ordered output by journal_entry_id (ascending)")
        print("  • Same standard fields (17 fields total)")
        print("  • Compatible with main_global.py")
        print()
        print("📋 STANDARD FIELDS:")
        standard_fields = [
            'journal_entry_id', 'line_number', 'description', 'line_description',
            'posting_date', 'fiscal_year', 'period_number', 'gl_account_number',
            'amount', 'debit_amount', 'credit_amount', 'debit_credit_indicator',
            'prepared_by', 'entry_date', 'entry_time', 'gl_account_name', 'vendor_id'
        ]
        for i, field in enumerate(standard_fields, 1):
            print(f"  {i:2d}. {field}")
        print()
        print("Usage:")
        print("  python automatic_confirmation_trainer.py <csv_file> [erp_hint]")
        print()
        print("Examples:")
        print("  python automatic_confirmation_trainer.py data/journal.csv")
        print("  python automatic_confirmation_trainer.py data/journal.csv SAP")
        print("  python automatic_confirmation_trainer.py data/journal.csv Oracle")
        print()
        print("🎯 MODULAR OUTPUT FILES:")
        print("  • automatic_training_report_TIMESTAMP.txt")
        print("  • automatic_training_header_TIMESTAMP.csv")
        print("  • automatic_training_detail_TIMESTAMP.csv")
        print()
        print("⚙️ PROCESSING CAPABILITIES:")
        print("  • Cleans currency symbols and text from numeric fields")
        print("  • Handles different number formats (1,234.56 vs 1.234,56)")
        print("  • Fills empty numeric fields with 0.0")
        print("  • Calculates amount = debit_amount - credit_amount if needed")
        print("  • Calculates debit/credit amounts from amount + indicator")
        print("  • Adjusts amount signs (positive for debits, negative for credits)")
        print("  • Validates total balance: debit_sum == credit_sum")
        print("  • Checks balance by journal entry: debit - credit = 0 per entry")
        print("  • Reports unbalanced entries in detail")
        return
    
    # Extraer parámetros
    csv_file = sys.argv[1]
    erp_hint = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Ejecutar entrenamiento automático
    result = run_automatic_training(csv_file, erp_hint)
    
    if not result['success']:
        print(f"❌ Training failed: {result.get('error')}")
        sys.exit(1)
    
    print(f"\n✅ Automatic training completed successfully!")
    print(f"📊 Check the generated files for detailed results.")

if __name__ == "__main__":
    main()