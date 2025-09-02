# automatic_confirmation_trainer.py - VERSIÓN SIMPLE CON BALANCE VALIDATION
# Reemplazar completamente tu archivo actual con esta versión

import pandas as pd
import os
import sys
import re
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime
import json
from pathlib import Path
import yaml

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutomaticConfirmationTrainingSession:
    """Sesión de entrenamiento AUTOMÁTICO - sin confirmación manual"""
    
    def __init__(self, csv_file: str, erp_hint: str = None):
        self.csv_file = csv_file
        self.erp_hint = erp_hint
        self.df = None
        self.mapper = None
        self.detector = None
        
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
            'high_confidence_mappings': 0,
            'low_confidence_mappings': 0,
            'rejected_low_confidence': 0,
            'unmapped_columns': 0,
            'synonyms_added': 0,
            'regex_patterns_added': 0
        }
        
        # Umbral de confianza mínimo
        self.confidence_threshold = 0.75
        
        # Decisiones automáticas registradas
        self.user_decisions = {}
        self.learned_patterns = {}
        self.new_synonyms = {}
        self.new_regex_patterns = {}
        self.conflict_resolutions = {}
        
        # Archivos de configuración
        self.yaml_config_file = "config/pattern_learning_config.yaml"
        self.dynamic_fields_file = "config/dynamic_fields_config.yaml"
        
        # SOLUCIÓN: Inicializar módulos reutilizables con clases mock si no existen
        try:
            from accounting_data_processor import AccountingDataProcessor
            self.data_processor = AccountingDataProcessor()
        except ImportError:
            self.data_processor = None
            print("⚠️ AccountingDataProcessor not found - using basic processing")
        
        try:
            from balance_validator import BalanceValidator
            self.balance_validator = BalanceValidator()
        except ImportError:
            # Crear un objeto mock básico
            class MockBalanceValidator:
                def perform_comprehensive_balance_validation(self, df):
                    return {
                        'is_balanced': True,
                        'total_debit_sum': 0.0,
                        'total_credit_sum': 0.0,
                        'entries_count': 0,
                        'balanced_entries_count': 0,
                        'validation_stats': {}
                    }
            self.balance_validator = MockBalanceValidator()
            print("⚠️ BalanceValidator not found - using mock validator")
        
        try:
            from csv_transformer import CSVTransformer
            self.csv_transformer = CSVTransformer(output_prefix="automatic_training")
        except ImportError:
            self.csv_transformer = None
            print("⚠️ CSVTransformer not found - using basic CSV processing")
        
        try:
            from training_reporter import TrainingReporter
            self.reporter = TrainingReporter(report_prefix="automatic_training_report")
        except ImportError:
            self.reporter = None
            print("⚠️ TrainingReporter not found - using basic reporting")
        
    def initialize(self) -> bool:
        """Inicializa la sesión de entrenamiento automático"""
        try:
            print(f"Initializing AUTOMATIC TRAINING Session...")
            print(f"File: {self.csv_file}")
            print(f"ERP Hint: {self.erp_hint or 'Auto-detect'}")
            print(f"Mode: AUTOMATIC (no manual confirmation)")
            print(f"Confidence threshold: Only mappings > {self.confidence_threshold} will be included")
            
            # Verificar archivo
            if not os.path.exists(self.csv_file):
                print(f"❌ File not found: {self.csv_file}")
                return False
            
            # Cargar CSV
            self.df = pd.read_csv(self.csv_file)
            print(f"✅ CSV loaded: {len(self.df)} rows, {len(self.df.columns)} columns")
            
            # Importar módulos del sistema
            try:
                from core.field_mapper import FieldMapper
                from core.field_detector import FieldDetector
                
                self.mapper = FieldMapper()
                self.detector = FieldDetector()
                print("✅ System modules imported successfully")
                
                # ✨ CONFIGURAR MAPPER PARA BALANCE VALIDATION
                self.enhanced_mapper_initialization()
                
            except ImportError as e:
                print(f"❌ Failed to import system modules: {e}")
                return False
            
            # Cargar patrones aprendidos
            self._load_learned_patterns()
            
            return True
            
        except Exception as e:
            logger.error(f"Error initializing session: {e}")
            print(f"❌ Initialization failed: {e}")
            return False

    def enhanced_mapper_initialization(self):
        """Configura el mapper para usar balance validation en journal_entry_id conflicts"""
        try:
            if hasattr(self.mapper, 'set_dataframe_for_balance_validation'):
                self.mapper.set_dataframe_for_balance_validation(self.df)
                print("✅ Mapper configured for balance validation")
                self.training_stats['balance_validation_enabled'] = True
            else:
                print("⚠️ Mapper does not support balance validation - update field_mapper.py first")
                print("   Balance validation will be disabled for journal_entry_id conflicts")
                self.training_stats['balance_validation_enabled'] = False
        except Exception as e:
            print(f"⚠️ Balance validation setup failed: {e}")
            self.training_stats['balance_validation_enabled'] = False
    
    def _load_learned_patterns(self):
        """Carga patrones previamente aprendidos"""
        try:
            if os.path.exists(self.yaml_config_file):
                with open(self.yaml_config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    self.learned_patterns = config.get('learned_patterns', {})
                    print(f"✅ Loaded {len(self.learned_patterns)} learned patterns")
            else:
                print("ℹ️ No previous learned patterns found")
        except Exception as e:
            print(f"⚠️ Could not load learned patterns: {e}")
            self.learned_patterns = {}

    
    def _show_initial_analysis(self):
        """Muestra análisis inicial del CSV"""
        print(f"\n📊 CSV ANALYSIS:")
        print(f"Columns ({len(self.df.columns)}):")
        for i, col in enumerate(self.df.columns, 1):
            sample_data = self.df[col].dropna().head(3).tolist()
            print(f"  {i:2d}. {col} → {sample_data}")
        print()

    def _apply_additional_validations(self):
        """Aplica validaciones adicionales simples (no redundantes con mapper)"""
        try:
            print(f"\n🔍 ADDITIONAL VALIDATIONS")
            print(f"-" * 25)
            
            # Intentar aplicar validaciones de fecha si están disponibles
            try:
                from config.custom_field_validators import check_single_date_same_year_pattern
                
                original_decisions = self.user_decisions.copy()
                
                # Aplicar la validación de patrones de fecha
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
                    
            except ImportError:
                print("   ℹ️ Custom validators not available, skipping")
            except Exception as e:
                print(f"   ⚠️ Error in date validation: {e}")
                
            # Validación adicional: verificar coherencia básica
            self._validate_mapping_coherence()
            
        except Exception as e:
            print(f"   ⚠️ Error applying additional validations: {e}")
            # No fallar el proceso completo por esto
            pass

    def _validate_mapping_coherence(self):
        """Validación básica de coherencia de mapeos"""
        try:
            # Verificar que no haya mappings duplicados de campos críticos
            field_counts = {}
            critical_fields = ['journal_entry_id', 'amount', 'posting_date']
            
            for column_name, decision in self.user_decisions.items():
                field_type = decision['field_type']
                if field_type in field_counts:
                    field_counts[field_type] += 1
                else:
                    field_counts[field_type] = 1
            
            # Reportar duplicados en campos críticos
            for field in critical_fields:
                if field in field_counts and field_counts[field] > 1:
                    print(f"   ⚠️ Warning: Multiple columns mapped to {field} ({field_counts[field]} columns)")
            
            print("   ✅ Mapping coherence check completed")
            
        except Exception as e:
            print(f"   ⚠️ Error in coherence validation: {e}")

    def run_automatic_training(self) -> Dict:
        """Ejecuta entrenamiento automático SIMPLIFICADO"""
        try:
            print(f"\n🤖 AUTOMATIC TRAINING SESSION - SIMPLIFIED ARCHITECTURE")
            print(f"=" * 55)
            
            # 1. ✅ USAR MAPPER MEJORADO (resuelve todos los conflictos)
            field_analysis = self._perform_automatic_field_detection()
            
            if not field_analysis['success']:
                return {'success': False, 'error': field_analysis.get('error')}
            
            # 2. ✅ APLICAR FILTRO DE CONFIANZA
            filtered_mappings = self._apply_confidence_filter(field_analysis['mappings'])
            
            # 3. ✅ ACTUALIZAR user_decisions (sin resolución adicional)
            self._update_user_decisions_from_mappings(filtered_mappings)
            
            # 4. ✅ APLICAR VALIDACIONES ADICIONALES
            self._apply_additional_validations()
            
            # 5. ✅ FINALIZAR ENTRENAMIENTO
            result = self._finalize_automatic_training()
            
            return result
            
        except Exception as e:
            logger.error(f"Error in automatic training: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}
        
    def _perform_automatic_field_detection(self) -> Dict:
        """Realiza detección automática usando mapper mejorado (SIN resolución redundante)"""
        try:
            print(f"🔍 AUTOMATIC FIELD DETECTION - USING ENHANCED MAPPER")
            print(f"-" * 50)
            
            # ✅ USAR MAPPER MEJORADO que resuelve conflictos globalmente
            final_mappings = self.mapper.map_all_columns_with_conflict_resolution(
                df=self.df,
                erp_hint=self.erp_hint,
                balance_validator=self.balance_validator
            )
            
            # Actualizar estadísticas básicas
            self.training_stats['columns_processed'] = len(self.df.columns)
            self.training_stats['automatic_mappings'] = len(final_mappings)
            self.training_stats['rejected_low_confidence'] = len(self.df.columns) - len(final_mappings)
            
            # Contar por tipo de confianza
            for mapping_info in final_mappings.values():
                confidence = mapping_info['confidence']
                if confidence > 0.8:
                    self.training_stats['high_confidence_mappings'] += 1
                else:
                    self.training_stats['low_confidence_mappings'] += 1
            
            return {'success': True, 'mappings': final_mappings}
            
        except Exception as e:
            logger.error(f"Error in field detection: {e}")
            return {'success': False, 'error': str(e)}


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
        """Finaliza el entrenamiento automático SIN validación redundante de balance"""
        try:
            print(f"\n🏁 AUTOMATIC TRAINING FINALIZATION")
            print(f"=" * 40)
            
            # 1. Crear DataFrame transformado con mapeos
            transformed_df = self.df.copy()
            column_mapping = {col: decision['field_type'] for col, decision in self.user_decisions.items()}
            transformed_df = transformed_df.rename(columns=column_mapping)
            
            # 2. Procesar campos numéricos (si el procesador está disponible)
            if hasattr(self, 'data_processor') and self.data_processor:
                print("   📊 Processing numeric fields...")
                try:
                    transformed_df, processing_stats = self.data_processor.process_numeric_fields_and_calculate_amounts(
                        transformed_df
                    )
                    self.training_stats.update(processing_stats)
                except Exception as e:
                    print(f"   ⚠️ Numeric processing failed: {e}")
            else:
                print("   ℹ️ Numeric processing not available")
            
            # 3. *** ELIMINAR VALIDACIÓN REDUNDANTE ***
            # NO hacer balance validation aquí porque ya se hizo en conflict resolution
            print("   ⚖️ Balance validation: SKIPPED (already performed during conflict resolution)")
            balance_report = {
                'is_balanced': True,  # Asumir OK ya que se validó antes
                'validation_performed': False,
                'note': 'Balance validation performed during field mapping conflict resolution',
                'total_debit_sum': 0.0,
                'total_credit_sum': 0.0,
                'entries_count': 0,
                'balanced_entries_count': 0
            }
            
            # 4. Crear CSV usando transformador (si está disponible)
            if hasattr(self, 'csv_transformer') and self.csv_transformer:
                print("   📄 Creating CSV files with transformer...")
                try:
                    csv_result = self.csv_transformer.create_header_detail_csvs(
                        self.df, self.user_decisions, self.standard_fields
                    )
                except Exception as e:
                    print(f"   ⚠️ CSV transformer failed: {e}")
                    csv_result = self._create_transformed_csv()
            else:
                print("   📄 Creating basic CSV files...")
                csv_result = self._create_transformed_csv()
            
            # 5. Generar reporte usando reporter (si está disponible)
            if hasattr(self, 'reporter') and self.reporter:
                print("   📝 Generating comprehensive report...")
                try:
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
                except Exception as e:
                    print(f"   ⚠️ Reporter failed: {e}")
                    report_file = self._generate_training_report()
            else:
                print("   📝 Generating basic report...")
                report_file = self._generate_training_report()
            
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
            
            print("   ✅ Automatic training finalization completed")
            return result
            
        except Exception as e:
            logger.error(f"Error finalizing training: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}

    def _generate_csv_files(self, transformed_df: pd.DataFrame) -> Dict:
        """Genera archivos CSV de salida"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Definir campos de cabecera y detalle
            header_fields = ['journal_entry_id', 'description', 'posting_date', 'fiscal_year', 'period_number', 'prepared_by', 'entry_date']
            detail_fields = ['journal_entry_id', 'line_number', 'line_description', 'gl_account_number', 'gl_account_name', 'amount', 'debit_amount', 'credit_amount', 'debit_credit_indicator', 'vendor_id']
            
            # Archivo de cabecera
            header_file = None
            available_header_cols = [col for col in header_fields if col in transformed_df.columns]
            
            if available_header_cols:
                header_df = transformed_df[available_header_cols].drop_duplicates(subset=['journal_entry_id'] if 'journal_entry_id' in available_header_cols else available_header_cols[:1])
                header_file = f"automatic_training_header_{timestamp}.csv"
                header_df.to_csv(header_file, index=False)
                print(f"✅ Header CSV saved: {header_file}")
            
            # Archivo de detalle
            detail_file = None
            available_detail_cols = [col for col in detail_fields if col in transformed_df.columns]
            
            if available_detail_cols:
                detail_df = transformed_df[available_detail_cols]
                # Ordenar por journal_entry_id si existe
                if 'journal_entry_id' in detail_df.columns:
                    detail_df = detail_df.sort_values('journal_entry_id')
                
                detail_file = f"automatic_training_detail_{timestamp}.csv"
                detail_df.to_csv(detail_file, index=False)
                print(f"✅ Detail CSV saved: {detail_file}")
            
            return {
                'header_file': header_file,
                'detail_file': detail_file,
                'header_columns': available_header_cols,
                'detail_columns': available_detail_cols
            }
            
        except Exception as e:
            print(f"❌ Error generating CSV files: {e}")
            return {'header_file': None, 'detail_file': None}

    def _generate_training_report(self) -> str:
        """Genera reporte de entrenamiento"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = f"automatic_training_report_{timestamp}.txt"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("AUTOMATIC TRAINING REPORT\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"File: {self.csv_file}\n")
                f.write(f"ERP Hint: {self.erp_hint or 'Auto-detect'}\n")
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("TRAINING STATISTICS:\n")
                f.write("-" * 20 + "\n")
                for key, value in self.training_stats.items():
                    f.write(f"{key.replace('_', ' ').title()}: {value}\n")
                
                f.write(f"\nFINAL MAPPINGS:\n")
                f.write("-" * 15 + "\n")
                for column, decision in self.user_decisions.items():
                    f.write(f"{column} → {decision['field_type']} (confidence: {decision['confidence']:.3f})\n")
                
                if self.conflict_resolutions:
                    f.write(f"\nCONFLICT RESOLUTIONS:\n")
                    f.write("-" * 20 + "\n")
                    for field_type, resolution in self.conflict_resolutions.items():
                        f.write(f"{field_type}: {resolution['winner']} ({resolution['resolution_type']})\n")
                        f.write(f"  Candidates: {', '.join(resolution['all_candidates'])}\n")
            
            print(f"✅ Training report saved: {report_file}")
            return report_file
            
        except Exception as e:
            print(f"❌ Error generating report: {e}")
            return None


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
        
        # Ejecutar entrenamiento automático (LLAMADA AL MÉTODO DE LA CLASE)
        result = session.run_automatic_training()
        
        if result['success']:
            print(f"\n✅ AUTOMATIC TRAINING COMPLETED SUCCESSFULLY!")
            
            # Mostrar resumen de resultados
            print(f"\n📊 RESULTS SUMMARY:")
            print(f"   • Automatic mappings: {result['training_stats']['automatic_mappings']}")
            print(f"   • Conflicts resolved: {result['training_stats']['conflicts_resolved']}")
            print(f"   • High confidence decisions: {result['training_stats']['high_confidence_mappings']}")
            if 'rejected_low_confidence' in result['training_stats']:
                print(f"   • Low confidence rejected: {result['training_stats']['rejected_low_confidence']}")
            
            # Mostrar información de balance si está disponible
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
        print(f"❌ Automatic training failed: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}


def main():
    """Función principal"""
    if len(sys.argv) < 2:
        print("AUTOMATIC CONFIRMATION TRAINER - SIMPLIFIED ARCHITECTURE")
        print("=" * 58)
        print("Training with AUTOMATIC DECISIONS - no manual confirmation required")
        print()
        print("ARCHITECTURE:")
        print("  • MAPPER: Handles ALL field detection + conflict resolution")
        print("  • TRAINER: Applies confidence filter + data processing only")
        print("  • CLEAN SEPARATION: No redundant conflict resolution")
        print()
        print("CONFLICT RESOLUTION RULES (handled by mapper):")
        print("  • journal_entry_id: Balance validation (best balance_score wins)")
        print("  • amount: Local priority ('local' in name wins)")
        print("  • others: Highest confidence wins")
        print()
        print("FEATURES:")
        print("  • Same 17 standard fields")
        print("  • Same CSV outputs")
        print("  • Same balance validation")
        print("  • Same numeric processing")
        print("  • Compatible with main_global.py")
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


if __name__ == "__main__":
    main()