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

        
        # CAMPOS ESTÁNDAR
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
            'balance_validation_enabled': False,
            'balance_validation_wins': 0
        }
        
        # Umbral de confianza mínimo
        self.confidence_threshold = 0.75
        
        # Decisiones automáticas registradas
        self.user_decisions = {}
        self.learned_patterns = {}
        self.conflict_resolutions = {}
        
        # Archivos de configuración
        self.yaml_config_file = "config/pattern_learning_config.yaml"
        self.dynamic_fields_file = "config/dynamic_fields_config.yaml"
        
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

    def run_automatic_training(self) -> Dict:
        """Ejecuta el entrenamiento automático completo"""
        try:
            print(f"\n🤖 Starting AUTOMATIC TRAINING...")
            print(f"=" * 50)
            
            # 1. Análisis inicial del DataFrame
            self._show_initial_analysis()
            
            # 2. Detección automática de campos
            field_analysis = self._perform_automatic_field_detection()
            if not field_analysis['success']:
                return field_analysis
            
            # 3. Resolver conflictos automáticamente
            final_mappings = self._resolve_conflicts_automatically(field_analysis['mappings'])
            
            # 4. Aplicar filtro de confianza
            filtered_mappings = self._apply_confidence_filter(final_mappings)
            
            # 5. Actualizar decisiones de usuario
            self._update_user_decisions_from_mappings(filtered_mappings)
            
            # 6. Finalizar entrenamiento
            result = self._finalize_automatic_training()
            
            return result
            
        except Exception as e:
            logger.error(f"Error in automatic training: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}
    
    def _show_initial_analysis(self):
        """Muestra análisis inicial del CSV"""
        print(f"\n📊 CSV ANALYSIS:")
        print(f"Columns ({len(self.df.columns)}):")
        for i, col in enumerate(self.df.columns, 1):
            sample_data = self.df[col].dropna().head(3).tolist()
            print(f"  {i:2d}. {col} → {sample_data}")
        print()

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
        """Finaliza el entrenamiento automático"""
        try:
            print(f"\n🏁 FINALIZING AUTOMATIC TRAINING")
            print(f"=" * 40)
            
            # Crear DataFrame transformado
            transformed_df = self.df.copy()
            column_mapping = {col: decision['field_type'] for col, decision in self.user_decisions.items()}
            transformed_df = transformed_df.rename(columns=column_mapping)
            
            # Generar archivos CSV
            csv_result = self._generate_csv_files(transformed_df)
            
            # Generar reporte
            report_file = self._generate_training_report()
            
            # Preparar resultado final
            result = {
                'success': True,
                'training_stats': self.training_stats,
                'user_decisions': self.user_decisions,
                'conflict_resolutions': self.conflict_resolutions,
                'report_file': report_file,
                **csv_result
            }
            
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