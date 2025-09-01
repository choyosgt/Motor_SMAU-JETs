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
        """Realiza detección automática de campos usando el field_mapper"""
        try:
            print(f"🔍 AUTOMATIC FIELD DETECTION")
            print(f"-" * 30)
            
            mappings = {}
            self.training_stats['columns_processed'] = len(self.df.columns)
            
            for column in self.df.columns:
                print(f"\nAnalyzing column: '{column}'")
                
                # Obtener datos de muestra para el análisis
                sample_data = self.df[column].dropna().head(100)
                
                # Usar find_field_mapping del mapper (que ahora tiene balance validation)
                mapping_result = self.mapper.find_field_mapping(
                    field_name=column,
                    erp_system=self.erp_hint,
                    sample_data=sample_data
                )
                
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
                print(f"   ✅ {field_type}: {column} (no conflict)")
            
            else:
                # Conflicto detectado - resolver automáticamente
                print(f"   ⚠️ CONFLICT - {field_type}: {len(candidates)} candidates")
                for col, conf in candidates:
                    print(f"      - {col}: {conf:.3f}")
                
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
                
                if field_type == 'amount':
                    self.training_stats['amount_conflicts_resolved'] += 1
                    
                print(f"      🏆 WINNER: {winner_column} ({resolution_type})")
        
        return final_mappings

    def _resolve_field_conflict(self, field_type: str, candidates: List[Tuple[str, float]]) -> Tuple[str, float, str]:
        """Resuelve conflicto para un field_type específico usando reglas automáticas"""
        
        # REGLA ESPECIAL para 'amount': priorizar columnas 'local'
        if field_type == 'amount':
            for column, confidence in candidates:
                if 'local' in column.lower():
                    print(f"      💡 AMOUNT SPECIAL RULE: '{column}' selected (contains 'local')")
                    return (column, confidence, 'amount_local_priority')
        
        # REGLA ESPECIAL para 'journal_entry_id': 
        # Si balance validation está enabled, ya se resolvió en el field_mapper
        # Solo llegamos aquí si no se pudo resolver con balance validation
        if field_type == 'journal_entry_id':
            # Verificar si hubo balance validation wins
            balance_wins = self.training_stats.get('balance_validation_wins', 0)
            if balance_wins > 0:
                print(f"      🏆 Balance validation already resolved this conflict")
        
        # REGLA GENERAL: mayor confianza gana
        candidates_sorted = sorted(candidates, key=lambda x: x[1], reverse=True)
        winner_column, winner_confidence = candidates_sorted[0]
        
        return (winner_column, winner_confidence, 'highest_confidence')

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


def run_automatic_training(csv_file: str, erp_hint: str = None) -> Dict:
    """Función principal para ejecutar entrenamiento automático"""
    try:
        print(f"🤖 AUTOMATIC CONFIRMATION TRAINER - SIMPLE VERSION")
        print(f"=" * 55)
        print(f"Training with balance validation for journal_entry_id conflicts")
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
            print(f"\n📊 RESULTS SUMMARY:")
            print(f"   • Automatic mappings: {result['training_stats']['automatic_mappings']}")
            print(f"   • Conflicts resolved: {result['training_stats']['conflicts_resolved']}")
            print(f"   • High confidence decisions: {result['training_stats']['high_confidence_mappings']}")
            print(f"   • Low confidence rejected: {result['training_stats']['rejected_low_confidence']}")
            print(f"   • Balance validation: {'✅ ENABLED' if result['training_stats']['balance_validation_enabled'] else '❌ DISABLED'}")
            
            if result['training_stats']['balance_validation_enabled']:
                print(f"   • Balance validation wins: {result['training_stats'].get('balance_validation_wins', 0)}")
            
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
    """Función principal"""
    if len(sys.argv) < 2:
        print("AUTOMATIC CONFIRMATION TRAINER - SIMPLE VERSION WITH BALANCE VALIDATION")
        print("=" * 70)
        print("Usage:")
        print("  python automatic_confirmation_trainer.py <csv_file> [erp_hint]")
        print()
        print("Examples:")
        print("  python automatic_confirmation_trainer.py data/journal.csv")
        print("  python automatic_confirmation_trainer.py data/journal.csv SAP")
        print("  python automatic_confirmation_trainer.py data/journal.csv Navision")
        print()
        print("Features:")
        print("  ✅ Automatic field detection and mapping")
        print("  ✅ Balance validation for journal_entry_id conflicts")
        print("  ✅ Confidence-based filtering (threshold: 0.75)")
        print("  ✅ Special rules for 'amount' field (prioritizes 'local')")
        print("  ✅ Automatic conflict resolution")
        print("  ✅ CSV output generation (header + detail)")
        print("  ✅ Comprehensive training reports")
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