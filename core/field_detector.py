# core/field_detector.py - DETECTOR MEJORADO CON APRENDIZAJE DE PATRONES

import pandas as pd
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import re

try:
    from .field_mapper import EnhancedFieldMapper
    from .dynamic_field_loader import DynamicFieldLoader
    from ..config.custom_field_validators import validator_registry
except ImportError:
    # Fallback para casos donde los imports relativos no funcionan
    try:
        from field_mapper import EnhancedFieldMapper
        from dynamic_field_loader import DynamicFieldLoader
        from config.custom_field_validators import validator_registry
    except ImportError:
        print("⚠️ Warning: Could not import some modules. Some features may not work.")
        EnhancedFieldMapper = None
        DynamicFieldLoader = None
        validator_registry = None

logger = logging.getLogger(__name__)

class EnhancedFieldDetector:
    """
    Detector de campos mejorado con:
    - Aprendizaje de patrones de contenido
    - Validación avanzada de tipos de datos
    - Corrección automática de detecciones erróneas
    - Sistema de retroalimentación y mejora continua
    """
    
    def __init__(self, config_source: str = None, use_content_validation: bool = True, 
                 confidence_thresholds: Dict = None):
        """
        Inicializa el detector mejorado
        
        Args:
            config_source: Fuente de configuración
            use_content_validation: Si usar validación de contenido
            confidence_thresholds: Umbrales de confianza personalizados
        """
        
        # Verificar dependencias
        if not pd:
            raise ImportError("pandas is required. Install with: pip install pandas")
        
        # Componentes principales
        self.field_mapper = EnhancedFieldMapper(config_source) if EnhancedFieldMapper else None
        self.field_loader = self.field_mapper.field_loader if self.field_mapper else None
        self.use_content_validation = use_content_validation
        
        # Configuración de umbrales mejorada
        self.confidence_thresholds = confidence_thresholds or {
            'exact_match': 0.95,        # Coincidencia exacta
            'partial_match': 0.75,      # Coincidencia parcial
            'semantic_match': 0.55,     # Similitud semántica
            'pattern_match': 0.45,      # Coincidencia por patrón
            'content_validation': 0.35,  # Validación de contenido
            'learned_pattern': 0.65,    # Patrón aprendido
            'correction_threshold': 0.3  # Umbral para aplicar correcciones
        }
        
        # Cache para optimización
        self._similarity_cache = {}
        self._erp_detection_cache = {}
        self._content_validation_cache = {}
        self._pattern_analysis_cache = {}
        
        # Estadísticas de detección mejoradas
        self.detection_stats = {
            'total_detections': 0,
            'successful_detections': 0,
            'cache_hits': 0,
            'content_validations': 0,
            'pattern_learnings': 0,
            'automatic_corrections': 0,
            'erp_auto_detections': 0,
            'confidence_improvements': 0,
            'journal_id_conflicts_resolved': 0
        }
        
        # Registro de correcciones automáticas
        self.auto_corrections = []
        
        # Sistema de retroalimentación
        self.feedback_system = {
            'successful_patterns': defaultdict(list),
            'failed_patterns': defaultdict(list),
            'user_corrections': defaultdict(list)
        }
        
        print(f"✓ EnhancedFieldDetector initialized with advanced pattern learning")
    
    def detect_fields(self, df: pd.DataFrame, erp_hint: str = None, 
                     content_analysis: bool = None, learning_mode: bool = True) -> Dict:
        """
        Detecta campos en un DataFrame con aprendizaje de patrones mejorado
        
        Args:
            df: DataFrame a analizar
            erp_hint: Sugerencia del sistema ERP
            content_analysis: Si usar análisis de contenido
            learning_mode: Si está en modo aprendizaje
        
        Returns:
            Diccionario con candidatos y estadísticas
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
        
        start_time = time.time()
        
        try:
            self.detection_stats['total_detections'] += 1
            
            # Usar configuración por defecto si no se especifica
            if content_analysis is None:
                content_analysis = self.use_content_validation
            
            # Auto-detectar ERP si no se proporciona
            if not erp_hint:
                erp_hint = self.auto_detect_erp(df)
                if erp_hint:
                    self.detection_stats['erp_auto_detections'] += 1
            
            print(f"🔍 Enhanced Field Detection Started")
            print(f"   📊 DataFrame: {len(df)} rows, {len(df.columns)} columns")
            print(f"   🏢 ERP: {erp_hint or 'Generic'}")
            print(f"   🧠 Content Analysis: {'✅' if content_analysis else '❌'}")
            print(f"   📚 Learning Mode: {'✅' if learning_mode else '❌'}")
            
            # Análisis inicial de tipos de datos
            data_types_analysis = self._analyze_data_types(df)
            
            # Detección principal con análisis mejorado
            candidates = {}
            confidence_scores = {}
            
            for column_name in df.columns:
                print(f"\n🔍 Analyzing column: '{column_name}'")
                
                # Obtener datos de muestra
                column_data = df[column_name]
                sample_data = column_data.dropna().head(20)
                
                if len(sample_data) == 0:
                    print(f"   ⚠️ No data available")
                    continue
                
                # Análisis multi-nivel mejorado
                column_candidates = self._analyze_column_enhanced(
                    column_name, column_data, erp_hint, content_analysis, learning_mode
                )
                
                if column_candidates:
                    # Aplicar correcciones automáticas si es necesario
                    corrected_candidates = self._apply_smart_corrections(
                        column_candidates, sample_data, column_name
                    )
                    
                    if corrected_candidates != column_candidates:
                        self.detection_stats['automatic_corrections'] += 1
                        self.auto_corrections.append({
                            'column': column_name,
                            'original': column_candidates[0]['field_type'],
                            'corrected': corrected_candidates[0]['field_type'],
                            'reason': 'content_validation_failed'
                        })
                        print(f"   🔧 Auto-correction applied")
                    
                    # Organizar candidatos por tipo de campo
                    for candidate in corrected_candidates:
                        field_type = candidate['field_type']
                        if field_type not in candidates:
                            candidates[field_type] = []
                        candidates[field_type].append(candidate)
                        
                        # Guardar score de confianza
                        if field_type not in confidence_scores:
                            confidence_scores[field_type] = []
                        confidence_scores[field_type].append(candidate['confidence'])
                else:
                    print(f"   ❌ No candidates found")
            
            # Calcular métricas de calidad
            quality_metrics = self._calculate_quality_metrics(candidates, df)
            print(f"\n🔍 Checking for journal_entry_id conflicts...")

            original_journal_candidates = len(candidates.get('journal_entry_id', []))

            if original_journal_candidates > 1:
                print(f"   Found {original_journal_candidates} journal_entry_id candidates - resolving...")
                candidates = self._resolve_journal_entry_id_conflicts(candidates, df)
                
                final_journal_candidates = len(candidates.get('journal_entry_id', []))
                if final_journal_candidates < original_journal_candidates:
                    print(f"   ✅ Conflict resolved: {original_journal_candidates} → {final_journal_candidates} candidates")
                else:
                    print(f"   ⚠️ Conflict remains: keeping {final_journal_candidates} candidates")
                
                # Recalcular quality_metrics después de resolución
                quality_metrics = self._calculate_quality_metrics(candidates, df)
                
                quality_metrics['journal_id_conflict_resolution'] = {
                    'original_candidates': original_journal_candidates,
                    'final_candidates': len(candidates.get('journal_entry_id', [])),
                    'conflict_resolved': original_journal_candidates > 1 and len(candidates.get('journal_entry_id', [])) <= 1
                }
            else:
                print(f"   No journal_entry_id conflicts detected")

            detection_time = time.time() - start_time
            self.detection_stats['successful_detections'] += 1
            
            # Resultado mejorado
            result = {
                'candidates': candidates,
                'confidence_scores': confidence_scores,
                'erp_detected': erp_hint,
                'detection_time': detection_time,
                'total_columns': len(df.columns),
                'data_types_analysis': data_types_analysis,
                'quality_metrics': quality_metrics,
                'auto_corrections': self.auto_corrections.copy(),
                'learning_enabled': learning_mode,
                'content_analysis_used': content_analysis
            }
            
            print(f"\n🎯 Detection completed in {detection_time:.3f}s")
            print(f"   ✅ Candidates found: {len(candidates)}")
            print(f"   🔧 Auto-corrections: {len(self.auto_corrections)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in enhanced field detection: {e}")
            return {
                'error': str(e),
                'candidates': {},
                'total_columns': len(df.columns) if isinstance(df, pd.DataFrame) else 0
            }
    
    def _analyze_data_types(self, df: pd.DataFrame) -> Dict:
        """Analiza los tipos de datos del DataFrame"""
        analysis = {
            'column_types': {},
            'null_percentages': {},
            'unique_value_counts': {},
            'sample_values': {}
        }
        
        for column in df.columns:
            # Tipo de datos pandas
            analysis['column_types'][column] = str(df[column].dtype)
            
            # Porcentaje de nulos
            null_pct = (df[column].isnull().sum() / len(df)) * 100
            analysis['null_percentages'][column] = round(null_pct, 2)
            
            # Valores únicos
            analysis['unique_value_counts'][column] = df[column].nunique()
            
            # Valores de muestra
            sample_values = df[column].dropna().head(5).tolist()
            analysis['sample_values'][column] = [str(val) for val in sample_values]
        
        return analysis
    
    def _analyze_column_enhanced(self, column_name: str, column_data: pd.Series, 
                               erp_hint: str = None, content_analysis: bool = True,
                               learning_mode: bool = True) -> List[Dict]:
        """Análisis mejorado de una columna específica"""
        candidates = []
        
        if not self.field_mapper:
            return candidates
        
        # Obtener datos de muestra
        sample_data = column_data.dropna().head(20)
        
        # 1. Análisis con field_mapper mejorado
        mapping_result = self.field_mapper.find_field_mapping(
            column_name, erp_hint, sample_data
        )
        
        if mapping_result:
            field_type, confidence = mapping_result
            candidates.append({
                'field_type': field_type,
                'field_name': self._get_field_display_name(field_type),
                'column_name': column_name,
                'confidence': confidence,
                'source': 'enhanced_mapper',
                'data_type': self._infer_data_type(sample_data),
                'erp_system': erp_hint or 'Generic'
            })
            print(f"   🎯 Enhanced mapper: {field_type} ({confidence:.3f})")
        
        # 2. Validación con validadores especializados (si está habilitado)
        if content_analysis and validator_registry:
            validator_candidates = self._analyze_with_validators(
                column_name, sample_data, erp_hint
            )
            
            # Combinar con candidatos del mapper
            for val_candidate in validator_candidates:
                # Verificar si ya existe este tipo de campo
                existing = next((c for c in candidates if c['field_type'] == val_candidate['field_type']), None)
                if existing:
                    # Promediar confianzas si ya existe
                    existing['confidence'] = (existing['confidence'] + val_candidate['confidence']) / 2
                    existing['source'] = 'enhanced_mapper+validator'
                else:
                    candidates.append(val_candidate)
            
            self.detection_stats['content_validations'] += 1
        
        # 3. Análisis de patrones aprendidos (si está en modo aprendizaje)
        if learning_mode and self.field_mapper.pattern_learner:
            pattern_candidates = self._analyze_with_learned_patterns(
                column_name, sample_data, erp_hint
            )
            
            # Combinar con candidatos existentes
            for pat_candidate in pattern_candidates:
                existing = next((c for c in candidates if c['field_type'] == pat_candidate['field_type']), None)
                if existing:
                    # Dar más peso a patrones aprendidos si la confianza es alta
                    if pat_candidate['confidence'] > 0.7:
                        existing['confidence'] = max(existing['confidence'], pat_candidate['confidence'])
                        existing['source'] += '+learned_pattern'
                else:
                    candidates.append(pat_candidate)
            
            self.detection_stats['pattern_learnings'] += 1
        
        # Ordenar candidatos por confianza
        candidates.sort(key=lambda x: x['confidence'], reverse=True)
        
        # Limitar a top 3 candidatos
        return candidates[:3]
    
    def _analyze_with_validators(self, column_name: str, sample_data: pd.Series, 
                               erp_hint: str = None) -> List[Dict]:
        """Análisis usando validadores especializados"""
        candidates = []
        
        if not validator_registry:
            return candidates
        
        # Probar todos los validadores disponibles
        for field_type in validator_registry.validators.keys():
            try:
                validation_score = validator_registry.validate_field(field_type, sample_data)
                
                if validation_score > self.confidence_thresholds['content_validation']:
                    candidates.append({
                        'field_type': field_type,
                        'field_name': self._get_field_display_name(field_type),
                        'column_name': column_name,
                        'confidence': validation_score,
                        'source': 'content_validator',
                        'data_type': self._infer_data_type(sample_data),
                        'erp_system': erp_hint or 'Generic'
                    })
                    
                    if validation_score > 0.7:
                        print(f"   ✅ Validator {field_type}: {validation_score:.3f}")
                    
            except Exception as e:
                logger.warning(f"Error in validator {field_type}: {e}")
        
        return candidates
    
    def _analyze_with_learned_patterns(self, column_name: str, sample_data: pd.Series, 
                                     erp_hint: str = None) -> List[Dict]:
        """Análisis usando patrones aprendidos"""
        candidates = []
        
        if not self.field_mapper or not self.field_mapper.pattern_learner:
            return candidates
        
        # Obtener definiciones de campos disponibles
        field_definitions = self.field_mapper.field_loader.get_field_definitions()
        
        for field_type in field_definitions.keys():
            try:
                pattern_score = self.field_mapper.pattern_learner.calculate_pattern_match_score(
                    field_type, sample_data
                )
                
                if pattern_score > self.confidence_thresholds['learned_pattern']:
                    candidates.append({
                        'field_type': field_type,
                        'field_name': self._get_field_display_name(field_type),
                        'column_name': column_name,
                        'confidence': pattern_score,
                        'source': 'learned_pattern',
                        'data_type': self._infer_data_type(sample_data),
                        'erp_system': erp_hint or 'Generic'
                    })
                    
                    if pattern_score > 0.8:
                        print(f"   🧠 Learned pattern {field_type}: {pattern_score:.3f}")
                    
            except Exception as e:
                logger.warning(f"Error in pattern analysis for {field_type}: {e}")
        
        return candidates
    
    def _apply_smart_corrections(self, candidates: List[Dict], sample_data: pd.Series, 
                               column_name: str) -> List[Dict]:
        """Aplica correcciones inteligentes basadas en validación de contenido"""
        if not candidates or not validator_registry:
            return candidates
        
        corrected_candidates = candidates.copy()
        best_candidate = corrected_candidates[0]
        
        # Validar el mejor candidato con validador especializado
        validation_score = validator_registry.validate_field(
            best_candidate['field_type'], sample_data
        )
        
        # Si la validación falla, buscar alternativa
        if validation_score < self.confidence_thresholds['correction_threshold']:
            print(f"   ⚠️ Validation failed for {best_candidate['field_type']} (score: {validation_score:.3f})")
            
            # Buscar mejor alternativa usando validadores
            best_alternative = None
            best_alt_score = 0.0
            
            for field_type in validator_registry.validators.keys():
                if field_type != best_candidate['field_type']:
                    alt_score = validator_registry.validate_field(field_type, sample_data)
                    if alt_score > best_alt_score and alt_score > 0.5:
                        best_alt_score = alt_score
                        best_alternative = field_type
            
            # Si encontramos una mejor alternativa, reemplazar
            if best_alternative:
                print(f"   🔧 Correction: {best_candidate['field_type']} -> {best_alternative} "
                      f"(score improved: {validation_score:.3f} -> {best_alt_score:.3f})")
                
                # Actualizar el mejor candidato
                corrected_candidates[0] = {
                    **best_candidate,
                    'field_type': best_alternative,
                    'field_name': self._get_field_display_name(best_alternative),
                    'confidence': best_alt_score,
                    'source': best_candidate['source'] + '+auto_corrected'
                }
        
        return corrected_candidates
    
    def _calculate_quality_metrics(self, candidates: Dict, df: pd.DataFrame) -> Dict:
        """Calcula métricas de calidad de la detección"""
        metrics = {
            'total_columns': len(df.columns),
            'detected_columns': 0,
            'detection_rate': 0.0,
            'avg_confidence': 0.0,
            'field_type_distribution': {},
            'data_coverage': 0.0
        }
        
        if not candidates:
            return metrics
        
        # Contar columnas detectadas
        detected_columns = set()
        all_confidences = []
        
        for field_type, field_candidates in candidates.items():
            metrics['field_type_distribution'][field_type] = len(field_candidates)
            
            for candidate in field_candidates:
                detected_columns.add(candidate['column_name'])
                all_confidences.append(candidate['confidence'])
        
        # Calcular métricas
        metrics['detected_columns'] = len(detected_columns)
        metrics['detection_rate'] = (len(detected_columns) / len(df.columns)) * 100
        metrics['avg_confidence'] = sum(all_confidences) / len(all_confidences) if all_confidences else 0.0
        
        # Calcular cobertura de datos (porcentaje de filas con datos)
        total_cells = len(df) * len(df.columns)
        non_null_cells = df.count().sum()
        metrics['data_coverage'] = (non_null_cells / total_cells) * 100 if total_cells > 0 else 0.0
        
        return metrics
    
    def _get_field_display_name(self, field_type: str) -> str:
        """Obtiene el nombre de visualización de un tipo de campo"""
        if not self.field_loader:
            return field_type
        
        field_definitions = self.field_loader.get_field_definitions()
        if field_type in field_definitions:
            return field_definitions[field_type].name
        
        return field_type
    
    def _infer_data_type(self, sample_data: pd.Series) -> str:
        """Infiere el tipo de datos de una muestra"""
        if len(sample_data) == 0:
            return 'unknown'
        
        # Verificar si es numérico
        numeric_count = 0
        date_count = 0
        
        for value in sample_data.head(10):
            value_str = str(value).strip()
            
            # Verificar si es fecha
            if self._is_date_like(value_str):
                date_count += 1
            # Verificar si es numérico
            elif self._is_numeric(value_str):
                numeric_count += 1
        
        total_checked = min(10, len(sample_data))
        
        if date_count / total_checked > 0.6:
            return 'date'
        elif numeric_count / total_checked > 0.6:
            return 'numeric'
        else:
            return 'text'
    
    def _is_date_like(self, value: str) -> bool:
        """Verifica si un valor parece una fecha"""
        date_patterns = [
            r'^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$',
            r'^\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}$',
            r'^\d{1,2}-[a-zA-Z]{3}-\d{2,4}$'
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)
    
    def _is_numeric(self, value: str) -> bool:
        """Verifica si un valor es numérico"""
        try:
            clean_value = str(value).replace(',', '.').replace(' ', '')
            if clean_value.startswith('-'):
                clean_value = clean_value[1:]
            float(clean_value)
            return True
        except:
            return False
    def _resolve_journal_entry_id_conflicts(self, candidates, df):
        """Resuelve conflictos de journal_entry_id usando balance testing"""
        journal_id_candidates = candidates.get('journal_entry_id', [])
        
        if len(journal_id_candidates) <= 1:
            return candidates
        
        print(f"\n🎯 RESOLVING journal_entry_id CONFLICT")
        print(f"   Found {len(journal_id_candidates)} candidates")
        
        has_balance_fields = ('debit_amount' in df.columns and 'credit_amount' in df.columns)
        
        if not has_balance_fields:
            print(f"   ⚠️ No balance fields found - keeping original candidates")
            return candidates
        
        candidate_columns = [c['column_name'] for c in journal_id_candidates]
        print(f"   Testing candidates: {candidate_columns}")
        
        try:
            balance_result = self._test_journal_id_candidates_balance(df, candidate_columns)
            
            if balance_result['success']:
                winning_column = balance_result['selected_candidate']
                winner_balance_rate = balance_result.get('confidence', 0.0)
                
                print(f"   ✅ Winner: '{winning_column}' (balance rate: {winner_balance_rate*100:.1f}%)")
                
                updated_candidates = []
                for candidate in journal_id_candidates:
                    if candidate['column_name'] == winning_column:
                        updated_candidate = candidate.copy()
                        updated_candidate['confidence'] = min(1.0, candidate['confidence'] + 0.2)
                        updated_candidate['source'] += '+balance_validated'
                        updated_candidates.append(updated_candidate)
                        
                        self.detection_stats['journal_id_conflicts_resolved'] += 1
                        
                    elif candidate['confidence'] > 0.6:
                        updated_candidate = candidate.copy()
                        updated_candidate['confidence'] = max(0.3, candidate['confidence'] - 0.3)
                        updated_candidate['source'] += '+balance_rejected'
                        updated_candidates.append(updated_candidate)
                
                candidates['journal_entry_id'] = updated_candidates
                
            else:
                print(f"   ❌ Balance testing failed: {balance_result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"   ⚠️ Balance testing error - keeping original candidates")
        
        return candidates

    def _test_journal_id_candidates_balance(self, df, candidate_columns, sample_size=10, min_entries_per_sample=2):
        """Prueba candidatos usando balance de asientos"""
        if not candidate_columns:
            return {'success': False, 'error': 'No candidates provided', 'selected_candidate': None}
        
        results = {}
        
        for candidate in candidate_columns:
            if candidate not in df.columns:
                continue
                
            try:
                result = self._test_single_candidate_balance(df, candidate, sample_size, min_entries_per_sample)
                results[candidate] = result
            except Exception as e:
                results[candidate] = {'valid': False, 'reason': f'Error: {str(e)}', 'balanced_rate': 0.0}
        
        valid_results = {k: v for k, v in results.items() if v.get('valid', False)}
        
        if not valid_results:
            return {'success': False, 'error': 'No valid candidates found', 'selected_candidate': None, 'all_results': results}
        
        best_candidate = max(valid_results.keys(), key=lambda x: valid_results[x]['balanced_rate'])
        
        return {
            'success': True,
            'selected_candidate': best_candidate,
            'confidence': valid_results[best_candidate]['balanced_rate'],
            'all_results': results
        }

    def _test_single_candidate_balance(self, df, candidate_column, sample_size, min_entries_per_sample):
        """Prueba un candidato individual usando balance"""
        try:
            unique_entries = df[candidate_column].dropna().unique()
            
            if len(unique_entries) == 0:
                return {'valid': False, 'reason': 'No unique values found', 'balanced_rate': 0.0}
            
            if len(unique_entries) > sample_size:
                import numpy as np
                np.random.seed(42)
                sample_entries = np.random.choice(unique_entries, size=sample_size, replace=False)
            else:
                sample_entries = unique_entries
            
            balanced_count = 0
            valid_entries_tested = 0
            
            for entry_id in sample_entries:
                entry_lines = df[df[candidate_column] == entry_id]
                
                if len(entry_lines) < min_entries_per_sample:
                    continue
                    
                total_debit = entry_lines['debit_amount'].sum()
                total_credit = entry_lines['credit_amount'].sum()
                balance_difference = abs(total_debit - total_credit)
                
                valid_entries_tested += 1
                
                if balance_difference < 0.01:
                    balanced_count += 1
            
            if valid_entries_tested == 0:
                return {'valid': False, 'reason': f'No entries with {min_entries_per_sample}+ lines found', 'balanced_rate': 0.0}
            
            balanced_rate = balanced_count / valid_entries_tested
            
            return {
                'valid': True,
                'balanced_entries': balanced_count,
                'total_entries': valid_entries_tested,
                'balanced_rate': balanced_rate,
                'unique_values_count': len(unique_entries)
            }
            
        except Exception as e:
            return {'valid': False, 'reason': f'Error during testing: {str(e)}', 'balanced_rate': 0.0}

    def auto_detect_erp(self, df: pd.DataFrame) -> Optional[str]:
        """Auto-detecta el sistema ERP basado en nombres de columnas"""
        if df.empty:
            return None
        
        column_names = [col.lower() for col in df.columns]
        column_names_str = ' '.join(column_names)
        
        # Cache para optimización
        cache_key = str(sorted(column_names))
        if cache_key in self._erp_detection_cache:
            return self._erp_detection_cache[cache_key]
        
        # Patrones de detección ERP mejorados
        erp_patterns = {
            'SAP': [
                'belnr', 'bukrs', 'hkont', 'shkzg', 'dmbtr', 'waers',
                'bldat', 'budat', 'xblnr', 'bschl', 'kostl'
            ],
            'Oracle': [
                'je_header_id', 'je_line_num', 'code_combination_id',
                'entered_dr', 'entered_cr', 'accounted_dr', 'accounted_cr'
            ],
            'Navision': [
                'document_no', 'posting_date', 'g_l_account_no',
                'amount_lcy', 'debit_amount', 'credit_amount'
            ],
            'SAGE': [
                'reference', 'account_code', 'nominal_code',
                'transaction_type', 'net_amount', 'tax_amount'
            ],
            'PeopleSoft': [
                'business_unit', 'journal_id', 'journal_line',
                'account', 'monetary_amount', 'statistics_amount'
            ]
        }
        
        # Calcular scores para cada ERP
        erp_scores = {}
        for erp_name, patterns in erp_patterns.items():
            score = 0
            for pattern in patterns:
                if pattern in column_names_str:
                    score += 1
            
            if score > 0:
                erp_scores[erp_name] = score / len(patterns)
        
        # Seleccionar el ERP con mayor score
        if erp_scores:
            best_erp = max(erp_scores, key=erp_scores.get)
            if erp_scores[best_erp] > 0.3:  # Umbral mínimo
                self._erp_detection_cache[cache_key] = best_erp
                return best_erp
        
        # Fallback a Generic_ES
        self._erp_detection_cache[cache_key] = 'Generic_ES'
        return 'Generic_ES'
    
    def get_available_field_types(self) -> List[str]:
        """Retorna lista de tipos de campo disponibles"""
        if not self.field_loader:
            return []
        
        return list(self.field_loader.get_field_definitions().keys())
    
    def get_detection_stats(self) -> Dict:
        """Retorna estadísticas de detección"""
        stats = self.detection_stats.copy()
        
        if stats['total_detections'] > 0:
            stats['success_rate'] = (stats['successful_detections'] / stats['total_detections']) * 100
            stats['cache_hit_rate'] = (stats['cache_hits'] / stats['total_detections']) * 100
        else:
            stats['success_rate'] = 0.0
            stats['cache_hit_rate'] = 0.0
        
        return stats
    
    def get_detection_summary(self, df: pd.DataFrame = None) -> Dict:
        """Retorna resumen de la última detección"""
        summary = {
            'detection_stats': self.get_detection_stats(),
            'auto_corrections': len(self.auto_corrections),
            'available_field_types': len(self.get_available_field_types()),
            'cache_size': len(self._similarity_cache),
            'learning_enabled': bool(self.field_mapper and self.field_mapper.pattern_learner)
        }
        
        if df is not None:
            summary['dataframe_info'] = {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_usage': df.memory_usage(deep=True).sum()
            }
        
        return summary
    
    def clear_cache(self):
        """Limpia todos los caches para liberar memoria"""
        self._similarity_cache.clear()
        self._erp_detection_cache.clear()
        self._content_validation_cache.clear()
        self._pattern_analysis_cache.clear()
        
        if self.field_mapper:
            self.field_mapper.clear_cache()
        
        print("✓ All detection caches cleared")
    
    def export_learned_patterns(self, output_file: str = None) -> str:
        """Exporta patrones aprendidos a archivo JSON"""
        if not output_file:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_file = f"learned_patterns_export_{timestamp}.json"
        
        export_data = {
            'detection_stats': self.detection_stats,
            'auto_corrections': self.auto_corrections,
            'feedback_system': dict(self.feedback_system),
            'confidence_thresholds': self.confidence_thresholds
        }
        
        if self.field_mapper and self.field_mapper.pattern_learner:
            export_data['learned_patterns'] = self.field_mapper.pattern_learner.learned_patterns
        
        import json
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        print(f"📤 Learned patterns exported to: {output_file}")
        return output_file


# Mantener compatibilidad con el código existente
FieldDetector = EnhancedFieldDetector