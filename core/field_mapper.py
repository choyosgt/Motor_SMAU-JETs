# core/field_mapper.py
"""
Mapeador de campos mejorado con lógica avanzada de detección y MAPEO ÚNICO
ACTUALIZADO: Nuevos campos gl_account_name y vendor_id, nombres de campos actualizados
"""

import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any
from datetime import datetime
import logging
from collections import Counter


# Import local con manejo de errores mejorado
try:
    from .dynamic_field_loader import DynamicFieldLoader
    from .dynamic_field_definition import DynamicFieldDefinition
except ImportError:
    # Fallback para desarrollo en Spyder
    import sys
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))
    
    try:
        from dynamic_field_loader import DynamicFieldLoader
        from dynamic_field_definition import DynamicFieldDefinition
    except ImportError as e:
        print(f"⚠️ Warning: Could not import required modules: {e}")
        print("Creating minimal fallback classes...")
        
        # Crear clases fallback mínimas para permitir ejecución
        class DynamicFieldLoader:
            def __init__(self, config_source=None):
                self.field_definitions = {}
            def get_field_definitions(self):
                return {}
            def get_field_definition(self, field_type):
                return None
            def get_statistics(self):
                return {'total_fields': 0}
                
        class DynamicFieldDefinition:
            def __init__(self, code, **kwargs):
                self.code = code
            def get_synonyms_for_erp(self, erp):
                return []
            def get_all_synonyms(self):
                return []

logger = logging.getLogger(__name__)

class FieldMapper:
    """
    Mapeador de campos mejorado con lógica avanzada de detección
    ACTUALIZADO: Nuevos campos gl_account_name y vendor_id, nombres de campos actualizados
    """
    
    def __init__(self, config_source: Union[str, Path] = None):
        self.config_source = config_source
        self.field_loader = DynamicFieldLoader(config_source)
        
        # Cache para optimización
        self._normalization_cache = {}
        self._mapping_cache = {}
        self._erp_synonyms_cache = {}
        self._content_analysis_cache = {}

        self._dataframe_for_balance = None
        self._balance_validator = None
        self._numeric_fields_prepared = False

        try:
            from balance_validator import BalanceValidator
            self._balance_validator = BalanceValidator(tolerance=0.01)
        except ImportError:
            print("⚠️ BalanceValidator not available - journal_entry_id balance validation disabled")
            self._balance_validator = None
        
        # MEJORADO: Control de mapeo único más inteligente
        self._used_field_mappings = {}  # {field_type: column_name}
        self._column_mappings = {}      # {column_name: field_type}
        self._confidence_by_column = {} # {column_name: confidence}
        
        # Estadísticas de uso
        self.mapping_stats = {
            'total_mappings_requested': 0,
            'cache_hits': 0,
            'successful_mappings': 0,
            'failed_mappings': 0,
            'conflicts_resolved': 0,
            'content_analysis_used': 0,
            'unique_mapping_conflicts': 0,
            'header_forced_mappings': 0,
            'smart_reassignments': 0  # NUEVO
        }
        
        # Configuración de normalización
        self.accent_map = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ü': 'u',
            'ñ': 'n', 'ç': 'c', 'à': 'a', 'è': 'e', 'ì': 'i', 'ò': 'o', 'ù': 'u'
        }
        
        # Traducciones básicas para idiomas comunes
        self.translation_map = {
            # Alemán
            'datum': 'fecha', 'betrag': 'importe', 'konto': 'cuenta', 'soll': 'debe', 'haben': 'haber',
            'kostenstelle': 'centro_coste', 'projekt': 'proyecto', 'waehrung': 'moneda',
            'buchung': 'asiento', 'beleg': 'documento', 'periode': 'periodo',
            'lieferant': 'proveedor', 'kontoname': 'nombre_cuenta',
            
            # Francés
            'date': 'fecha', 'montant': 'importe', 'compte': 'cuenta', 'debit': 'debe', 'credit': 'haber',
            'centre': 'centro', 'projet': 'proyecto', 'devise': 'moneda',
            'ecriture': 'asiento', 'document': 'documento', 'periode': 'periodo',
            'fournisseur': 'proveedor', 'nomcompte': 'nombre_cuenta',
            
            # Italiano
            'data': 'fecha', 'importo': 'importe', 'conto': 'cuenta', 'dare': 'debe', 'avere': 'haber',
            'centro': 'centro', 'progetto': 'proyecto', 'valuta': 'moneda',
            'scrittura': 'asiento', 'documento': 'documento', 'periodo': 'periodo',
            'fornitore': 'proveedor', 'nomeconto': 'nombre_cuenta',
            
            # Portugués
            'data': 'fecha', 'valor': 'importe', 'conta': 'cuenta', 'debito': 'debe', 'credito': 'haber',
            'centro': 'centro', 'projeto': 'proyecto', 'moeda': 'moneda',
            'lancamento': 'asiento', 'documento': 'documento', 'periodo': 'periodo',
            'fornecedor': 'proveedor', 'nomeconta': 'nombre_cuenta'
        }
        
        print(f"✓ Enhanced FieldMapper (UPDATED) initialized with {len(self.get_all_field_types())} field types")
    
    def reload_and_update(self, force: bool = False) -> bool:
        """Recarga configuración y actualiza mapeos"""
        if self.field_loader.reload_configuration(force):
            self._clear_caches()
            print("✓ Field mappings updated")
            return True
        return False
    
    def reset_mappings(self):
        """Resetea todos los mapeos únicos"""
        self._used_field_mappings.clear()
        self._column_mappings.clear()
        self._confidence_by_column.clear()
        self.mapping_stats['unique_mapping_conflicts'] = 0
        self.mapping_stats['header_forced_mappings'] = 0
        self.mapping_stats['smart_reassignments'] = 0
        print("✓ Unique mappings reset")
    
    def get_all_field_synonyms(self, field_type: str, erp_system: str = None) -> List[str]:
        """Obtiene sinónimos combinando todas las fuentes"""
        cache_key = f"{field_type}_{erp_system or 'all'}"
        
        if cache_key in self._erp_synonyms_cache:
            self.mapping_stats['cache_hits'] += 1
            return self._erp_synonyms_cache[cache_key]
        
        synonyms = []
        field_def = self.field_loader.get_field_definition(field_type)
        
        if field_def:
            if erp_system:
                synonyms = field_def.get_synonyms_for_erp(erp_system)
            else:
                synonyms = field_def.get_all_synonyms()
        
        # Cache del resultado
        self._erp_synonyms_cache[cache_key] = synonyms
        return synonyms
    def set_dataframe_for_balance_validation(self, df: pd.DataFrame):
        """Configura el DataFrame completo para poder hacer balance validation en journal_entry_id conflicts"""
        self._dataframe_for_balance = df.copy()
        self._numeric_fields_prepared = False
        
        print(f"🗃️ DataFrame set for balance validation: {df.shape[0]} rows, {df.shape[1]} columns")
    
    def find_field_mapping(self, field_name: str, erp_system: str = None, 
                          sample_data: pd.Series = None) -> Optional[Tuple[str, float]]:
        """
        Busca mapeo mejorado con análisis de contenido y MAPEO ÚNICO INTELIGENTE
        ACTUALIZADO: Nuevos campos y nombres actualizados
        """
        self.mapping_stats['total_mappings_requested'] += 1
        
        # REGLA ESPECIAL: Si la descripción contiene "Cabecera" o "header", forzar description
        field_name_lower = field_name.lower()
        if ('cabecera' in field_name_lower or 'header' in field_name_lower) and 'description' in field_name_lower:
            if 'description' not in self._used_field_mappings:
                self._used_field_mappings['description'] = field_name
                self._column_mappings[field_name] = 'description'
                self._confidence_by_column[field_name] = 0.95
                self.mapping_stats['header_forced_mappings'] += 1
                self.mapping_stats['successful_mappings'] += 1
                print(f"🎯 FORCED MAPPING: '{field_name}' -> description (contains 'Cabecera'/'header')")
                return ('description', 0.95)
            else:
                print(f"⚠️ description already mapped to '{self._used_field_mappings['description']}'")
        
        # Normalizar nombre de campo
        normalized_name = self._normalize_field_name(field_name)
        
        # Intentar traducir si parece ser otro idioma
        translated_name = self._try_translate_field_name(field_name)
        if translated_name != field_name:
            logger.debug(f"Translated '{field_name}' to '{translated_name}'")
        
        # MEJORADO: Análisis de contenido ANTES de buscar coincidencias
        content_analysis = self._enhanced_content_analysis(field_name, sample_data) if sample_data is not None else {}
        
        # Buscar coincidencias exactas primero
        exact_matches = self._find_exact_matches(field_name, erp_system)
        
        # MEJORADO: Filtrar y evaluar coincidencias con contenido
        best_match = self._find_best_match_with_content(field_name, exact_matches, content_analysis, sample_data)
        
        if best_match:
            field_type, confidence = best_match
            
            # MEJORADO: Verificar si hay conflicto y resolverlo inteligentemente
            conflict_resolution = self._resolve_mapping_conflict(field_name, field_type, confidence, sample_data)
            
            if conflict_resolution:
                final_field_type, final_confidence = conflict_resolution
                
                # Registrar mapeo único
                self._used_field_mappings[final_field_type] = field_name
                self._column_mappings[field_name] = final_field_type
                self._confidence_by_column[field_name] = final_confidence
                
                self.mapping_stats['successful_mappings'] += 1
                return (final_field_type, final_confidence)
        
        # No se pudo mapear
        self.mapping_stats['failed_mappings'] += 1
        return None
    
    def _enhanced_content_analysis(self, field_name: str, sample_data: pd.Series) -> Dict[str, float]:
        """MEJORADO: Análisis de contenido más preciso con nuevos campos"""
        if sample_data is None or len(sample_data) == 0:
            return {}
        
        analysis = {}
        clean_data = sample_data.dropna()
        
        if len(clean_data) == 0:
            return {}
        
        # Convertir a string para análisis general
        str_data = clean_data.astype(str)
        
        # 1. ANÁLISIS NUMÉRICO MEJORADO
        numeric_analysis = self._analyze_numeric_content(clean_data)
        analysis.update(numeric_analysis)
        
        # 2. ANÁLISIS DE TEXTO MEJORADO
        text_analysis = self._analyze_text_content(str_data, field_name)
        analysis.update(text_analysis)
        
        # 3. ANÁLISIS DE FECHAS MEJORADO
        date_analysis = self._analyze_date_content_improved(str_data)
        analysis.update(date_analysis)
        
        # 4. ANÁLISIS DE PATRONES ESPECÍFICOS
        pattern_analysis = self._analyze_field_patterns(field_name, clean_data)
        analysis.update(pattern_analysis)
        
        # 5. NUEVO: ANÁLISIS DE VENDOR_ID
        vendor_analysis = self._analyze_vendor_id_content(field_name, str_data)
        analysis.update(vendor_analysis)
        
        # 6. NUEVO: ANÁLISIS DE GL_ACCOUNT_NAME
        account_name_analysis = self._analyze_gl_account_name_content(field_name, str_data)
        analysis.update(account_name_analysis)
        
        return analysis
    
    def _analyze_numeric_content(self, data: pd.Series) -> Dict[str, float]:
        """Análisis numérico mejorado con nombres actualizados"""
        analysis = {}
        
        try:
            # Intentar conversión numérica
            numeric_data = pd.to_numeric(data, errors='coerce')
            non_null_numeric = numeric_data.dropna()
            
            if len(non_null_numeric) == 0:
                return analysis
            
            numeric_ratio = len(non_null_numeric) / len(data)
            
            if numeric_ratio < 0.7:  # Si menos del 70% son numéricos, no es campo numérico
                return analysis
            
            # Estadísticas básicas
            zero_count = (non_null_numeric == 0).sum()
            positive_count = (non_null_numeric > 0).sum()
            negative_count = (non_null_numeric < 0).sum()
            total_count = len(non_null_numeric)
            
            # Análisis de rangos
            min_val = non_null_numeric.min()
            max_val = non_null_numeric.max()
            mean_val = non_null_numeric.mean()
            std_val = non_null_numeric.std()
            
            # MEJORADO: Detección de amounts vs otros tipos numéricos
            if abs(mean_val) > 1 and std_val > 1:  # Valores monetarios típicos
                zero_ratio = zero_count / total_count
                
                if zero_ratio > 0.3:  # Muchos ceros → debit_amount o credit_amount
                    if positive_count > negative_count:
                        analysis['debit_amount'] = 0.8
                    else:
                        analysis['credit_amount'] = 0.7
                else:  # Pocos ceros → amount general
                    analysis['amount'] = 0.9
            
            # MEJORADO: Detección de números de documento (valores pequeños, poco variados)
            elif max_val <= 1000 and std_val < 10:  # Números pequeños con poca variación
                unique_ratio = len(non_null_numeric.unique()) / len(non_null_numeric)
                if unique_ratio < 0.2:  # Poca variabilidad → número de documento o similar
                    analysis['document_number'] = 0.7
                    # NO sugerir amount para este tipo de datos
            
            # MEJORADO: Detección de años fiscales
            elif all(1900 <= val <= 2100 for val in non_null_numeric if pd.notna(val)):
                unique_years = len(non_null_numeric.unique())
                if unique_years <= 5:  # Pocos años únicos
                    analysis['fiscal_year'] = 0.9
            
            # MEJORADO: Detección de line numbers (secuenciales)
            elif max_val <= 100 and min_val >= 1:
                consecutive_count = 0
                sorted_values = sorted(non_null_numeric)
                for i in range(1, min(len(sorted_values), 20)):
                    if sorted_values[i] == sorted_values[i-1] + 1:
                        consecutive_count += 1
                
                if consecutive_count > len(sorted_values) * 0.3:
                    analysis['line_number'] = 0.8
            
            # MEJORADO: Detección de journal entry IDs (valores repetidos)
            elif len(non_null_numeric.unique()) < len(non_null_numeric) * 0.7:
                value_counts = non_null_numeric.value_counts()
                if (value_counts > 1).sum() > 0:  # Hay valores repetidos
                    analysis['journal_entry_id'] = 0.7
            
            # NUEVO: Detección de vendor_id (numérico)
            elif max_val <= 999999 and min_val >= 1:  # Rango típico de IDs
                unique_ratio = len(non_null_numeric.unique()) / len(non_null_numeric)
                if unique_ratio > 0.8:  # Alta variabilidad → IDs únicos
                    analysis['vendor_id'] = 0.6
            
        except Exception as e:
            logger.debug(f"Error in numeric analysis: {e}")
        
        return analysis
    
    def _analyze_text_content(self, str_data: pd.Series, field_name: str) -> Dict[str, float]:
        """Análisis de contenido de texto mejorado con nombres actualizados"""
        analysis = {}
        
        try:
            # Verificar si realmente es texto (no números convertidos a string)
            numeric_like = 0
            for val in str_data.head(10):
                try:
                    float(val)
                    numeric_like += 1
                except:
                    pass
            
            if numeric_like > len(str_data.head(10)) * 0.8:
                # Es principalmente numérico convertido a string, no analizar como texto
                return analysis
            
            unique_ratio = len(str_data.unique()) / len(str_data)
            avg_length = str_data.str.len().mean()
            
            # Análisis de descripción basado en nombre del campo
            field_lower = field_name.lower()
            
            if 'descripcion' in field_lower or 'description' in field_lower:
                if unique_ratio > 0.7:  # Alta variabilidad
                    analysis['line_description'] = 0.8
                else:  # Baja variabilidad
                    analysis['description'] = 0.7
            
            elif 'concepto' in field_lower or 'concept' in field_lower:
                analysis['description'] = 0.8
            
            # Análisis general de texto
            elif avg_length > 10 and unique_ratio > 0.5:
                analysis['line_description'] = 0.6
            elif avg_length > 5 and unique_ratio < 0.3:
                analysis['description'] = 0.5
            
        except Exception as e:
            logger.debug(f"Error in text analysis: {e}")
        
        return analysis
    
    def _analyze_date_content_improved(self, str_data: pd.Series) -> Dict[str, float]:
        """Análisis de fechas MEJORADO con nombres actualizados"""
        analysis = {}
        
        try:
            # Patrones de fecha más específicos - INCLUYE DD.MM.YYYY
            date_patterns = [
            # ========== FORMATOS BÁSICOS CON 4 DÍGITOS DE AÑO ==========
            r'^\d{4}-\d{2}-\d{2}$',          # YYYY-MM-DD
            r'^\d{4}-\d{1,2}-\d{1,2}$',      # YYYY-M-D
            r'^\d{2}/\d{2}/\d{4}$',          # DD/MM/YYYY
            r'^\d{1,2}/\d{1,2}/\d{4}$',      # D/M/YYYY
            r'^\d{4}/\d{2}/\d{2}$',          # YYYY/MM/DD
            r'^\d{4}/\d{1,2}/\d{1,2}$',      # YYYY/M/D
            r'^\d{2}-\d{2}-\d{4}$',          # DD-MM-YYYY
            r'^\d{1,2}-\d{1,2}-\d{4}$',      # D-M-YYYY
            r'^\d{2}\.\d{2}\.\d{4}$',        # DD.MM.YYYY 
            r'^\d{1,2}\.\d{1,2}\.\d{4}$',    # D.M.YYYY 
            r'^\d{4}\.\d{2}\.\d{2}$',        # YYYY.MM.DD 
            r'^\d{4}\.\d{1,2}\.\d{1,2}$',    # YYYY.M.D
            r'^\d{8}$',                      # YYYYMMDD
            
            # ========== FORMATOS CON 2 DÍGITOS DE AÑO ==========
            r'^\d{2}/\d{2}/\d{2}$',          # DD/MM/YY
            r'^\d{1,2}/\d{1,2}/\d{2}$',      # D/M/YY
            r'^\d{2}-\d{2}-\d{2}$',          # DD-MM-YY
            r'^\d{1,2}-\d{1,2}-\d{2}$',      # D-M-YY
            r'^\d{2}\.\d{2}\.\d{2}$',        # DD.MM.YY
            r'^\d{1,2}\.\d{1,2}\.\d{2}$',    # D.M.YY
            r'^\d{6}$',                      # DDMMYY o YYMMDD
            
            # ========== FORMATOS AMERICANOS ==========
            r'^\d{1,2}/\d{1,2}/\d{4}$',      # M/D/YYYY (duplicado pero importante)
            r'^\d{2}/\d{2}/\d{4}$',          # MM/DD/YYYY 
            r'^\d{1,2}-\d{1,2}-\d{4}$',      # M-D-YYYY
            r'^\d{2}-\d{2}-\d{4}$',          # MM-DD-YYYY
            
            # ========== FORMATOS CON NOMBRES DE MES (ABREVIADOS) ==========
            r'^\d{1,2}[-\s]?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-\s]?\d{2,4}$',  # D-Jan-YYYY
            r'^\d{1,2}[-\s]?(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-\s]?\d{2,4}$',  # D-jan-yyyy
            r'^\d{1,2}[-\s]?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[-\s]?\d{2,4}$',  # D-JAN-YYYY
            r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-\s]?\d{1,2}[-\s]?\d{2,4}$',  # Jan-D-YYYY
            r'^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-\s]?\d{1,2}[-\s]?\d{2,4}$',  # jan-d-yyyy
            r'^(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[-\s]?\d{1,2}[-\s]?\d{2,4}$',  # JAN-D-YYYY
            r'^\d{2,4}[-\s]?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-\s]?\d{1,2}$',  # YYYY-Jan-D
            
            # ========== FORMATOS CON NOMBRES DE MES (COMPLETOS) ==========
            r'^\d{1,2}[-\s]?(January|February|March|April|May|June|July|August|September|October|November|December)[-\s]?\d{2,4}$',
            r'^\d{1,2}[-\s]?(january|february|march|april|may|june|july|august|september|october|november|december)[-\s]?\d{2,4}$',
            r'^(January|February|March|April|May|June|July|August|September|October|November|December)[-\s]?\d{1,2}[-\s]?\d{2,4}$',
            r'^(january|february|march|april|may|june|july|august|september|october|november|december)[-\s]?\d{1,2}[-\s]?\d{2,4}$',
            
            # ========== FORMATOS EN ESPAÑOL ==========
            r'^\d{1,2}[-\s]?(Ene|Feb|Mar|Abr|May|Jun|Jul|Ago|Sep|Oct|Nov|Dic)[-\s]?\d{2,4}$',  # D-Ene-YYYY
            r'^\d{1,2}[-\s]?(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)[-\s]?\d{2,4}$',  # D-ene-yyyy
            r'^\d{1,2}[-\s]?(Enero|Febrero|Marzo|Abril|Mayo|Junio|Julio|Agosto|Septiembre|Octubre|Noviembre|Diciembre)[-\s]?\d{2,4}$',
            r'^\d{1,2}[-\s]?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)[-\s]?\d{2,4}$',
            
            # ========== FORMATOS CON TIEMPO INCLUIDO ==========
            r'^\d{4}-\d{2}-\d{2}\s\d{1,2}:\d{2}$',                    # YYYY-MM-DD HH:MM
            r'^\d{4}-\d{2}-\d{2}\s\d{1,2}:\d{2}:\d{2}$',              # YYYY-MM-DD HH:MM:SS
            r'^\d{2}/\d{2}/\d{4}\s\d{1,2}:\d{2}$',                    # DD/MM/YYYY HH:MM
            r'^\d{2}/\d{2}/\d{4}\s\d{1,2}:\d{2}:\d{2}$',              # DD/MM/YYYY HH:MM:SS
            r'^\d{1,2}/\d{1,2}/\d{4}\s\d{1,2}:\d{2}(:\d{2})?$',       # D/M/YYYY HH:MM(:SS)?
            r'^\d{2}\.\d{2}\.\d{4}\s\d{1,2}:\d{2}(:\d{2})?$',         # DD.MM.YYYY HH:MM(:SS)?
            r'^\d{4}/\d{2}/\d{2}\s\d{1,2}:\d{2}(:\d{2})?$',           # YYYY/MM/DD HH:MM(:SS)?
            
            # ========== FORMATOS ISO Y TÉCNICOS ==========
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',                 # ISO 8601: YYYY-MM-DDTHH:MM:SS
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$',                # ISO 8601 with Z
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z?$',        # ISO 8601 con milisegundos
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$',  # ISO 8601 con timezone
            
            # ========== FORMATOS CON SEPARADORES ALTERNATIVOS ==========
            r'^\d{1,2}\s\d{1,2}\s\d{2,4}$',           # D M YYYY (espacios)
            r'^\d{2,4}\s\d{1,2}\s\d{1,2}$',           # YYYY M D (espacios)
            r'^\d{1,2}_\d{1,2}_\d{2,4}$',             # D_M_YYYY (guiones bajos)
            r'^\d{2,4}_\d{1,2}_\d{1,2}$',             # YYYY_M_D (guiones bajos)
            r'^\d{1,2}\|\d{1,2}\|\d{2,4}$',           # D|M|YYYY (pipes)
            r'^\d{2,4}\|\d{1,2}\|\d{1,2}$',           # YYYY|M|D (pipes)
            
            # ========== FORMATOS ESPECÍFICOS DE ERP ==========
            r'^\d{4}\d{2}\d{2}$',                     # YYYYMMDD (SAP típico)
            r'^\d{2}\d{2}\d{4}$',                     # DDMMYYYY
            r'^\d{2}\d{2}\d{2}$',                     # DDMMYY
            r'^\d{6}$',                               # YYMMDD o DDMMYY
            r'^\d{4}-\d{3}$',                         # YYYY-DDD (día juliano)
            r'^\d{2}/\d{4}$',                         # MM/YYYY (solo mes y año)
            r'^\d{1,2}/\d{4}$',                       # M/YYYY
            r'^\d{4}/\d{2}$',                         # YYYY/MM
            r'^\d{4}/\d{1,2}$',                       # YYYY/M
            r'^\d{4}-\d{2}$',                         # YYYY-MM
            r'^\d{4}-\d{1,2}$',                       # YYYY-M
            
            # ========== FORMATOS DE TIMESTAMPS ==========
            r'^\d{10}$',                              # Unix timestamp (10 dígitos)
            r'^\d{13}$',                              # Unix timestamp milisegundos (13 dígitos)
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$',  # YYYY-MM-DD HH:MM:SS.microseconds
            
            # ========== FORMATOS REGIONALES ESPECÍFICOS ==========
            # Alemán
            r'^\d{1,2}\.\d{1,2}\.\d{4}$',             # D.M.YYYY (alemán)
            r'^\d{2}\.\d{2}\.\d{2}$',                 # DD.MM.YY (alemán)
            
            # Francés  
            r'^\d{1,2}/\d{1,2}/\d{4}$',               # D/M/YYYY (francés)
            r'^\d{1,2}-\d{1,2}-\d{4}$',               # D-M-YYYY (francés)
            
            # Reino Unido
            r'^\d{1,2}/\d{1,2}/\d{4}$',               # DD/MM/YYYY (UK)
            r'^\d{1,2}-\d{1,2}-\d{4}$',               # DD-MM-YYYY (UK)
            
            # ========== FORMATOS POCO COMUNES PERO POSIBLES ==========
            r'^\d{1,2}st|nd|rd|th\s\w+\s\d{4}$',      # 1st January 2024
            r'^\w+\s\d{1,2}st|nd|rd|th,?\s\d{4}$',    # January 1st, 2024
            r'^\w{3}\s\d{1,2},?\s\d{4}$',             # Jan 1, 2024
            r'^\d{1,2}\s\w{3}\s\d{4}$',               # 1 Jan 2024
            r'^\d{4}年\d{1,2}月\d{1,2}日$',             # Formato japonés: 2024年1月1日
            r'^\d{4}.\d{1,2}.\d{1,2}$',               # YYYY.M.D (punto como separador genérico)
            ]   

            
            date_like_count = 0
            total_checked = min(len(str_data), 20)  # Limitar la verificación
            
            for val in str_data.head(total_checked):
                val_str = str(val).strip()
                
                # Verificar patrones de fecha
                if any(re.match(pattern, val_str) for pattern in date_patterns):
                    date_like_count += 1
                    continue
                
                # Verificar con pandas (más permisivo pero verificar resultado)
                try:
                    parsed_date = pd.to_datetime(val_str, errors='coerce')
                    if pd.notna(parsed_date) and 1900 <= parsed_date.year <= 2100:
                        # Verificar que no sea un número convertido incorrectamente
                        if not val_str.replace('.', '').replace('/', '').replace('-', '').isdigit() or len(val_str) > 6:
                            date_like_count += 1
                except:
                    pass
            
            if total_checked > 0:
                date_ratio = date_like_count / total_checked
                
                if date_ratio >= 0.8:
                    analysis['posting_date'] = 0.9
                    analysis['entry_date'] = 0.85
                elif date_ratio >= 0.6:
                    analysis['posting_date'] = 0.7
                    analysis['entry_date'] = 0.65
                elif date_ratio >= 0.4:
                    analysis['posting_date'] = 0.5
                    analysis['entry_date'] = 0.45
                    

                
        except Exception as e:
            logger.debug(f"Error in date analysis: {e}")
        
        return analysis
    
    def _analyze_vendor_id_content(self, field_name: str, str_data: pd.Series) -> Dict[str, float]:
        """NUEVO: Análisis específico para vendor_id"""
        analysis = {}
        field_lower = field_name.lower()
        
        # Patrones de nombre que indican vendor_id
        vendor_patterns = [
            'proveedor', 'vendor', 'supplier', 'fornecedor', 'fournisseur', 'fornitore', 'lieferant'
        ]
        
        if any(pattern in field_lower for pattern in vendor_patterns):
            # Verificar si contiene 'id', 'codigo', 'code', 'num'
            if any(id_pattern in field_lower for id_pattern in ['id', 'codigo', 'code', 'num']):
                analysis['vendor_id'] = 0.9
            else:
                # Podría ser vendor_id si es alfanumérico corto
                avg_length = str_data.str.len().mean()
                unique_ratio = len(str_data.unique()) / len(str_data)
                
                if avg_length <= 15 and unique_ratio > 0.8:  # IDs cortos y únicos
                    analysis['vendor_id'] = 0.7
        
        return analysis
    
    def _analyze_gl_account_name_content(self, field_name: str, str_data: pd.Series) -> Dict[str, float]:
        """NUEVO: Análisis específico para gl_account_name"""
        analysis = {}
        field_lower = field_name.lower()
        
        # Patrones de nombre que indican gl_account_name
        account_name_patterns = [
            'nombre', 'name', 'denominacion', 'description', 'desc', 'titel', 'titre', 'titolo'
        ]
        
        account_patterns = [
            'cuenta', 'account', 'conto', 'compte', 'konto'
        ]
        
        # Verificar si es nombre de cuenta
        has_name_pattern = any(pattern in field_lower for pattern in account_name_patterns)
        has_account_pattern = any(pattern in field_lower for pattern in account_patterns)
        
        if has_name_pattern and has_account_pattern:
            # Es muy probable que sea nombre de cuenta
            analysis['gl_account_name'] = 0.9
        elif has_name_pattern and ('gl' in field_lower or 'mayor' in field_lower):
            # General ledger name
            analysis['gl_account_name'] = 0.8
        elif has_account_pattern and not any(num_pattern in field_lower for num_pattern in ['num', 'number', 'codigo', 'code']):
            # Cuenta pero no número de cuenta
            avg_length = str_data.str.len().mean()
            if avg_length > 10:  # Nombres suelen ser más largos que códigos
                analysis['gl_account_name'] = 0.7
        
        return analysis
    
    def _analyze_field_patterns(self, field_name: str, data: pd.Series) -> Dict[str, float]:
        """Análisis basado en patrones del nombre del campo con nombres actualizados"""
        analysis = {}
        field_lower = field_name.lower()
        
        # Patrones de nombres de campo específicos actualizados
        field_patterns = {
            'saldo': {'amount': 0.95},
            'balance': {'amount': 0.95},
            'importe': {'amount': 0.9},
            'total': {'amount': 0.85},
            'debe': {'debit_amount': 0.95},
            'haber': {'credit_amount': 0.95},
            'debit': {'debit_amount': 0.95},
            'credit': {'credit_amount': 0.95},
            'fecha': {'posting_date': 0.9},
            'date': {'posting_date': 0.9},
            'asiento': {'journal_entry_id': 0.9},
            'journal': {'journal_entry_id': 0.9},
            'cuenta': {'gl_account_number': 0.9},
            'account': {'gl_account_number': 0.9},
            'año': {'fiscal_year': 0.9},
            'year': {'fiscal_year': 0.9},
            'doc': {'document_number': 0.8},
            'documento': {'document_number': 0.8},
            'numero': {'document_number': 0.7},
            'num': {'document_number': 0.7},
            'periodo': {'period_number': 0.9},
            'period': {'period_number': 0.9},
            'preparado': {'prepared_by': 0.8},
            'prepared': {'prepared_by': 0.8},
            'entrada': {'entry_date': 0.8},
            'entry': {'entry_date': 0.8},
            'proveedor': {'vendor_id': 0.7},
            'vendor': {'vendor_id': 0.7},
            'supplier': {'vendor_id': 0.7},
        }
        
        for pattern, mappings in field_patterns.items():
            if pattern in field_lower:
                for field_type, confidence in mappings.items():
                    analysis[field_type] = confidence
                break  # Solo el primer patrón que coincida
        
        return analysis
    
    def _find_best_match_with_content(self, field_name: str, exact_matches: List[Tuple[str, float]], 
                                    content_analysis: Dict[str, float], sample_data: pd.Series) -> Optional[Tuple[str, float]]:
        """Encuentra el mejor mapeo combinando coincidencias exactas y análisis de contenido"""
        
        if not exact_matches and not content_analysis:
            return None
        
        # Combinar todas las opciones
        all_candidates = {}
        
        # Añadir coincidencias exactas
        for field_type, confidence in exact_matches:
            all_candidates[field_type] = confidence
        
        # FILTRAR banderas internas antes de procesar
        internal_flags = {
            'is_date', 'is_numeric', 'is_text', 'is_monetary', 
            'is_repetitive', 'is_sequential', 'date_like', 'amount_like'
        }
        
        # Lista de field_types válidos (puedes obtenerla dinámicamente si tienes acceso)
        valid_field_types = {
            'journal_entry_id', 'line_number', 'description', 'line_description',
            'posting_date', 'fiscal_year', 'period_number', 'gl_account_number',
            'amount', 'debit_amount', 'credit_amount', 'debit_credit_indicator',
            'prepared_by', 'entry_date', 'entry_time', 'gl_account_name', 'vendor_id'
        }
        
        # Añadir/mejorar con análisis de contenido
        for field_type, content_confidence in content_analysis.items():
            # FILTRO: Omitir banderas internas
            if field_type in internal_flags:
                continue
                
            # FILTRO: Solo field_types válidos
            if field_type not in valid_field_types:
                continue
                
            if field_type in all_candidates:
                # Combinar confianzas (promedio ponderado)
                existing_conf = all_candidates[field_type]
                combined_conf = (existing_conf * 0.7) + (content_confidence * 0.3)
                all_candidates[field_type] = min(combined_conf, 1.0)
            else:
                # Añadir nueva opción del análisis de contenido
                all_candidates[field_type] = content_confidence * 0.8  # Factor de ajuste
        
        if not all_candidates:
            return None
        
        # Encontrar el mejor candidato
        best_field_type = max(all_candidates.keys(), key=lambda x: all_candidates[x])
        best_confidence = all_candidates[best_field_type]
        
        # Verificar umbral mínimo
        if best_confidence < 0.3:
            return None
        
        return (best_field_type, best_confidence)
    
    def _resolve_mapping_conflict(self, field_name: str, field_type: str, confidence: float, 
                                sample_data: pd.Series) -> Optional[Tuple[str, float]]:
        """ENHANCED: Resuelve conflictos con balance validation para journal_entry_id"""
        
        # Si el campo no está usado, asignar directamente
        if field_type not in self._used_field_mappings:
            return (field_type, confidence)
        
        # Hay conflicto - obtener información del mapeo existente
        existing_column = self._used_field_mappings[field_type]
        existing_confidence = self._confidence_by_column.get(existing_column, 0.0)
        
        # ✨ NUEVA LÓGICA ESPECIAL PARA JOURNAL_ENTRY_ID
        if field_type == 'journal_entry_id' and self._balance_validator and self._dataframe_for_balance is not None:
            print(f"🔍 JOURNAL_ENTRY_ID BALANCE VALIDATION CONFLICT:")
            print(f"   Existing: '{existing_column}' (confidence: {existing_confidence:.3f})")
            print(f"   New:      '{field_name}' (confidence: {confidence:.3f})")
            
            # Probar balance validation con ambos candidatos
            balance_winner = self._resolve_journal_entry_id_by_balance(
                existing_column, existing_confidence,
                field_name, confidence
            )
            
            if balance_winner:
                winner_column, winner_confidence, reason = balance_winner
                
                # Si el ganador es diferente al existente, hacer reassignment
                if winner_column != existing_column:
                    # Liberar el mapeo anterior
                    del self._used_field_mappings[field_type]
                    del self._column_mappings[existing_column]
                    if existing_column in self._confidence_by_column:
                        del self._confidence_by_column[existing_column]
                    
                    self.mapping_stats['balance_validation_wins'] = self.mapping_stats.get('balance_validation_wins', 0) + 1
                    print(f"🏆 BALANCE VALIDATION WINNER: '{winner_column}' ({reason})")
                    
                    return (field_type, winner_confidence)
                else:
                    print(f"✅ Existing mapping '{existing_column}' confirmed by balance validation")
                    return None  # Mantener mapeo existente
            else:
                print(f"⚠️ Balance validation inconclusive - using confidence comparison")
        
        # LÓGICA ORIGINAL para otros campos o fallback
        should_reassign = False
        
        # Razón 1: La nueva confianza es significativamente mayor
        if confidence > existing_confidence + 0.2:
            should_reassign = True
            reason = f"higher confidence ({confidence:.3f} vs {existing_confidence:.3f})"
        
        # Razón 2: Análisis de contenido específico para amounts (si existe el método)
        elif field_type == 'amount' and hasattr(self, '_is_better_amount_candidate') and sample_data is not None:
            if self._is_better_amount_candidate(field_name, sample_data):
                should_reassign = True
                reason = "better amount candidate based on content"
        
        # Razón 3: Nombre del campo más específico (si existe el método)
        elif hasattr(self, '_has_better_field_name') and self._has_better_field_name(field_name, existing_column, field_type):
            should_reassign = True
            reason = "more specific field name"
        
        if should_reassign:
            # Liberar el mapeo anterior
            del self._used_field_mappings[field_type]
            del self._column_mappings[existing_column]
            if existing_column in self._confidence_by_column:
                del self._confidence_by_column[existing_column]
            
            self.mapping_stats['smart_reassignments'] = self.mapping_stats.get('smart_reassignments', 0) + 1
            print(f"🔄 SMART REASSIGNMENT: '{field_name}' takes '{field_type}' from '{existing_column}' ({reason})")
            
            return (field_type, confidence)
        else:
            # Mantener el mapeo existente
            self.mapping_stats['unique_mapping_conflicts'] = self.mapping_stats.get('unique_mapping_conflicts', 0) + 1
            print(f"⚠️ Field '{field_type}' already mapped to '{existing_column}' with better confidence, skipping '{field_name}'")
            return None
    
    def _resolve_journal_entry_id_by_balance(self, existing_column: str, existing_confidence: float,
                                        new_column: str, new_confidence: float) -> Optional[Tuple[str, float, str]]:
        """
        Resuelve conflicto de journal_entry_id usando balance validation
        Reutiliza la lógica existente del balance_validator.py
        """
        try:
            # Preparar campos numéricos si es necesario
            if not self._numeric_fields_prepared:
                self._prepare_numeric_fields()
            
            # Identificar campos amount para balance validation
            amount_columns = self._identify_amount_columns()
            
            if not amount_columns:
                print(f"   ⚠️ No amount columns identified - cannot perform balance validation")
                return None
            
            print(f"   🧮 Testing balance validation with amount columns: {list(amount_columns.keys())}")

            if len(amount_columns) == 0:
                print(f"   ❌ No amount columns with confidence >= 0.75 found")
                print(f"      Cannot perform reliable balance validation - skipping")
                return None
            
            # Probar candidato existente
            existing_score = self._test_journal_entry_candidate(existing_column, amount_columns)
            print(f"   📊 '{existing_column}': balance_score = {existing_score:.3f}")
            
            # Probar nuevo candidato  
            new_score = self._test_journal_entry_candidate(new_column, amount_columns)
            print(f"   📊 '{new_column}': balance_score = {new_score:.3f}")
            
            # Determinar ganador
            score_diff = abs(existing_score - new_score)
            
            if score_diff < 0.1:  # Scores muy similares, usar confianza
                if new_confidence > existing_confidence:
                    return (new_column, new_confidence, f"balance_tie_confidence_wins")
                else:
                    return (existing_column, existing_confidence, f"balance_tie_confidence_wins")
            elif new_score > existing_score:
                return (new_column, new_confidence, f"better_balance_score_{new_score:.3f}")
            else:
                return (existing_column, existing_confidence, f"better_balance_score_{existing_score:.3f}")
        
        except Exception as e:
            print(f"   ❌ Balance validation error: {e}")
            return None

    # 5. MÉTODO AUXILIAR: Preparar campos numéricos (reutiliza _analyze_numeric_content existente)  
    def _prepare_numeric_fields(self):
        """Prepara campos numéricos usando las funciones de análisis existentes"""
        if self._dataframe_for_balance is None:
            return
            
        print(f"   🔢 Preparing numeric fields for balance validation...")
        
        # Usar el método existente _analyze_numeric_content para identificar campos
        for column in self._dataframe_for_balance.columns:
            try:
                sample_data = self._dataframe_for_balance[column].dropna().head(100)
                
                # Usar la función existente de análisis numérico
                numeric_analysis = self._analyze_numeric_content(sample_data)
                
                # Si el análisis sugiere que es un campo amount, prepararlo
                if any(field_type in ['amount', 'debit_amount', 'credit_amount'] for field_type in numeric_analysis.keys()):
                    # Limpiar campo numérico usando lógica similar al automatic_confirmation_trainer
                    cleaned_series = self._clean_numeric_column(self._dataframe_for_balance[column])
                    self._dataframe_for_balance[f"{column}_numeric"] = cleaned_series
                    
            except Exception as e:
                continue
        
        self._numeric_fields_prepared = True

    # 6. MÉTODO AUXILIAR: Limpiar columna numérica (extraído del automatic_confirmation_trainer)
    def _clean_numeric_column(self, series: pd.Series) -> pd.Series:
        """Limpia una columna numérica - ADAPTADO del automatic_confirmation_trainer"""
        def clean_numeric_value(val):
            if pd.isna(val) or val == '':
                return 0.0
            
            try:
                val_str = str(val).strip()
                
                # Remover símbolos comunes de moneda y espacios
                import re
                val_str = re.sub(r'[€$£¥₹\s]', '', val_str)
                
                # Extraer primer número encontrado
                numbers = re.findall(r'-?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?', val_str)
                if numbers:
                    first_num = numbers[0]
                    # Normalizar formato decimal
                    if ',' in first_num and '.' in first_num:
                        # 1,234.56 vs 1.234,56
                        if first_num.rfind('.') > first_num.rfind(','):
                            first_num = first_num.replace(',', '')
                        else:
                            first_num = first_num.replace('.', '').replace(',', '.')
                    elif first_num.count(',') == 1 and len(first_num.split(',')[1]) <= 2:
                        # Decimal europeo: 1234,56
                        first_num = first_num.replace(',', '.')
                    else:
                        # Separador de miles
                        first_num = first_num.replace(',', '')
                    
                    return float(first_num)
                
                return 0.0
            except:
                return 0.0
        
        return series.apply(clean_numeric_value)

    # 7. MÉTODO AUXILIAR: Identificar columnas de amount
    def _identify_amount_columns(self) -> Dict[str, str]:
        """CORREGIDO: Identifica qué columnas contienen amounts usando SOLO la confianza original"""
        amount_columns = {}
        
        # SOLO usar mapeos ya realizados con verificación de confianza ORIGINAL
        for field_type, column_name in self._used_field_mappings.items():
            if field_type in ['debit_amount', 'credit_amount', 'amount']:
                try:
                    # ✅ USAR SIEMPRE la confianza original (la que está registrada)
                    column_confidence = getattr(self, '_confidence_by_column', {}).get(column_name, 0.0)
                    if column_confidence >= 0.75:
                        amount_columns[field_type] = column_name
                        print(f"      ✅ {field_type}: '{column_name}' (confidence: {column_confidence:.3f})")
                    else:
                        print(f"      ❌ {field_type}: '{column_name}' skipped (confidence: {column_confidence:.3f} < 0.75)")
                except Exception as e:
                    print(f"      ❌ {field_type}: '{column_name}' skipped (error checking confidence)")
                    continue
        
        # ✅ SIMPLIFICACIÓN: NO analizar otras columnas para evitar inconsistencias
        # Si no hay suficientes campos con alta confianza, mejor no usar balance validation
        
        # Mostrar resumen
        if len(amount_columns) == 0:
            print(f"      ❌ No amount fields found with confidence >= 0.75")
            print(f"      🔍 Balance validation requires pre-mapped fields with high confidence")
        elif len(amount_columns) == 1:
            print(f"      ⚠️ Only 1 amount field found - balance validation may be limited")
        else:
            print(f"      ✅ Found {len(amount_columns)} amount fields for balance validation")
                    
        return amount_columns

    # 8. MÉTODO AUXILIAR: Probar un candidato de journal_entry_id
    def _test_journal_entry_candidate(self, journal_column: str, amount_columns: Dict[str, str]) -> float:
        """Prueba un candidato de journal_entry_id y retorna balance score"""
        try:
            # Crear DataFrame de prueba
            test_df = pd.DataFrame()
            test_df['journal_entry_id'] = self._dataframe_for_balance[journal_column]
            
            # Agregar campos amount limpios
            for field_type, column_name in amount_columns.items():
                if f"{column_name}_numeric" in self._dataframe_for_balance.columns:
                    test_df[field_type] = self._dataframe_for_balance[f"{column_name}_numeric"]
                else:
                    test_df[field_type] = self._clean_numeric_column(self._dataframe_for_balance[column_name])
            
            # Asegurar que tenemos debit_amount y credit_amount
            if 'debit_amount' not in test_df.columns:
                test_df['debit_amount'] = 0.0
            if 'credit_amount' not in test_df.columns:
                test_df['credit_amount'] = 0.0
                
            # Si solo tenemos 'amount', distribuir en debit/credit
            if 'amount' in test_df.columns and test_df['debit_amount'].sum() == 0 and test_df['credit_amount'].sum() == 0:
                test_df['debit_amount'] = test_df['amount'].apply(lambda x: x if x > 0 else 0)
                test_df['credit_amount'] = test_df['amount'].apply(lambda x: -x if x < 0 else 0)
            
            # Ejecutar balance validation usando el validator existente
            balance_report = self._balance_validator.perform_comprehensive_balance_validation(test_df)
            
            # Calcular score (0-1) basado en el reporte
            return self._calculate_balance_score(balance_report)
            
        except Exception as e:
            print(f"     Error testing candidate '{journal_column}': {e}")
            return 0.0

    # 9. MÉTODO AUXILIAR: Calcular score de balance 
    def _calculate_balance_score(self, balance_report: Dict[str, Any]) -> float:
        """Calcula score 0-1 basado en balance validation results"""
        try:
            score = 0.0
            
            # Factor 1: Balance total (40% del score)
            if balance_report.get('is_balanced', False):
                score += 0.4
            else:
                # Penalizar por diferencia relativa
                total_diff = abs(balance_report.get('total_balance_difference', float('inf')))
                total_sum = balance_report.get('total_debit_sum', 0) + balance_report.get('total_credit_sum', 0)
                if total_sum > 0:
                    diff_ratio = total_diff / total_sum
                    score += 0.4 * max(0, 1 - diff_ratio * 5)  # Penalizar diferencias
            
            # Factor 2: Tasa de asientos balanceados (60% del score)  
            entries_count = balance_report.get('entries_count', 0)
            if entries_count > 0:
                balanced_count = balance_report.get('balanced_entries_count', 0)
                balance_rate = balanced_count / entries_count
                score += 0.6 * balance_rate
            
            return min(score, 1.0)
            
        except Exception as e:
            return 0.0
    def _is_better_amount_candidate(self, field_name: str, sample_data: pd.Series) -> bool:
        """Verifica si una columna es mejor candidata para amount"""
        try:
            # Verificar nombre del campo
            field_lower = field_name.lower()
            amount_indicators = ['saldo', 'balance', 'importe', 'amount', 'total']
            
            if any(indicator in field_lower for indicator in amount_indicators):
                # Verificar contenido
                numeric_data = pd.to_numeric(sample_data, errors='coerce').dropna()
                if len(numeric_data) > 0:
                    # Verificar que son valores monetarios (variedad y rango apropiado)
                    std_val = numeric_data.std()
                    mean_val = abs(numeric_data.mean())
                    
                    if std_val > 1 and mean_val > 1:  # Valores monetarios típicos
                        return True
            
            return False
            
        except Exception:
            return False
    
    def _has_better_field_name(self, new_field_name: str, existing_field_name: str, field_type: str) -> bool:
        """Compara nombres de campo para determinar cuál es más específico"""
        
        # Diccionario de especificidad por tipo de campo actualizado
        specificity_keywords = {
            'amount': ['saldo', 'balance', 'importe', 'amount'],
            'debit_amount': ['debe', 'debit'],
            'credit_amount': ['haber', 'credit'],
            'journal_entry_id': ['asiento', 'journal'],
            'posting_date': ['fecha', 'date'],
            'gl_account_number': ['cuenta', 'account'],
            'gl_account_name': ['nombre', 'name'],
            'vendor_id': ['proveedor', 'vendor', 'supplier']
        }
        
        if field_type not in specificity_keywords:
            return False
        
        keywords = specificity_keywords[field_type]
        new_score = sum(1 for kw in keywords if kw in new_field_name.lower())
        existing_score = sum(1 for kw in keywords if kw in existing_field_name.lower())
        
        return new_score > existing_score
    
    def _find_exact_matches(self, field_name: str, erp_system: str = None) -> List[Tuple[str, float]]:
        """Encuentra coincidencias exactas con prioridad por ERP"""
        normalized_name = self._normalize_field_name(field_name)
        exact_matches = []
        
        field_definitions = self.field_loader.get_field_definitions()
        
        for field_type, field_def in field_definitions.items():
            # Prioridad 1: Coincidencia exacta en ERP específico
            if erp_system and erp_system in field_def.synonyms_by_erp:
                for synonym in field_def.synonyms_by_erp[erp_system]:
                    if normalized_name == self._normalize_field_name(synonym.name):
                        if not self._is_problematic_partial_match(field_name, synonym.name):
                            confidence = min(0.95 + (synonym.confidence_boost * 0.05), 1.0)
                            exact_matches.append((field_type, confidence))
            
            # Prioridad 2: Coincidencia exacta en cualquier ERP
            for erp_synonyms in field_def.synonyms_by_erp.values():
                for synonym in erp_synonyms:
                    if normalized_name == self._normalize_field_name(synonym.name):
                        if not self._is_problematic_partial_match(field_name, synonym.name):
                            confidence = min(0.85 + (synonym.confidence_boost * 0.1), 1.0)
                            exact_matches.append((field_type, confidence))
            
            # Prioridad 3: Coincidencia exacta con código de campo
            if normalized_name == self._normalize_field_name(field_def.code):
                exact_matches.append((field_type, 0.90))
        
        # Eliminar duplicados manteniendo el mejor score
        unique_matches = {}
        for field_type, confidence in exact_matches:
            if field_type not in unique_matches or confidence > unique_matches[field_type]:
                unique_matches[field_type] = confidence
        
        return [(field_type, confidence) for field_type, confidence in unique_matches.items()]
    
    def _is_problematic_partial_match(self, field_name: str, synonym_name: str) -> bool:
        """Detecta coincidencias parciales problemáticas"""
        field_lower = field_name.lower()
        synonym_lower = synonym_name.lower()
        
        if field_lower != synonym_lower:
            if synonym_lower in field_lower:
                problematic_prefixes = ['fecha', 'numero', 'codigo', 'tipo', 'descripcion']
                for prefix in problematic_prefixes:
                    if field_lower.startswith(prefix) and synonym_lower not in prefix:
                        return True
        
        return False
    
    def _try_translate_field_name(self, field_name: str) -> str:
        """Intenta traducir nombres de campos de otros idiomas"""
        field_lower = field_name.lower()
        normalized = self._normalize_field_name(field_lower)
        
        for foreign_word, spanish_word in self.translation_map.items():
            if foreign_word in normalized:
                return field_name.replace(foreign_word, spanish_word)
        
        return field_name
    
    def get_confidence_boost(self, field_name: str, field_type: str, erp_system: str = None) -> float:
        """Obtiene el boost de confianza para un campo específico"""
        field_def = self.field_loader.get_field_definition(field_type)
        if not field_def:
            return 0.0
        
        boost = 0.0
        
        if erp_system:
            boost = field_def.get_confidence_for_erp(erp_system)
        else:
            all_confidences = [
                field_def.get_confidence_for_erp(erp) 
                for erp in field_def.synonyms_by_erp.keys()
            ]
            boost = sum(all_confidences) / len(all_confidences) if all_confidences else 0.0
        
        return self._normalize_confidence_score(boost)
    
    def add_dynamic_synonym(self, field_type: str, synonym_name: str, 
                           erp_system: str = "Custom", confidence_boost: float = 0.0) -> bool:
        """Añade un sinónimo dinámicamente"""
        field_def = self.field_loader.get_field_definition(field_type)
        
        if field_def:
            success = field_def.add_synonym(erp_system, synonym_name, confidence_boost)
            if success:
                self._clear_caches()
                print(f"✓ Added synonym: {synonym_name} -> {field_type} ({erp_system})")
            return success
        else:
            print(f"❌ Field not found: {field_type}")
            return False
    
    def remove_dynamic_synonym(self, field_type: str, synonym_name: str, erp_system: str) -> bool:
        """Elimina un sinónimo dinámicamente"""
        field_def = self.field_loader.get_field_definition(field_type)
        
        if field_def:
            success = field_def.remove_synonym(erp_system, synonym_name)
            if success:
                self._clear_caches()
                print(f"✓ Removed synonym: {synonym_name} from {field_type} ({erp_system})")
            return success
        else:
            print(f"❌ Field not found: {field_type}")
            return False
    
    def get_all_erp_systems(self) -> List[str]:
        """Obtiene lista de todos los sistemas ERP configurados"""
        erp_systems = set()
        
        field_definitions = self.field_loader.get_field_definitions()
        for field_def in field_definitions.values():
            erp_systems.update(field_def.synonyms_by_erp.keys())
        
        return sorted(list(erp_systems))
    
    def get_all_field_types(self) -> List[str]:
        """Obtiene lista de todos los tipos de campo configurados"""
        return list(self.field_loader.get_field_definitions().keys())
    
    def _normalize_field_name(self, name: str) -> str:
        """Normaliza nombre de campo con cache para optimización"""
        if not name:
            return ""
        
        if name in self._normalization_cache:
            return self._normalization_cache[name]
        
        normalized = re.sub(r'[^a-zA-Z0-9]', '', name.lower())
        
        for accented, plain in self.accent_map.items():
            normalized = normalized.replace(accented, plain)
        
        self._normalization_cache[name] = normalized
        
        return normalized
    
    def _clear_caches(self):
        """Limpia todos los caches"""
        self._normalization_cache.clear()
        self._mapping_cache.clear()
        self._erp_synonyms_cache.clear()
        self._content_analysis_cache.clear()
        logger.debug("Enhanced field mapper caches cleared")
    
    def _normalize_confidence_score(self, raw_score: float) -> float:
        """Función auxiliar para normalizar cualquier score a rango 0-1"""
        if raw_score < 0:
            return 0.0
        elif raw_score > 1:
            return 1.0
        else:
            return raw_score
    
    def get_mapping_statistics(self) -> Dict:
        """Obtiene estadísticas mejoradas de los mapeos incluyendo mapeo único"""
        field_definitions = self.field_loader.get_field_definitions()
        
        total_synonyms = sum(
            len(field_def.get_all_synonyms()) 
            for field_def in field_definitions.values()
        )
        
        erp_systems = self.get_all_erp_systems()
        
        return {
            'total_field_types': len(field_definitions),
            'total_synonyms': total_synonyms,
            'erp_systems': len(erp_systems),
            'erp_systems_list': erp_systems,
            'unique_mappings': {
                'total_mapped_fields': len(self._used_field_mappings),
                'mapped_columns': len(self._column_mappings),
                'available_fields': len(field_definitions) - len(self._used_field_mappings)
            },
            'cache_sizes': {
                'normalization_cache': len(self._normalization_cache),
                'mapping_cache': len(self._mapping_cache),
                'erp_synonyms_cache': len(self._erp_synonyms_cache),
                'content_analysis_cache': len(self._content_analysis_cache)
            },
            'usage_stats': self.mapping_stats.copy(),
            'field_loader_stats': self.field_loader.get_statistics()
        }
    
    def analyze_dataframe_with_unique_mapping(self, df: pd.DataFrame, erp_system: str = None) -> Dict:
        """
        MEJORADO: Analiza un DataFrame completo con mapeo único inteligente
        """
        print(f"🔍 INTELLIGENT UNIQUE MAPPING ANALYSIS: {df.shape[0]} rows, {df.shape[1]} columns")
        print("=" * 70)
        
        # Resetear mapeos únicos
        self.reset_mappings()
        
        results = {
            'total_columns': len(df.columns),
            'erp_system': erp_system,
            'field_mappings': {},
            'conflicts_found': [],
            'suggestions': [],
            'confidence_scores': {},
            'unique_mapping_stats': {
                'successful_mappings': 0,
                'failed_mappings': 0,
                'forced_headers': 0,
                'smart_reassignments': 0
            }
        }
        
        # Análisis en orden de prioridad (campos más específicos primero)
        column_priority = self._prioritize_columns(df.columns.tolist())
        
        for column in column_priority:
            sample_data = df[column].dropna().head(100)
            mapping_result = self.find_field_mapping(column, erp_system, sample_data)
            
            if mapping_result:
                field_type, confidence = mapping_result
                results['field_mappings'][column] = field_type
                results['confidence_scores'][column] = confidence
                results['unique_mapping_stats']['successful_mappings'] += 1
            else:
                results['unique_mapping_stats']['failed_mappings'] += 1
                results['suggestions'].append(
                    f"Column '{column}' could not be mapped (all suitable fields may be taken)."
                )
        
        # Copiar estadísticas de reasignaciones
        results['unique_mapping_stats']['smart_reassignments'] = self.mapping_stats['smart_reassignments']
        results['unique_mapping_stats']['forced_headers'] = self.mapping_stats['header_forced_mappings']
        
        detection_rate = len(results['field_mappings']) / len(df.columns) * 100
        print(f"\n📊 INTELLIGENT MAPPING RESULTS:")
        print(f"  • Detection rate: {detection_rate:.1f}%")
        print(f"  • Successful mappings: {results['unique_mapping_stats']['successful_mappings']}")
        print(f"  • Failed mappings: {results['unique_mapping_stats']['failed_mappings']}")
        print(f"  • Smart reassignments: {results['unique_mapping_stats']['smart_reassignments']}")
        print(f"  • Forced headers: {results['unique_mapping_stats']['forced_headers']}")
        
        return results
    
    def _prioritize_columns(self, columns: List[str]) -> List[str]:
        """NUEVO: Prioriza columnas para análisis (campos más específicos primero)"""
        
        # Definir prioridades por patrones de nombre actualizados
        priority_patterns = [
            # Prioridad 1: Campos muy específicos
            (['saldo', 'balance'], 1),
            (['debe', 'debit'], 1),
            (['haber', 'credit'], 1),
            
            # Prioridad 2: Fechas y IDs
            (['fecha', 'date'], 2),
            (['asiento', 'journal'], 2),
            (['cuenta', 'account'], 2),
            
            # Prioridad 3: Descripciones con cabecera
            (['cabecera', 'header'], 3),
            (['concepto', 'concept'], 3),
            
            # Prioridad 4: Descripciones generales
            (['descripcion', 'description'], 4),
            
            # Prioridad 5: Números de documento y nuevos campos
            (['doc', 'documento', 'numero'], 5),
            (['proveedor', 'vendor', 'supplier'], 5),
            (['nombre', 'name'], 5),
            
            # Prioridad 6: Otros campos
            ([], 6)  # Default
        ]
        
        column_priorities = {}
        
        for column in columns:
            column_lower = column.lower()
            priority = 6  # Default
            
            for patterns, prio in priority_patterns:
                if any(pattern in column_lower for pattern in patterns):
                    priority = prio
                    break
            
            column_priorities[column] = priority
        
        # Ordenar por prioridad (menor número = mayor prioridad)
        return sorted(columns, key=lambda col: column_priorities[col])


# Funciones de utilidad para Spyder (manteniendo compatibilidad)
def create_field_mapper(config_file: str = None) -> FieldMapper:
    """Función de conveniencia para crear mapper mejorado en Spyder"""
    return FieldMapper(config_source=config_file)

def test_updated_field_mapper():
    """Test del mapper actualizado para Spyder - Versión robusta"""
    print("🧪 Testing UPDATED FieldMapper...")
    
    try:
        # Primero probar la creación básica
        mapper = create_field_mapper()
        print("✓ Updated Mapper created successfully")
        
        # Verificar estadísticas básicas
        try:
            stats = mapper.get_mapping_statistics()
            print(f"✓ Mapper initialized with {stats['total_field_types']} field types")
        except Exception as e:
            print(f"⚠️ Warning: Could not get full statistics: {e}")
            print("✓ Continuing with basic functionality test...")
        
        # Test básico de análisis de contenido
        print(f"\n🔍 Testing content analysis:")
        
        # Datos de prueba simplificados
        import pandas as pd
        test_data = pd.DataFrame({
            'Fecha': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'Asiento': [1, 2, 3],
            'Línea': [1, 2, 1],
            'Cuenta': ['1001', '1002', '1003'],
            'Nombre Cuenta': ['Efectivo', 'Bancos', 'Clientes'],
            'Descripción': ['Desc A', 'Desc B', 'Desc C'],
            'Concepto': ['Concepto A', 'Concepto A', 'Concepto B'],
            'Debe': [100.0, 0.0, 200.0],
            'Haber': [0.0, 100.0, 0.0],
            'Saldo': [12650.0, -12650.0, 1188.29],
            'Proveedor ID': ['PROV001', 'PROV002', 'PROV001'],
            'Preparado Por': ['User1', 'User2', 'User1'],
            'Fecha Entrada': ['2024-01-01 10:00', '2024-01-02 11:00', '2024-01-03 09:00']
        })
        
        print(f"  ✓ Test dataset created: {test_data.shape[0]} rows, {test_data.shape[1]} columns")
        
        # Test de análisis individual de columnas
        print(f"\n🎯 Testing individual column analysis:")
        
        test_columns = ['Saldo', 'Debe', 'Haber', 'Fecha', 'Asiento', 'Nombre Cuenta', 'Proveedor ID']
        
        for column in test_columns:
            if column in test_data.columns:
                try:
                    sample_data = test_data[column].dropna().head(10)
                    result = mapper.find_field_mapping(column, "Generic_ES", sample_data)
                    
                    if result:
                        field_type, confidence = result
                        print(f"  ✓ {column:<20} -> {field_type:<25} ({confidence:.3f})")
                    else:
                        print(f"  ⚠️ {column:<20} -> No mapping found")
                        
                except Exception as e:
                    print(f"  ❌ {column:<20} -> Error: {e}")
        
        # Test de análisis completo si es posible
        try:
            print(f"\n📊 Testing complete dataframe analysis:")
            analysis_result = mapper.analyze_dataframe_with_unique_mapping(test_data, "Generic_ES")
            
            mapped_count = len(analysis_result.get('field_mappings', {}))
            total_columns = analysis_result.get('total_columns', len(test_data.columns))
            detection_rate = (mapped_count / total_columns * 100) if total_columns > 0 else 0
            
            print(f"  ✓ Mapped {mapped_count}/{total_columns} columns ({detection_rate:.1f}% detection rate)")
            
            # Mostrar mapeos encontrados
            if 'field_mappings' in analysis_result:
                print(f"\n  📋 Field Mappings Found:")
                for column, field_type in analysis_result['field_mappings'].items():
                    confidence = analysis_result.get('confidence_scores', {}).get(column, 0.0)
                    print(f"    • {column:<20} -> {field_type:<25} ({confidence:.3f})")
            
        except Exception as e:
            print(f"  ⚠️ Complete analysis failed: {e}")
            print(f"  ℹ️ This is expected if field definitions are not fully loaded")
        
        # Test de funciones de análisis de contenido
        print(f"\n🔬 Testing content analysis functions:")
        
        try:
            # Test análisis numérico
            numeric_sample = test_data['Saldo']
            numeric_analysis = mapper._analyze_numeric_content(numeric_sample)
            print(f"  ✓ Numeric analysis: {len(numeric_analysis)} patterns detected")
            
            # Test análisis de texto
            text_sample = test_data['Nombre Cuenta'].astype(str)
            text_analysis = mapper._analyze_text_content(text_sample, 'Nombre Cuenta')
            print(f"  ✓ Text analysis: {len(text_analysis)} patterns detected")
            
            # Test análisis de fechas
            date_sample = test_data['Fecha'].astype(str)
            date_analysis = mapper._analyze_date_content_improved(date_sample)
            print(f"  ✓ Date analysis: {len(date_analysis)} patterns detected")
            
            # Test nuevos análisis específicos
            vendor_analysis = mapper._analyze_vendor_id_content('Proveedor ID', test_data['Proveedor ID'].astype(str))
            print(f"  ✓ Vendor ID analysis: {len(vendor_analysis)} patterns detected")
            
            account_name_analysis = mapper._analyze_gl_account_name_content('Nombre Cuenta', test_data['Nombre Cuenta'].astype(str))
            print(f"  ✓ GL Account Name analysis: {len(account_name_analysis)} patterns detected")
            
        except Exception as e:
            print(f"  ⚠️ Content analysis test failed: {e}")
        
        print(f"\n✅ Updated FieldMapper test completed successfully!")
        print(f"💡 Note: Some features may be limited if field definitions couldn't be fully loaded")
        
        return mapper
        
    except Exception as e:
        print(f"❌ Updated mapper test failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    test_updated_field_mapper()