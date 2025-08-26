# config/custom_field_validators.py - VALIDADORES MEJORADOS CON APRENDIZAJE DE PATRONES

import pandas as pd
import re
import numpy as np
from typing import Union, List, Dict, Any
from datetime import datetime
import logging
import json
import os

logger = logging.getLogger(__name__)

class PatternValidatorRegistry:
    """Registro de validadores con aprendizaje de patrones"""
    
    def __init__(self):
        self.validators = {}
        self.learned_patterns_file = "config/validator_patterns.json"
        self.learned_patterns = self._load_learned_patterns()
    
    def _load_learned_patterns(self) -> Dict:
        """Carga patrones aprendidos"""
        try:
            if os.path.exists(self.learned_patterns_file):
                with open(self.learned_patterns_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading validator patterns: {e}")
        
        return {}
    
    def save_learned_patterns(self):
        """Guarda patrones aprendidos"""
        try:
            os.makedirs(os.path.dirname(self.learned_patterns_file), exist_ok=True)
            with open(self.learned_patterns_file, 'w', encoding='utf-8') as f:
                json.dump(self.learned_patterns, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving validator patterns: {e}")
    
    def register_validator(self, field_type: str, validator_func):
        """Registra un validador para un tipo de campo"""
        self.validators[field_type] = validator_func
    
    def validate_field(self, field_type: str, data: pd.Series) -> float:
        """Ejecuta validación para un tipo de campo"""
        if field_type in self.validators:
            return self.validators[field_type](data, self.learned_patterns)
        return 0.0
    
    def learn_pattern(self, field_type: str, data: pd.Series, pattern_info: Dict):
        """Aprende un patrón para un tipo de campo"""
        if field_type not in self.learned_patterns:
            self.learned_patterns[field_type] = {
                "patterns": [],
                "statistics": {},
                "examples": [],
                "confidence": 0.5
            }
        
        # Añadir nuevo patrón
        if pattern_info not in self.learned_patterns[field_type]["patterns"]:
            self.learned_patterns[field_type]["patterns"].append(pattern_info)
        
        # Actualizar ejemplos
        examples = data.dropna().astype(str).head(5).tolist()
        for example in examples:
            if example not in self.learned_patterns[field_type]["examples"]:
                self.learned_patterns[field_type]["examples"].append(example)
                
        # Limitar ejemplos a 20
        if len(self.learned_patterns[field_type]["examples"]) > 20:
            self.learned_patterns[field_type]["examples"] = \
                self.learned_patterns[field_type]["examples"][-20:]
        
        # Incrementar confianza
        current_conf = self.learned_patterns[field_type]["confidence"]
        self.learned_patterns[field_type]["confidence"] = min(current_conf + 0.05, 0.95)
        
        self.save_learned_patterns()

# Instancia global del registro
validator_registry = PatternValidatorRegistry()

def validate_journal_entry_id(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida identificadores de asientos contables con patrones aprendidos
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna().astype(str)
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        # Patrones base
        base_patterns = [
            r'^\d{6,15}$',                    # Números largos: 123456789012
            r'^JE\d{6,12}$',                  # JE123456789
            r'^AST\d{4,10}$',                 # AST20240001
            r'^[A-Z]{2,4}\d{6,12}$',         # JOUR123456789
            r'^\d{4}[A-Z]{2,4}\d{4,8}$',     # 2024JE00001234
            r'^[A-Z0-9]{8,20}$'              # General alfanumérico
        ]
        
        # Añadir patrones aprendidos si existen
        if learned_patterns and 'journal_entry_id' in learned_patterns:
            learned_journal_patterns = learned_patterns['journal_entry_id'].get('patterns', [])
            for pattern_info in learned_journal_patterns:
                if 'regex' in pattern_info:
                    base_patterns.append(pattern_info['regex'])
        
        for value in clean_series:
            value = str(value).strip()
            
            # Verificar patrones
            matched = False
            for pattern in base_patterns:
                try:
                    if re.match(pattern, value.upper()):
                        valid_count += 1
                        matched = True
                        break
                except re.error:
                    continue
            
            if not matched:
                # Validaciones adicionales
                if len(value) >= 6 and value.isdigit():
                    valid_count += 0.8  # Números largos probablemente válidos
                elif len(value) >= 4 and re.match(r'^[A-Z0-9]+$', value.upper()):
                    valid_count += 0.6  # Alfanumérico general
                
                # PENALIZAR si parece fecha
                if _is_date_like(value):
                    valid_count -= 0.5
                
                # PENALIZAR si es muy corto
                if len(value) < 3:
                    valid_count -= 0.3
        
        return max(0.0, min(valid_count / total_count, 1.0))
        
    except Exception as e:
        logger.warning(f"Error validating journal_entry_id: {e}")
        return 0.0

def validate_line_number(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida números de línea de asientos con patrones aprendidos
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna()
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        for value in clean_series:
            try:
                # Convertir a número
                num_value = float(str(value).replace(',', '.'))
                
                # Verificar rango válido para líneas de asiento
                if 1 <= num_value <= 9999 and num_value == int(num_value):
                    valid_count += 1
                elif str(value).strip().isdigit() and 1 <= int(str(value)) <= 99999:
                    valid_count += 0.8
                elif 0 <= num_value < 1:  # Decimales pequeños
                    valid_count += 0.3
                    
            except ValueError:
                value_str = str(value).strip()
                
                # PENALIZAR si parece fecha
                if _is_date_like(value_str):
                    valid_count -= 0.5
                
                # Verificar si es alfanumérico corto (posible ID de línea)
                if len(value_str) <= 5 and re.match(r'^[A-Z0-9]+$', value_str.upper()):
                    valid_count += 0.4
        
        return max(0.0, min(valid_count / total_count, 1.0))
        
    except Exception as e:
        logger.warning(f"Error validating line_number: {e}")
        return 0.0

def validate_posting_date(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida fechas efectivas con patrones aprendidos
    """
    return _validate_date_field(series, 'posting_date', learned_patterns)

def validate_entry_date(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida fechas de entrada con patrones aprendidos
    """
    return _validate_date_field(series, 'entry_date', learned_patterns)

def _validate_date_field(series: pd.Series, field_type: str, learned_patterns: Dict = None) -> float:
    """
    Validador genérico de fechas con aprendizaje de patrones
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna().astype(str)
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        # Patrones base de fechas - INCLUYE DD.MM.YYYY
        base_date_patterns = [
            r'^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$',  # dd/mm/yyyy (incluye puntos)
            r'^\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}$',    # yyyy/mm/dd (incluye puntos)
            r'^\d{1,2}-[A-Za-z]{3}-\d{2,4}$',          # dd-MMM-yyyy
            r'^\d{4}-\d{2}-\d{2}$',                     # yyyy-mm-dd
            r'^\d{2}/\d{2}/\d{4}$',                     # mm/dd/yyyy
            r'^\d{2}\.\d{2}\.\d{4}$',                   # DD.MM.YYYY ← ESPECÍFICO
            r'^\d{1,2}\.\d{1,2}\.\d{4}$',               # D.M.YYYY ← ESPECÍFICO
            r'^\d{4}\.\d{2}\.\d{2}$',                   # YYYY.MM.DD ← ESPECÍFICO
            r'^\d{8}$'                                   # yyyymmdd
        ]
        
        # Añadir patrones aprendidos
        if learned_patterns and field_type in learned_patterns:
            learned_date_patterns = learned_patterns[field_type].get('patterns', [])
            for pattern_info in learned_date_patterns:
                if 'regex' in pattern_info:
                    base_date_patterns.append(pattern_info['regex'])
        
        for value in clean_series:
            value_str = str(value).strip()
            
            # Verificar patrones de fecha
            date_matched = False
            for pattern in base_date_patterns:
                try:
                    if re.match(pattern, value_str):
                        valid_count += 1
                        date_matched = True
                        break
                except re.error:
                    continue
            
            if not date_matched:
                # Verificar si es parseable como fecha
                if _try_parse_date(value_str):
                    valid_count += 0.8
                # PENALIZAR si parece ID numérico largo
                elif len(value_str) > 8 and value_str.isdigit():
                    valid_count -= 0.3
        
        return valid_count / total_count
        
    except Exception as e:
        logger.warning(f"Error validating {field_type}: {e}")
        return 0.0

def validate_amount(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida importes monetarios con patrones aprendidos
    """
    return _validate_amount_field(series, 'amount', learned_patterns)

def validate_debit_amount(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida importes debe con patrones aprendidos
    """
    return _validate_amount_field(series, 'debit_amount', learned_patterns)

def validate_amount_credit(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida importes haber con patrones aprendidos
    """
    return _validate_amount_field(series, 'amount_credit', learned_patterns)

def _validate_amount_field(series: pd.Series, field_type: str, learned_patterns: Dict = None) -> float:
    """
    Validador genérico de importes con aprendizaje de patrones
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna().astype(str)
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        # Patrones base de importes
        base_amount_patterns = [
            r'^-?\d{1,3}(\.\d{3})*,\d{2}$',      # 1.234.567,89
            r'^-?\d{1,3}(,\d{3})*\.\d{2}$',      # 1,234,567.89
            r'^-?\d+[,\.]\d{1,4}$',              # 1234,56 o 1234.56
            r'^-?\d+$',                          # 1234
            r'^-?\d+\.\d+$',                     # 1234.56
            r'^-?\d+,\d+$',                      # 1234,56
            r'^-?\d{1,3}( \d{3})*[,\.]\d{2}$'    # 1 234 567,89
        ]
        
        # Añadir patrones aprendidos
        if learned_patterns and field_type in learned_patterns:
            learned_amount_patterns = learned_patterns[field_type].get('patterns', [])
            for pattern_info in learned_amount_patterns:
                if 'regex' in pattern_info:
                    base_amount_patterns.append(pattern_info['regex'])
        
        for value in clean_series:
            value_str = str(value).strip()
            
            # Verificar patrones de importe
            amount_matched = False
            for pattern in base_amount_patterns:
                try:
                    if re.match(pattern, value_str):
                        valid_count += 1
                        amount_matched = True
                        break
                except re.error:
                    continue
            
            if not amount_matched:
                # Verificar si es numérico convertible
                if _is_numeric(value_str):
                    valid_count += 0.7
                
                # PENALIZAR si contiene muchas letras (descripción)
                if len(value_str) > 2:
                    letter_count = sum(1 for c in value_str if c.isalpha())
                    if letter_count > len(value_str) * 0.5:
                        valid_count -= 0.5
                
                # PENALIZAR si parece fecha
                if _is_date_like(value_str):
                    valid_count -= 0.4
        
        return max(0.0, min(valid_count / total_count, 1.0))
        
    except Exception as e:
        logger.warning(f"Error validating {field_type}: {e}")
        return 0.0



def validate_debit_credit_indicator(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida indicadores debe/haber con patrones aprendidos
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna().astype(str)
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        # Indicadores válidos base
        valid_indicators = {
            'D', 'C', 'H', 'DEBE', 'HABER', 'DEBIT', 'CREDIT', 'DR', 'CR', 
            '1', '0', '-1', 'S', 'N', 'DB', 'CD', 'DEB', 'CRE'
        }
        
        # Añadir indicadores aprendidos
        if learned_patterns and 'debit_credit_indicator' in learned_patterns:
            examples = learned_patterns['debit_credit_indicator'].get('examples', [])
            for example in examples:
                valid_indicators.add(example.upper().strip())
        
        for value in clean_series:
            value_str = str(value).strip().upper()
            
            # Verificar indicadores conocidos
            if value_str in valid_indicators:
                valid_count += 1
            elif len(value_str) == 1 and value_str in 'DCHXSNYN+-':
                valid_count += 0.8
            elif value_str in ['YES', 'NO', 'SI', 'TRUE', 'FALSE']:
                valid_count += 0.6
            
            # PENALIZAR fuertemente si parece importe numérico grande
            if _is_numeric(value_str):
                try:
                    num_val = abs(float(value_str.replace(',', '.')))
                    if num_val > 10:
                        valid_count -= 0.8
                    elif num_val <= 1:
                        valid_count += 0.3  # Valores 0/1 son válidos como indicadores
                except:
                    pass
            
            # PENALIZAR si es muy largo
            if len(value_str) > 10:
                valid_count -= 0.5
        
        return max(0.0, min(valid_count / total_count, 1.0))
        
    except Exception as e:
        logger.warning(f"Error validating debit_credit_indicator: {e}")
        return 0.0

def validate_gl_account_number(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida números de cuenta contable con patrones aprendidos
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna().astype(str)
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        # Patrones base de cuentas contables
        account_patterns = [
            r'^\d{3,10}$',           # Solo números, 3-10 dígitos
            r'^\d{3,6}\.\d{2,4}$',   # Con punto: 1234.56
            r'^\d{3,6}-\d{2,4}$',    # Con guión: 1234-56
            r'^[A-Z]\d{3,9}$',       # Letra + números: A1234
            r'^\d{1,2}\.\d{2,3}\.\d{2,3}$'  # Formato jerárquico: 1.23.45
        ]
        
        # Añadir patrones aprendidos
        if learned_patterns and 'gl_account_number' in learned_patterns:
            learned_patterns_list = learned_patterns['gl_account_number'].get('patterns', [])
            for pattern_info in learned_patterns_list:
                if 'regex' in pattern_info:
                    account_patterns.append(pattern_info['regex'])
        
        for value in clean_series:
            value_str = str(value).strip()
            
            # Verificar patrones de cuenta
            account_matched = False
            for pattern in account_patterns:
                try:
                    if re.match(pattern, value_str):
                        valid_count += 1
                        account_matched = True
                        break
                except re.error:
                    continue
            
            if not account_matched:
                # Verificaciones adicionales
                if len(value_str) >= 3 and value_str.replace('.', '').replace('-', '').isdigit():
                    valid_count += 0.8
                elif len(value_str) >= 3 and re.match(r'^[A-Z0-9\.\-]+$', value_str.upper()):
                    valid_count += 0.6
                
                # PENALIZAR si parece período (números muy pequeños)
                try:
                    num_val = int(value_str.replace('.', '').replace('-', ''))
                    if 1 <= num_val <= 12:
                        valid_count -= 0.4
                except:
                    pass
                
                # PENALIZAR si es muy corto para ser cuenta
                if len(value_str) < 3:
                    valid_count -= 0.3
        
        return max(0.0, min(valid_count / total_count, 1.0))
        
    except Exception as e:
        logger.warning(f"Error validating gl_account_number: {e}")
        return 0.0

def validate_fiscal_year(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida años fiscales con patrones aprendidos
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna()
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        for value in clean_series:
            try:
                # Convertir a año
                year_value = int(float(str(value)))
                
                # Rango válido para años fiscales
                if 1950 <= year_value <= 2100:
                    valid_count += 1
                elif 50 <= year_value <= 99:  # Años en formato YY
                    valid_count += 0.8
                elif 0 <= year_value <= 49:   # Años 2000-2049 en formato YY
                    valid_count += 0.8
                    
            except ValueError:
                value_str = str(value).strip()
                
                # Verificar formatos de año fiscal
                if re.match(r'^FY\d{2,4}$', value_str.upper()):
                    valid_count += 0.9
                elif re.match(r'^\d{4}-\d{4}$', value_str):  # 2023-2024
                    valid_count += 0.9
        
        return valid_count / total_count
        
    except Exception as e:
        logger.warning(f"Error validating fiscal_year: {e}")
        return 0.0

def validate_period_number(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida períodos contables con patrones aprendidos
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna()
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        # Meses en español e inglés
        month_names = {
            'ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
            'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC',
            'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
            'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC',
            'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO',
            'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'
        }
        
        for value in clean_series:
            try:
                # Verificar si es período numérico
                period_number_value = int(float(str(value)))
                
                if 1 <= period_number_value <= 12:  # Meses
                    valid_count += 1
                elif 1 <= period_number_value <= 53:  # Semanas
                    valid_count += 0.6
                elif 1 <= period_number_value <= 4:   # Trimestres
                    valid_count += 0.8
                    
            except ValueError:
                value_str = str(value).upper().strip()
                
                # Verificar nombres de meses
                if any(month in value_str for month in month_names):
                    valid_count += 0.9
                elif re.match(r'^Q[1-4]$', value_str):  # Q1, Q2, etc.
                    valid_count += 0.9
                elif re.match(r'^T[1-4]$', value_str):  # T1, T2, etc.
                    valid_count += 0.9
                elif re.match(r'^\d{4}-\d{2}$', value_str):  # 2024-01
                    valid_count += 0.8
        
        return valid_count / total_count
        
    except Exception as e:
        logger.warning(f"Error validating period_number: {e}")
        return 0.0

def validate_je_header_description(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida descripciones de encabezado de asiento con patrones aprendidos
    """
    return _validate_description_field(series, 'description', learned_patterns)

def validate_je_line_description(series: pd.Series, learned_patterns: Dict = None) -> float:
    """
    Valida descripciones de línea de asiento con patrones aprendidos
    """
    return _validate_description_field(series, 'line_description', learned_patterns)

def _validate_description_field(series: pd.Series, field_type: str, learned_patterns: Dict = None) -> float:
    """
    Validador genérico de descripciones con aprendizaje de patrones
    """
    if series.empty:
        return 0.0
    
    try:
        clean_series = series.dropna().astype(str)
        if len(clean_series) == 0:
            return 0.0
        
        valid_count = 0
        total_count = len(clean_series)
        
        for value in clean_series:
            value_str = str(value).strip()
            
            # Validaciones básicas para descripción
            if len(value_str) >= 3:
                has_letters = any(c.isalpha() for c in value_str)
                has_spaces_or_long = ' ' in value_str or len(value_str) > 10
                
                if has_letters and has_spaces_or_long:
                    valid_count += 1
                elif has_letters:
                    valid_count += 0.7
                elif len(value_str) > 5:
                    valid_count += 0.4  # Podría ser descripción sin letras
            
            # PENALIZAR fuertemente si es completamente numérico
            if value_str.replace('.', '').replace(',', '').replace('-', '').replace(' ', '').isdigit():
                # Excepción: si es muy largo podría ser un ID descriptivo
                if len(value_str) < 8:
                    valid_count -= 0.6
                else:
                    valid_count -= 0.2
            
            # PENALIZAR si parece fecha
            if _is_date_like(value_str):
                valid_count -= 0.4
            
            # PENALIZAR si es muy corto para ser descripción
            if len(value_str) < 2:
                valid_count -= 0.5
        
        return max(0.0, min(valid_count / total_count, 1.0))
        
    except Exception as e:
        logger.warning(f"Error validating {field_type}: {e}")
        return 0.0

# ===== FUNCIONES DE UTILIDAD =====

def _is_date_like(value: str) -> bool:
    """Verifica si un valor parece una fecha"""
    value_str = str(value).strip()
    
    date_patterns = [
        r'^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$',
        r'^\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}$',
        r'^\d{1,2}-[a-zA-Z]{3}-\d{2,4}$',
        r'^\d{2,4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}$',
        r'^\d{8}$'  # YYYYMMDD
    ]
    
    return any(re.match(pattern, value_str) for pattern in date_patterns)

def _is_numeric(value: str) -> bool:
    """Verifica si un valor es numérico"""
    try:
        # Limpiar formato de número
        clean_value = str(value).replace(',', '.').replace(' ', '')
        # Permitir un signo negativo al inicio
        if clean_value.startswith('-'):
            clean_value = clean_value[1:]
        
        float(clean_value)
        return True
    except:
        return False

def _try_parse_date(value: str) -> bool:
    """Intenta parsear una fecha con varios formatos"""
    try:
        from dateutil import parser
        parser.parse(value)
        return True
    except:
        return False

# ===== REGISTRO DE VALIDADORES =====

# Registrar todos los validadores en el registro
validator_registry.register_validator('journal_entry_id', validate_journal_entry_id)
validator_registry.register_validator('line_number', validate_line_number)
validator_registry.register_validator('posting_date', validate_posting_date)
validator_registry.register_validator('entry_date', validate_entry_date)
validator_registry.register_validator('amount', validate_amount)
validator_registry.register_validator('debit_amount', validate_debit_amount)
validator_registry.register_validator('amount_credit', validate_amount_credit)
validator_registry.register_validator('debit_credit_indicator', validate_debit_credit_indicator)
validator_registry.register_validator('gl_account_number', validate_gl_account_number)
validator_registry.register_validator('fiscal_year', validate_fiscal_year)
validator_registry.register_validator('period_number', validate_period_number)
validator_registry.register_validator('description', validate_je_header_description)
validator_registry.register_validator('line_description', validate_je_line_description)

# Para compatibilidad con el código existente
AVAILABLE_VALIDATORS = {
    'validate_journal_entry_id': validate_journal_entry_id,
    'validate_line_number': validate_line_number,
    'validate_posting_date': validate_posting_date,
    'validate_entry_date': validate_entry_date,
    'validate_amount': validate_amount,
    'validate_debit_amount': validate_debit_amount,
    'validate_amount_credit': validate_amount_credit,

    'validate_debit_credit_indicator': validate_debit_credit_indicator,
    'validate_gl_account_number': validate_gl_account_number,
    'validate_fiscal_year': validate_fiscal_year,
    'validate_period_number': validate_period_number,
    'validate_je_header_description': validate_je_header_description,
    'validate_je_line_description': validate_je_line_description
}

def test_enhanced_validators():
    """
    Función de prueba para verificar que todos los validadores mejorados funcionan
    """
    print("🧪 Testing Enhanced AICPA Field Validators...")
    
    test_data = {
        'journal_entry_id': pd.Series(['JE123456789', '20240001234', 'AST12345678', '01/01/2024']),  # Último debe fallar
        'line_number': pd.Series([1, 2, 3, '18-dic-23']),  # Último debe fallar
        'posting_date': pd.Series(['01/01/2024', '2024-01-01', '01-JAN-24', '123456789']),  # Último debe fallar
        'amount': pd.Series(['-22.128.00', '-5.532.00', '22.128.00', 'EUR']),  # Último debe fallar
        'debit_credit_indicator': pd.Series(['D', 'C', 'H', '-22.128.00']),  # Último debe fallar
    }
    
    expected_results = {
        'journal_entry_id': [True, True, True, False],  # Fecha NO debe ser journal_entry_id
        'line_number': [True, True, True, False],  # Fecha NO debe ser line_number
        'posting_date': [True, True, True, False],  # Número NO debe ser fecha
        'amount': [True, True, True, False],  # Moneda NO debe ser amount
        'debit_credit_indicator': [True, True, True, False],  # Amount NO debe ser indicator
    }
    
    for field_type, data in test_data.items():
        print(f"\n🔍 Testing {field_type}:")
        
        for i, value in enumerate(data):
            single_series = pd.Series([value])
            score = validator_registry.validate_field(field_type, single_series)
            expected = expected_results[field_type][i]
            
            result_icon = "✅" if (score > 0.5) == expected else "❌"
            print(f"  {result_icon} Value '{value}' -> Score: {score:.3f} (Expected: {expected})")
    
    print(f"\n🎯 Enhanced validators test completed!")

def check_single_date_same_year_pattern(user_decisions: Dict, df: pd.DataFrame) -> Dict:
    """
    Comprueba si solo hay una fecha identificada como entry_date con alta confianza,
    y si todas las fechas son del mismo año, la cambia a posting_date.
    """
    # Buscar campos identificados como fechas
    date_fields = []
    entry_date_field = None
    
    for column_name, decision in user_decisions.items():
        field_type = decision['field_type']
        confidence = decision['confidence']
        
        if field_type in ['entry_date', 'posting_date']:
            date_fields.append((column_name, field_type, confidence))
            if field_type == 'entry_date':
                entry_date_field = (column_name, confidence)
    
    # Solo actuar si hay exactamente una fecha y es entry_date con alta confianza
    if len(date_fields) == 1 and entry_date_field and entry_date_field[1] >= 0.8:
        column_name = entry_date_field[0]
        print(f"\n🔍 CHECKING DATE PATTERN: Only one date field '{column_name}' identified as entry_date")
        
        try:
            # Obtener datos de la columna de fecha
            date_series = df[column_name].dropna()
            if len(date_series) == 0:
                return user_decisions
            
            # Parsear fechas y extraer años
            years = set()
            parsed_dates = 0
            
            for date_value in date_series:
                try:
                    from dateutil import parser
                    parsed_date = parser.parse(str(date_value))
                    years.add(parsed_date.year)
                    parsed_dates += 1
                except:
                    continue
            
            # Verificar si todas las fechas son del mismo año
            if len(years) == 1 and parsed_dates > 0:
                year = list(years)[0]
                print(f"   ✅ All {parsed_dates} dates are from the same year: {year}")
                print(f"   🔄 CHANGING field type: entry_date → posting_date")
                
                # Actualizar la decisión
                user_decisions[column_name]['field_type'] = 'posting_date'
                user_decisions[column_name]['confidence'] = min(user_decisions[column_name]['confidence'] + 0.1, 1.0)
                
                print(f"   📅 Field '{column_name}' reclassified as posting_date (confidence: {user_decisions[column_name]['confidence']:.3f})")
            else:
                print(f"   ℹ️ Dates span multiple years ({len(years)} years), keeping as entry_date")
                
        except Exception as e:
            print(f"   ⚠️ Error checking date pattern: {e}")
    
    return user_decisions

if __name__ == "__main__":
    test_enhanced_validators()