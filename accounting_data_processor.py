# accounting_data_processor.py - Módulo para procesamiento de datos contables reutilizable
# Funciones para limpieza numérica, cálculos de amount, debit/credit y validaciones

import pandas as pd
import re
from typing import Dict, List, Tuple, Any
import logging

logger = logging.getLogger(__name__)

class AccountingDataProcessor:
    """Procesador reutilizable para datos contables con limpieza numérica y cálculos"""
    
    def __init__(self):
        self.stats = {
            'zero_filled_fields': 0,
            'debit_credit_calculated': 0,
            'debit_amounts_from_indicator': 0,
            'credit_amounts_from_indicator': 0,
            'amount_signs_adjusted': 0,
            'fields_cleaned': 0
        }
    
    def process_numeric_fields_and_calculate_amounts(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Función principal que procesa campos numéricos y calcula amounts según disponibilidad
        
        Returns:
            Tuple[pd.DataFrame, Dict]: DataFrame procesado y estadísticas
        """
        try:
            print(f"\n💰 PROCESSING NUMERIC FIELDS AND CALCULATING AMOUNTS")
            print(f"-" * 50)
            
            # Reset statistics
            self.stats = {key: 0 for key in self.stats.keys()}
            
            # 1. Limpiar campos numéricos existentes
            df = self._clean_existing_numeric_fields(df)
            
            # 2. Detectar escenarios y aplicar cálculos apropiados
            has_amount = 'amount' in df.columns
            has_debit = 'debit_amount' in df.columns
            has_credit = 'credit_amount' in df.columns
            has_indicator = 'debit_credit_indicator' in df.columns
            
            # Escenario 1: Tiene debit/credit pero no amount
            if not has_amount and has_debit and has_credit:
                df = self._calculate_amount_from_debit_credit(df)
            
            # Escenario 2: Tiene amount + indicator pero no debit/credit
            elif has_amount and has_indicator and not has_debit and not has_credit:
                df = self._calculate_debit_credit_from_amount_indicator(df)
             # NUEVO ESCENARIO 3: Tiene solo amount, sin indicator ni debit/credit
            elif has_amount and not has_indicator and not has_debit and not has_credit:
                df = self._handle_amount_only_scenario(df)
            
            # Escenario 4: Ya tiene todos los campos
            elif has_amount and has_debit and has_credit:
                print(f"ℹ️  All amount fields already exist - only numeric cleaning applied")
            
            # Escenario 4: Casos incompletos
            else:
                self._report_incomplete_scenarios(has_amount, has_debit, has_credit, has_indicator)
            
            # 3. Mostrar resumen
            self._show_processing_summary(df)
            
            return df, self.stats.copy()
            
        except Exception as e:
            logger.error(f"Error in accounting data processing: {e}")
            print(f"Error processing numeric fields: {e}")
            return df, self.stats.copy()
    
    def _clean_existing_numeric_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos numéricos existentes rellenando con ceros"""
        numeric_fields = ['amount', 'debit_amount', 'credit_amount']
        
        for field in numeric_fields:
            if field in df.columns:
                print(f"Cleaning numeric field: {field}")
                original_samples = df[field].dropna().head(3).tolist()
                print(f"   Original values: {original_samples}")
                
                # Aplicar limpieza con zero-fill
                df[field] = df[field].apply(self._clean_numeric_value_with_zero_fill)
                
                cleaned_samples = df[field].head(3).tolist()
                print(f"   Cleaned values:  {cleaned_samples}")
                
                # Contar zero-fills
                zero_count = (df[field] == 0.0).sum()
                self.stats['zero_filled_fields'] += zero_count
                self.stats['fields_cleaned'] += 1
                print(f"   Zero-filled count: {zero_count}")
        
        return df
    
    def _calculate_amount_from_debit_credit(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula amount = debit_amount - credit_amount"""
        print(f"💡 CALCULATING AMOUNT: amount = debit_amount - credit_amount")
        
        df['amount'] = df['debit_amount'] - df['credit_amount']
        
        print(f"   ✅ Amount calculated for {len(df)} rows")
        print(f"   Sample calculated amounts: {df['amount'].head(3).tolist()}")
        
        return df
    
    def _calculate_debit_credit_from_amount_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula debit_amount y credit_amount desde amount + debit_credit_indicator"""
        print(f"💡 CALCULATING DEBIT/CREDIT AMOUNTS: Using amount + debit_credit_indicator")
        
        # Inicializar columnas
        df['debit_amount'] = 0.0
        df['credit_amount'] = 0.0
        
        # Limpiar indicador
        df['debit_credit_indicator'] = df['debit_credit_indicator'].fillna('').astype(str).str.strip().str.upper()
        
        # Patrones de identificación
        debit_patterns = ['D', 'DEBE', 'DEBIT', 'DR', 'DB', '1', 'S']
        credit_patterns = ['C', 'H', 'HABER', 'CREDIT', 'CR', 'CD', '0', '-1', 'N']
        
        # Crear máscaras
        is_debit = df['debit_credit_indicator'].isin(debit_patterns)
        is_credit = df['debit_credit_indicator'].isin(credit_patterns)
        
        # Asignar valores según indicador
        df.loc[is_debit, 'debit_amount'] = df.loc[is_debit, 'amount'].abs()
        df.loc[is_debit, 'credit_amount'] = 0.0
        
        df.loc[is_credit, 'debit_amount'] = 0.0
        df.loc[is_credit, 'credit_amount'] = df.loc[is_credit, 'amount'].abs()
        
        # NUEVA FUNCIONALIDAD: Ajustar signos de amount
        print(f"💡 ADJUSTING AMOUNT SIGNS: Negative for credits, positive for debits")
        df.loc[is_credit, 'amount'] = -df.loc[is_credit, 'amount'].abs()
        df.loc[is_debit, 'amount'] = df.loc[is_debit, 'amount'].abs()
        
        # Estadísticas
        debit_assigned = is_debit.sum()
        credit_assigned = is_credit.sum()
        unassigned = len(df) - debit_assigned - credit_assigned
        
        print(f"   ✅ Debit amounts assigned: {debit_assigned}")
        print(f"   ✅ Credit amounts assigned: {credit_assigned}")
        print(f"   ✅ Amount signs adjusted: {debit_assigned + credit_assigned}")
        
        if unassigned > 0:
            print(f"   ⚠️  Unrecognized indicators: {unassigned}")
            unrecognized = df[~(is_debit | is_credit)]['debit_credit_indicator'].unique()[:5]
            print(f"       Examples: {list(unrecognized)}")
        
        # Actualizar estadísticas
        self.stats['debit_credit_calculated'] = debit_assigned + credit_assigned
        self.stats['debit_amounts_from_indicator'] = debit_assigned
        self.stats['credit_amounts_from_indicator'] = credit_assigned
        self.stats['amount_signs_adjusted'] = debit_assigned + credit_assigned
        
        # Mostrar muestras
        self._show_calculation_samples(df, is_debit, is_credit)
        
        return df
    
    def _show_calculation_samples(self, df: pd.DataFrame, is_debit: pd.Series, is_credit: pd.Series):
        """Muestra muestras de los cálculos realizados"""
        print(f"   Sample results:")
        sample_indices = df.index[:3]
        for idx in sample_indices:
            if idx < len(df):
                indicator = df.loc[idx, 'debit_credit_indicator']
                amount = df.loc[idx, 'amount']
                debit = df.loc[idx, 'debit_amount']
                credit = df.loc[idx, 'credit_amount']
                print(f"     Row {idx}: Indicator='{indicator}', Amount={amount}, Debit={debit}, Credit={credit}")
    
    def _report_incomplete_scenarios(self, has_amount: bool, has_debit: bool, 
                                   has_credit: bool, has_indicator: bool):
        """Reporta escenarios donde no se pueden hacer cálculos completos"""
        if has_amount and not has_indicator:
            print(f"⚠️  Cannot calculate debit/credit amounts - missing debit_credit_indicator")
        elif not has_amount and not (has_debit and has_credit):
            print(f"⚠️  Cannot calculate amount - missing debit_amount or credit_amount fields")
        else:
            print(f"ℹ️  No additional calculations needed with current field combination")
    
    def _show_processing_summary(self, df: pd.DataFrame):
        """Muestra resumen del procesamiento realizado"""
        if self.stats['fields_cleaned'] > 0:
            print(f"\n📊 NUMERIC FIELDS PROCESSING SUMMARY:")
            numeric_fields = ['amount', 'debit_amount', 'credit_amount']
            for field in numeric_fields:
                if field in df.columns:
                    non_null_count = df[field].notna().sum()
                    zero_count = (df[field] == 0.0).sum()
                    print(f"   {field}: {non_null_count} valid values, {zero_count} zeros")
    
    def _clean_numeric_value_with_zero_fill(self, value) -> float:
        """Limpia un valor numérico eliminando texto de moneda y convirtiendo a float, RELLENANDO CON 0"""
        try:
            if pd.isna(value) or value == '' or value is None:
                return 0.0  # CAMBIO PRINCIPAL: Devolver 0.0 en lugar de None
            
            # Convertir a string si no lo es
            str_value = str(value).strip()
            
            if not str_value or str_value.lower() in ['', 'nan', 'none', 'null']:
                return 0.0  # CAMBIO PRINCIPAL: Devolver 0.0 en lugar de None
            
            # Remover espacios múltiples
            str_value = re.sub(r'\s+', ' ', str_value)
            
            # Patrones para limpiar monedas y texto
            # Ejemplos: "1000 EUR", "1,500.50 USD", "$1000", "1000€", "EUR 1000"
            currency_patterns = [
                r'\b[A-Z]{3}\b',        # EUR, USD, GBP, etc.
                r'[$€£¥₹₽¢]',          # Símbolos de moneda
                r'\b(USD|EUR|GBP|JPY|CAD|AUD|CHF|CNY|SEK|NOK|DKK|PLN|CZK|HUF|BGN|RON|HRK|RUB|TRY|BRL|MXN|ARS|CLP|PEN|COP|UYU|PYG|BOB|VEF|GYD|SRD|TTD|JMD|BBD|BSD|KYD|XCD|AWG|ANG|CUP|DOP|GTQ|HNL|NIO|CRC|PAB|BZD|SVC|HTG)\b',  # Códigos ISO comunes
                r'\b(DOLLAR|EURO|POUND|YEN|PESO|REAL|FRANC|KRONA|KRONE|ZLOTY|FORINT|LEU|LIRA|RUBLE|YUAN|RUPEE)\b',  # Nombres de monedas en inglés
                r'\b(DOLAR|EUROS|LIBRA|YENES|PESOS|REALES|FRANCOS|CORONAS|RUBLOS|YUANES|RUPIAS)\b'  # Nombres en español
            ]
            
            # Aplicar limpieza de monedas
            cleaned_value = str_value
            for pattern in currency_patterns:
                cleaned_value = re.sub(pattern, '', cleaned_value, flags=re.IGNORECASE)
            
            # Limpiar caracteres no numéricos excepto punto, coma y signo menos
            cleaned_value = re.sub(r'[^0-9.,-]', '', cleaned_value)
            
            # Manejar signos negativos
            is_negative = cleaned_value.count('-') % 2 == 1  # Impar = negativo
            cleaned_value = cleaned_value.replace('-', '')
            
            if not cleaned_value or not any(c.isdigit() for c in cleaned_value):
                return 0.0  # CAMBIO PRINCIPAL: Devolver 0.0 en lugar de None
            
            # Detectar formato de número mejorado
            if '.' in cleaned_value and ',' in cleaned_value:
                # Ambos presentes: detectar cuál es el decimal
                last_dot = cleaned_value.rfind('.')
                last_comma = cleaned_value.rfind(',')
                
                if last_dot > last_comma:
                    # Punto como decimal: "1,234.56"
                    cleaned_value = cleaned_value.replace(',', '')
                else:
                    # Coma como decimal: "1.234,56"
                    cleaned_value = cleaned_value.replace('.', '').replace(',', '.')
            
            elif ',' in cleaned_value:
                # Solo comas
                comma_parts = cleaned_value.split(',')
                if len(comma_parts) == 2 and len(comma_parts[1]) <= 3:
                    # Probablemente decimal: "1234,56"
                    cleaned_value = cleaned_value.replace(',', '.')
                else:
                    # Probablemente separador de miles: "1,234"
                    cleaned_value = cleaned_value.replace(',', '')
            
            elif '.' in cleaned_value:
                # Solo puntos - LÓGICA MEJORADA
                dot_parts = cleaned_value.split('.')
                if len(dot_parts) >= 2:
                    last_part = dot_parts[-1]
                    # Si la última parte tiene 1-3 dígitos, probablemente es decimal
                    if len(last_part) <= 3 and last_part.isdigit():
                        # Formato europeo: "229.006.45" -> separadores de miles + decimal
                        # Unir todas las partes excepto la última como entero
                        integer_part = ''.join(dot_parts[:-1])
                        decimal_part = last_part
                        cleaned_value = f"{integer_part}.{decimal_part}"
                    else:
                        # Todos los puntos son separadores de miles: "1.234.567"
                        cleaned_value = cleaned_value.replace('.', '')
            
            elif '.' in cleaned_value:
                # Solo puntos - LÓGICA MEJORADA
                dot_parts = cleaned_value.split('.')
                if len(dot_parts) >= 2:
                    last_part = dot_parts[-1]
                    # Si la última parte tiene 1-3 dígitos, probablemente es decimal
                    if len(last_part) <= 3 and last_part.isdigit():
                        # Formato europeo: "229.006.45" -> separadores de miles + decimal
                        # Unir todas las partes excepto la última como entero
                        integer_part = ''.join(dot_parts[:-1])
                        decimal_part = last_part
                        cleaned_value = f"{integer_part}.{decimal_part}"
                    else:
                        # Todos los puntos son separadores de miles: "1.234.567"
                        cleaned_value = cleaned_value.replace('.', '')
            
            # Convertir a float
            if cleaned_value and cleaned_value not in ['.', ',', '-', '+']:
                result = float(cleaned_value)
                return -result if is_negative else result
            else:
                return 0.0  # CAMBIO PRINCIPAL: Devolver 0.0 en lugar de None
            
        except (ValueError, TypeError):
            # Si no se puede convertir, intentar extraer números
            try:
                # Buscar patrones numéricos más complejos
                numbers = re.findall(r'-?\d+[.,]?\d*', str(value))
                if numbers:
                    # Tomar el primer número encontrado y limpiarlo recursivamente
                    first_num = numbers[0].replace(',', '.')
                    return float(first_num)
                return 0.0  # CAMBIO PRINCIPAL: Devolver 0.0 en lugar de None
            except:
                return 0.0  # CAMBIO PRINCIPAL: Devolver 0.0 en lugar de None

# Funciones de utilidad para usar directamente
def clean_numeric_field(series: pd.Series, field_name: str = "field") -> pd.Series:
    """Función utilitaria para limpiar una serie numérica"""
    processor = AccountingDataProcessor()
    print(f"Cleaning numeric field: {field_name}")
    return series.apply(processor._clean_numeric_value_with_zero_fill)

def calculate_amount_from_debit_credit(debit_series: pd.Series, credit_series: pd.Series) -> pd.Series:
    """Función utilitaria para calcular amount desde debit y credit"""
    return debit_series - credit_series

def split_amount_by_indicator(amount_series: pd.Series, indicator_series: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Función utilitaria para separar amount en debit_amount y credit_amount según indicador
    
    Returns:
        Tuple[debit_series, credit_series, adjusted_amount_series]
    """
    processor = AccountingDataProcessor()
    df_temp = pd.DataFrame({
        'amount': amount_series,
        'debit_credit_indicator': indicator_series
    })
    
    df_processed = processor._calculate_debit_credit_from_amount_indicator(df_temp)
    
    return df_processed['debit_amount'], df_processed['credit_amount'], df_processed['amount']