# accounting_data_processor.py - Módulo para procesamiento de datos contables reutilizable
# Funciones para limpieza numérica, cálculos de amount, debit/credit y validaciones

import pandas as pd
import re
from typing import Dict, List, Tuple, Any, Optional 
import logging
from collections import Counter

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
    def separate_datetime_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Separa campos que contienen fecha y hora combinados en campos separados
        Mantiene la lógica original con patrones de fecha expandidos
        """
        
        # ========== SEPARAR CAMPOS DATETIME COMBINADOS ==========
        try:
            print("🔧 Checking for combined DateTime fields...")
            
            def separate_datetime_field(df, field_name):
                """Separa un campo que contiene fecha y hora en dos campos separados - VERSIÓN COMPLETA"""
                if field_name not in df.columns:
                    return False
                
                sample_values = df[field_name].dropna().head(10)
                if len(sample_values) == 0:
                    return False
                
                # Verificar si contiene tanto fecha como hora
                datetime_detected = False
                pure_date_count = 0
                pure_time_count = 0
                
                for value in sample_values:
                    str_value = str(value).strip()
                    
                    # PRIMERO: Verificar si es una fecha pura sin componente de tiempo
                    # PATRONES EXPANDIDOS COMO PIDIÓ EL USUARIO
                    pure_date_patterns = [
                        # Formatos DD.MM.YYYY
                        r'^\d{1,2}\.\d{1,2}\.\d{4}$',
                        r'^\d{2}\.\d{2}\.\d{4}$',
                        
                        # Formatos DD/MM/YYYY
                        r'^\d{1,2}/\d{1,2}/\d{4}$',
                        r'^\d{2}/\d{2}/\d{4}$',
                        r'^\d{1,2}/\d{1,2}/\d{2}$',        # DD/MM/YY
                        
                        # Formatos DD-MM-YYYY
                        r'^\d{1,2}-\d{1,2}-\d{4}$',
                        r'^\d{2}-\d{2}-\d{4}$',
                        r'^\d{1,2}-\d{1,2}-\d{2}$',        # DD-MM-YY
                        
                        # Formatos YYYY-MM-DD (ISO)
                        r'^\d{4}-\d{1,2}-\d{1,2}$',
                        r'^\d{4}-\d{2}-\d{2}$',
                        
                        # Formatos YYYY/MM/DD
                        r'^\d{4}/\d{1,2}/\d{1,2}$',
                        r'^\d{4}/\d{2}/\d{2}$',
                        
                        # Formatos YYYY.MM.DD
                        r'^\d{4}\.\d{1,2}\.\d{1,2}$',
                        r'^\d{4}\.\d{2}\.\d{2}$',
                        
                        # Formatos sin separadores
                        r'^\d{8}$',                         # YYYYMMDD
                        r'^\d{6}$',                         # DDMMYY o YYMMDD
                        
                        # Formatos con espacios
                        r'^\d{1,2}\s+\d{1,2}\s+\d{4}$',    # DD MM YYYY
                        r'^\d{4}\s+\d{1,2}\s+\d{1,2}$',    # YYYY MM DD
                        
                        # Formatos de fecha con texto de mes
                        r'^\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}$',     # DD MMM YYYY
                        r'^\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$',  # DD Month YYYY
                        r'^\d{4}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}$',     # YYYY MMM DD
                        
                        # Formatos europeos
                        r'^\d{1,2}\s+(Ene|Feb|Mar|Abr|May|Jun|Jul|Ago|Sep|Oct|Nov|Dec)\s+\d{4}$',     # DD MMM YYYY (español)
                        r'^\d{1,2}\s+(Janeiro|Fevereiro|Marco|Abril|Maio|Junho|Julho|Agosto|Setembro|Outubro|Novembro|Dezembro)\s+\d{4}$',  # Portugués
                    ]
                    
                    # SEGUNDO: Verificar si es tiempo puro
                    # PATRONES EXPANDIDOS PARA TIEMPO
                    pure_time_patterns = [
                        r'^\d{1,2}:\d{2}:\d{2}$',           # HH:MM:SS
                        r'^\d{1,2}:\d{2}$',                 # HH:MM
                        r'^\d{1,2}:\d{2}:\d{2}\.\d+$',      # HH:MM:SS.microseconds
                        r'^\d{1,2}:\d{2}:\d{2} (AM|PM)$',   # HH:MM:SS AM/PM
                        r'^\d{1,2}:\d{2} (AM|PM)$',         # HH:MM AM/PM
                        r'^\d{1,2}h\d{2}m\d{2}s$',          # HHhMMmSSs
                        r'^\d{1,2}h\d{2}m$',                # HHhMMm
                        r'^\d{1,2}h\d{2}$',                 # HH:MM (con h)
                        r'^\d{6}$',                         # HHMMSS
                        r'^\d{4}$',                         # HHMM
                    ]
                    
                    # Si coincide con patrón de fecha pura, incrementar contador
                    if any(re.match(pattern, str_value, re.IGNORECASE) for pattern in pure_date_patterns):
                        pure_date_count += 1
                        continue
                    elif any(re.match(pattern, str_value, re.IGNORECASE) for pattern in pure_time_patterns):
                        pure_time_count += 1
                        continue
                    
                    # TERCERO: Solo si NO es fecha pura NI tiempo puro, verificar datetime combinado
                    # PATRONES EXPANDIDOS PARA DATETIME COMBINADO
                    combined_datetime_patterns = [
                        # Formato estándar con espacio
                        r'\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}',        # YYYY-MM-DD HH:MM
                        r'\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}',        # DD/MM/YYYY HH:MM
                        r'\d{1,2}-\d{1,2}-\d{4}\s+\d{1,2}:\d{2}',        # DD-MM-YYYY HH:MM
                        r'\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}',      # DD.MM.YYYY HH:MM
                        
                        # Formato ISO con T
                        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}',                # ISO format
                        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',          # ISO format con segundos
                        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z?',   # ISO format con microsegundos
                        
                        # Con múltiples espacios
                        r'\d{1,2}/\d{1,2}/\d{4}\s{2,}\d{1,2}:\d{2}',     # DD/MM/YYYY  HH:MM (espacios múltiples)
                        r'\d{4}-\d{2}-\d{2}\s{2,}\d{1,2}:\d{2}',         # YYYY-MM-DD  HH:MM
                        
                        # Con AM/PM
                        r'\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s*(AM|PM)',  # DD/MM/YYYY HH:MM AM/PM
                        r'\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM)',      # YYYY-MM-DD HH:MM AM/PM
                        
                        # Formato de base de datos
                        r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+',     # YYYY-MM-DD HH:MM:SS.microseconds
                        r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',          # YYYY-MM-DD HH:MM:SS
                        
                        # Formatos europeos con texto
                        r'\d{1,2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{1,2}:\d{2}',  # DD MMM YYYY HH:MM
                        r'\d{4} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2} \d{1,2}:\d{2}',  # YYYY MMM DD HH:MM
                        
                        # Otros separadores
                        r'\d{1,2}_\d{1,2}_\d{4}_\d{1,2}:\d{2}',          # DD_MM_YYYY_HH:MM
                        r'\d{4}_\d{2}_\d{2}_\d{2}:\d{2}',                # YYYY_MM_DD_HH:MM
                    ]
                    
                    if any(re.search(pattern, str_value, re.IGNORECASE) for pattern in combined_datetime_patterns):
                        datetime_detected = True
                        break
                
                # Evaluar resultados
                total_samples = len(sample_values)
                if total_samples == 0:
                    return False
                    
                pure_date_ratio = pure_date_count / total_samples
                pure_time_ratio = pure_time_count / total_samples
                
                # Si la mayoría son fechas puras, NO separar
                if pure_date_ratio >= 0.7:
                    print(f"   ℹ️ Field '{field_name}' contains pure dates (format like DD.MM.YYYY), NOT separating")
                    return False
                # Si la mayoría son tiempos puros, NO separar  
                elif pure_time_ratio >= 0.7:
                    print(f"   ℹ️ Field '{field_name}' contains pure times (format like HH:MM:SS), NOT separating")
                    return False
                # Solo separar si realmente detectamos datetime combinado
                elif not datetime_detected:
                    print(f"   ℹ️ Field '{field_name}' does not contain combined date+time, NOT separating")
                    return False
                
                print(f"   📅 Detected combined DateTime in '{field_name}', separating...")
                
                # Separar fecha y hora SOLO para valores que realmente son datetime combinados
                dates = []
                times = []
                
                for value in df[field_name]:
                    if pd.isna(value) or value == '':
                        dates.append('')
                        times.append('')
                        continue
                    
                    str_value = str(value).strip()
                    
                    # Verificar si este valor específico es fecha pura
                    if any(re.match(pattern, str_value, re.IGNORECASE) for pattern in pure_date_patterns):
                        dates.append(str_value)  # Mantener formato original
                        times.append('')
                        continue
                    
                    # Verificar si este valor específico es tiempo puro
                    if any(re.match(pattern, str_value, re.IGNORECASE) for pattern in pure_time_patterns):
                        dates.append('')  # No hay fecha
                        times.append(str_value)  # Mantener formato original
                        continue
                    
                    # Solo procesar como datetime combinado si realmente lo es
                    has_space_and_colon = ' ' in str_value and ':' in str_value
                    has_t_separator = 'T' in str_value and ':' in str_value
                    
                    if has_space_and_colon or has_t_separator:
                        try:
                            parsed_dt = pd.to_datetime(str_value, errors='raise')
                            
                            # Para fechas que SÍ tienen hora, convertir fecha a formato deseado
                            # Mantener formato DD.MM.YYYY si era el formato original
                            if '.' in str_value:
                                date_str = parsed_dt.strftime('%d.%m.%Y')
                            elif '/' in str_value:
                                date_str = parsed_dt.strftime('%d/%m/%Y')
                            elif '-' in str_value and str_value.startswith(str(parsed_dt.year)):
                                date_str = parsed_dt.strftime('%Y-%m-%d')
                            else:
                                date_str = parsed_dt.strftime('%d.%m.%Y')  # Default a formato con puntos
                            
                            # Extraer hora en formato estándar HH:MM:SS
                            time_str = parsed_dt.strftime('%H:%M:%S')
                            
                            dates.append(date_str)
                            times.append(time_str)
                            
                        except:
                            # Si no se puede parsear, mantener valor original en fecha y vacío en hora
                            dates.append(str_value)
                            times.append('')
                    else:
                        # No tiene formato combinado, mantener original
                        dates.append(str_value)
                        times.append('')
                
                # Determinar nombres de campos de destino
                if field_name == 'entry_date':
                    date_field = 'entry_date'
                    time_field = 'entry_time'
                elif field_name == 'entry_time':
                    date_field = 'entry_date' 
                    time_field = 'entry_time'
                else:
                    # Para otros campos, crear nombres derivados
                    date_field = f"{field_name}_date"
                    time_field = f"{field_name}_time"
                
                # Asignar los valores separados
                df[date_field] = dates
                
                # Para entry_time, siempre reemplazar el campo original con solo la hora
                if field_name in ['entry_time', 'entry_date']:
                    if field_name == 'entry_time':
                        # Reemplazar entry_time original con solo las horas
                        df['entry_time'] = times
                        print(f"   ✓ Replaced 'entry_time' with time-only values")
                    elif field_name == 'entry_date':
                        # Si entry_time no existe, crearlo; si existe y está vacío, llenarlo
                        if 'entry_time' not in df.columns:
                            df['entry_time'] = times
                        elif df['entry_time'].isna().all() or (df['entry_time'].astype(str).str.strip() == '').all():
                            df['entry_time'] = times
                        else:
                            # Si entry_time ya tiene datos válidos, no sobreescribir
                            print(f"   ⚠️ entry_time already has data, keeping original")
                else:
                    # Para otros campos, crear nombres derivados
                    if time_field not in df.columns or df[time_field].isna().all():
                        df[time_field] = times
                    else:
                        # Si ya existe time_field y tiene datos, crear uno nuevo
                        counter = 1
                        new_time_field = f"{time_field}_{counter}"
                        while new_time_field in df.columns:
                            counter += 1
                            new_time_field = f"{time_field}_{counter}"
                        df[new_time_field] = times
                        time_field = new_time_field
                
                print(f"   ✓ Separated into '{date_field}' and '{time_field}'")
                print(f"     Sample dates: {dates[:3]}")
                print(f"     Sample times: {times[:3]}")
                
                return True
            
            # Intentar separar entry_date si existe
            if 'entry_date' in df.columns:
                separate_datetime_field(df, 'entry_date')
                
            # Intentar separar entry_time si existe
            if 'entry_time' in df.columns:
                separate_datetime_field(df, 'entry_time')
            
            # También verificar posting_date por si acaso
            if 'posting_date' in df.columns:
                separate_datetime_field(df, 'posting_date')
                
        except Exception as e:
            print(f"⚠️ Error processing DateTime fields: {e}")

        print("✓ DateTime field separation completed")
        return df

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