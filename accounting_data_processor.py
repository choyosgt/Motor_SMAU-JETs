# accounting_data_processor.py - MODIFICADO según nuevos requerimientos
# CAMBIOS PRINCIPALES:
# - Eliminados valores absolutos en todas las operaciones
# - Modificados los 4 escenarios según especificaciones
# - Escenario 2 eliminado
# - Nuevo manejo de indicadores debit_credit_indicator

import pandas as pd
import re
from typing import Dict, List, Tuple, Any, Optional
import logging
from collections import Counter

logger = logging.getLogger(__name__)

class AccountingDataProcessor:
    """
    Procesador reutilizable para datos contables con limpieza numérica y cálculos
    MODIFICADO: Sin valores absolutos, nuevos escenarios
    """
    
    def __init__(self):
        self.stats = {
            'zero_filled_fields': 0,
            'debit_credit_calculated': 0,
            'debit_amounts_from_indicator': 0,
            'credit_amounts_from_indicator': 0,
            'amount_signs_adjusted': 0,
            'fields_cleaned': 0,
            'parentheses_negatives_processed': 0,
            'amount_calculated': 0,
            'indicators_created': 0
        }

    def separate_datetime_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Separa campos que contienen fecha y hora combinados en campos separados
        VERSIÓN CORREGIDA - Mantiene toda la funcionalidad original pero sin bucles infinitos
        ASEGURA que todas las fechas se conviertan a formato YYYY-MM-DD
        """
        
        def _separate_single_datetime_field(df, field_name):
            """
            Función auxiliar para separar un campo datetime individual
            CORREGIDA: Returns consistentes y sin efectos secundarios
            """
            if field_name not in df.columns:
                return False  # ✅ CORREGIDO: Return boolean consistente
            
            sample_values = df[field_name].dropna().head(10)
            if len(sample_values) == 0:
                return False
            
            # Verificar si contiene tanto fecha como hora
            datetime_detected = False
            pure_date_count = 0
            pure_time_count = 0
            
            # Variables para capturar formato detectado
            detected_format = None
            detected_dayfirst = True  # Por defecto europeo
            
            # MANTENER TODA LA LÓGICA ORIGINAL DE DETECCIÓN
            for value in sample_values:
                str_value = str(value).strip()
                
                # PRIMERO: Verificar si es una fecha pura sin componente de tiempo
                pure_date_patterns = [
                    r'^\d{1,2}\.\d{1,2}\.\d{4}$',      # DD.MM.YYYY
                    r'^\d{1,2}/\d{1,2}/\d{4}$',       # DD/MM/YYYY
                    r'^\d{1,2}-\d{1,2}-\d{4}$',       # DD-MM-YYYY
                    r'^\d{4}-\d{2}-\d{2}$',           # YYYY-MM-DD
                    r'^\d{4}/\d{2}/\d{2}$',           # YYYY/MM/DD
                    r'^\d{4}\.\d{2}\.\d{2}$',         # YYYY.MM.DD
                    r'^\d{8}$',                       # YYYYMMDD
                ]
                
                # SEGUNDO: Verificar si es tiempo puro
                pure_time_patterns = [
                    r'^\d{1,2}:\d{2}:\d{2}$',         # HH:MM:SS
                    r'^\d{1,2}:\d{2}$',               # HH:MM
                    r'^\d{1,2}:\d{2}:\d{2}\.\d+$',    # HH:MM:SS.microseconds
                ]
                
                # Si coincide con patrón de fecha pura, incrementar contador
                if any(re.match(pattern, str_value) for pattern in pure_date_patterns):
                    pure_date_count += 1
                    # Capturar formato de fecha pura
                    if not detected_format:
                        if re.match(r'^\d{1,2}\.\d{1,2}\.\d{4}$', str_value):
                            detected_format = '%d.%m.%Y'
                            detected_dayfirst = True
                        elif re.match(r'^\d{4}-\d{2}-\d{2}$', str_value):
                            detected_format = '%Y-%m-%d'
                            detected_dayfirst = False
                        elif re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', str_value):
                            detected_dayfirst = True  # Asumir europeo
                        else:
                            detected_dayfirst = '.' in str_value or not str_value.startswith(('20', '19'))
                    continue
                elif any(re.match(pattern, str_value) for pattern in pure_time_patterns):
                    pure_time_count += 1
                    continue
                
                # TERCERO: Solo si NO es fecha pura NI tiempo puro, verificar datetime combinado
                combined_datetime_patterns = [
                    r'\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}',        # YYYY-MM-DD HH:MM
                    r'\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}',    # DD/MM/YYYY HH:MM
                    r'\d{1,2}-\d{1,2}-\d{4}\s+\d{1,2}:\d{2}',    # DD-MM-YYYY HH:MM
                    r'\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}',  # DD.MM.YYYY HH:MM
                    r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',      # ISO format
                ]
                
                for pattern in combined_datetime_patterns:
                    if re.search(pattern, str_value):
                        datetime_detected = True
                        # Capturar formato datetime combinado
                        if not detected_format:
                            if re.search(r'\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}', str_value):
                                detected_format = '%d.%m.%Y %H:%M:%S'
                                detected_dayfirst = True
                            elif re.search(r'\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}', str_value):
                                detected_format = '%Y-%m-%d %H:%M:%S'
                                detected_dayfirst = False
                            elif re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', str_value):
                                detected_format = '%Y-%m-%dT%H:%M:%S'
                                detected_dayfirst = False
                            else:
                                # Para otros, detectar por contenido
                                detected_dayfirst = '.' in str_value or ('/' in str_value and not str_value.startswith(('20', '19')))
                        break
                
                if datetime_detected:
                    break
            
            # Evaluar resultados - MANTENER TODA LA LÓGICA ORIGINAL
            total_samples = len(sample_values)
            if total_samples == 0:
                return False
                
            pure_date_ratio = pure_date_count / total_samples
            pure_time_ratio = pure_time_count / total_samples
            
            # NUEVA LÓGICA: Convertir fechas a YYYY-MM-DD incluso si no se separan
            def convert_to_standard_date(value, dayfirst=detected_dayfirst):
                """Convierte cualquier fecha a formato YYYY-MM-DD"""
                if pd.isna(value) or value == '':
                    return value
                
                str_value = str(value).strip()
                try:
                    parsed_dt = pd.to_datetime(str_value, dayfirst=dayfirst, errors='coerce')
                    if not pd.isna(parsed_dt):
                        return parsed_dt.strftime('%Y-%m-%d')
                    else:
                        return str_value
                except:
                    return str_value
            
            # Si la mayoría son fechas puras, convertir a YYYY-MM-DD pero NO separar
            if pure_date_ratio >= 0.7:
                print(f"   ℹ️ Field '{field_name}' contains pure dates, converting to YYYY-MM-DD format")
                # Convertir todas las fechas al formato estándar
                df[field_name] = df[field_name].apply(lambda x: convert_to_standard_date(x, detected_dayfirst))
                return False  # No separamos, solo convertimos formato
            # Si la mayoría son tiempos puros, NO separar  
            elif pure_time_ratio >= 0.7:
                print(f"   ℹ️ Field '{field_name}' contains pure times (format like HH:MM:SS), NOT separating")
                return False
            # Solo separar si realmente detectamos datetime combinado
            elif not datetime_detected:
                print(f"   ℹ️ Field '{field_name}' does not contain combined date+time")
                # Aún así, intentar convertir fechas a formato estándar si las hay
                df[field_name] = df[field_name].apply(lambda x: convert_to_standard_date(x, detected_dayfirst))
                return False
            
            print(f"   📅 Detected combined DateTime in '{field_name}', separating...")
            
            # MANTENER TODA LA LÓGICA ORIGINAL DE SEPARACIÓN
            dates = []
            times = []
            
            for value in df[field_name]:
                if pd.isna(value) or value == '':
                    dates.append('')
                    times.append('')
                    continue
                
                str_value = str(value).strip()
                
                # Verificar si este valor específico es fecha pura
                if any(re.match(pattern, str_value) for pattern in pure_date_patterns):
                    try:
                        parsed_dt = pd.to_datetime(str_value, dayfirst=detected_dayfirst, errors='coerce')
                        date_str = parsed_dt.strftime('%Y-%m-%d') if not pd.isna(parsed_dt) else str_value
                    except:
                        date_str = str_value

                    dates.append(date_str)
                    times.append('')
                    continue
                
                # Verificar si este valor específico es tiempo puro
                if any(re.match(pattern, str_value) for pattern in pure_time_patterns):
                    dates.append('')  # No hay fecha
                    times.append(str_value)  
                    continue
                
                # Solo procesar como datetime combinado si realmente lo es
                has_space_and_colon = ' ' in str_value and ':' in str_value
                has_t_separator = 'T' in str_value and ':' in str_value
                
                if has_space_and_colon or has_t_separator:
                    try:
                        # Usar formato detectado en lugar de parsing genérico
                        if detected_format:
                            try:
                                parsed_dt = pd.to_datetime(str_value, format=detected_format)
                            except:
                                # Si falla formato específico, usar dayfirst detectado como fallback
                                parsed_dt = pd.to_datetime(str_value, dayfirst=detected_dayfirst, errors='raise')
                        else:
                            # Fallback si no se detectó formato específico
                            parsed_dt = pd.to_datetime(str_value, dayfirst=detected_dayfirst, errors='raise')
                        
                        # SIEMPRE convertir fecha a YYYY-MM-DD
                        date_str = parsed_dt.strftime('%Y-%m-%d')
                        time_str = parsed_dt.strftime('%H:%M:%S')

                        dates.append(date_str)
                        times.append(time_str)
                        
                    except Exception as e:
                        # Si falla el parseo, mantener original pero intentar convertir fecha
                        try:
                            parsed_dt = pd.to_datetime(str_value, dayfirst=detected_dayfirst, errors='coerce')
                            date_str = parsed_dt.strftime('%Y-%m-%d') if not pd.isna(parsed_dt) else str_value
                        except:
                            date_str = str_value
                        dates.append(date_str)
                        times.append('')
                else:
                    try:
                        parsed_dt = pd.to_datetime(str_value, dayfirst=detected_dayfirst, errors='coerce')
                        date_str = parsed_dt.strftime('%Y-%m-%d') if not pd.isna(parsed_dt) else str_value
                    except:
                        date_str = str_value
                    dates.append(date_str)
                    times.append('')
            
            # Solo actualizar si realmente se procesó algo
            if any(time for time in times if time):
                # Determinar nombres de campos de salida
                if field_name == 'entry_date':
                    date_field = 'entry_date'
                    time_field = 'entry_time'
                elif field_name == 'entry_time':
                    date_field = 'entry_date'
                    time_field = 'entry_time'
                else:
                    # Para otros campos como posting_date, crear campos derivados
                    date_field = field_name
                    time_field = field_name.replace('_date', '_time').replace('date', 'time')
                    if time_field == date_field:
                        time_field = f"{field_name}_time"
                
                # Actualizar el DataFrame con fechas separadas
                df[date_field] = dates
                
                # Para tiempo, verificar si ya existe el campo
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
                print(f"     Sample dates: {[d for d in dates[:3] if d]}")
                print(f"     Sample times: {[t for t in times[:3] if t]}")
                
                return True  
            else:
                # Si no había tiempos para separar, al menos actualizar las fechas convertidas
                df[field_name] = dates
                return False
            
            return False  
        
        try:
            print("🔧 Checking for combined DateTime fields...")
            
            fields_to_process = ['entry_date', 'entry_time', 'posting_date']
            
            for field_name in fields_to_process:
                if field_name in df.columns:
                    success = _separate_single_datetime_field(df, field_name)
                    if success:
                        print(f"   ✅ Successfully processed '{field_name}'")
                    else:
                        print(f"   ℹ️ No processing needed for '{field_name}'")
                
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
            print(f"\n PROCESSING NUMERIC FIELDS AND CALCULATING AMOUNTS")
            
            # Reset statistics
            self.stats = {key: 0 for key in self.stats.keys()}
            
            # 1. Limpiar campos numéricos existentes 
            df = self._clean_existing_numeric_fields(df)
            
            # 2. Detectar escenarios y aplicar cálculos apropiados
            has_amount = 'amount' in df.columns
            has_debit = 'debit_amount' in df.columns
            has_credit = 'credit_amount' in df.columns
            has_indicator = (
            'debit_credit_indicator' in df.columns and 
            not df['debit_credit_indicator'].isna().all() and
            (df['debit_credit_indicator'] != '').any()
        )
            
            print(f"\n🔍 SCENARIO DETECTION:")
            print(f"   Has amount: {has_amount}")
            print(f"   Has debit_amount: {has_debit}")
            print(f"   Has credit_amount: {has_credit}")
            print(f"   Has debit_credit_indicator: {has_indicator}")
            
            # ESCENARIO 1: Tiene debit/credit pero no amount
            # Calcula amount como debit - credit y crea indicador
            if not has_amount and has_debit and has_credit:
                df = self.debit_credit_to_amount(df)
            
            # ESCENARIO 2: Tiene solo amount, sin indicator ni debit/credit
            # Solo crea indicador
            elif has_amount and not has_indicator and not has_debit and not has_credit:
                df = self.amount_only_create_indicator(df)
            
            # ESCENARIO 3: Ya tiene amount y debit_credit_indicator, no tiene que hacer nada
            elif has_amount and has_indicator:
                print(f"ℹ️  SCENARIO 3: Amount and indicator exist - no calculations needed")
                print(f"   Only numeric cleaning applied")

            # *** NUEVO ESCENARIO 4: Tiene amount + debit + credit pero no indicator ***
            elif has_amount and has_debit and has_credit and not has_indicator:
                df = self.create_indicator_from_debit_credit_pattern(df)
            
            # Casos incompletos o no reconocidos
            else:
                self._report_incomplete_scenarios(has_amount, has_debit, has_credit, has_indicator)
            
            # 3. Mostrar resumen final
            self._show_processing_summary(df)
            
            return df, self.stats.copy()
            
        except Exception as e:
            logger.error(f"Error in accounting data processing: {e}")
            print(f"Error processing numeric fields: {e}")
            return df, self.stats.copy()

    def debit_credit_to_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ESCENARIO 1: Tiene debit_amount y credit_amount pero no amount
        - Calcula amount = debit_amount - credit_amount (SIN valores absolutos)
        - Crea debit_credit_indicator: 'D' si debit != 0 y credit == 0, 'H' si debit == 0 y credit != 0
        """
        print(f"💡 SCENARIO 1: Calculating amount from debit/credit + creating indicator")
        
        # Limpiar campos debit y credit
        df['debit_amount'] = df['debit_amount'].apply(self._clean_numeric_value_with_zero_fill)
        df['credit_amount'] = df['credit_amount'].apply(self._clean_numeric_value_with_zero_fill)
        
        # Calcular amount SIN valores absolutos
        df['amount'] = df['debit_amount'] - df['credit_amount']
        
        # Crear indicador debit_credit_indicator
        df['debit_credit_indicator'] = ''
        
        # D si debit != 0 y credit == 0
        mask_debit = (df['credit_amount'] == 0)
        df.loc[mask_debit, 'debit_credit_indicator'] = 'D'
        
        # H si debit == 0 y credit > 0
        mask_credit = (df['debit_amount'] == 0) & (df['credit_amount'] != 0)
        df.loc[mask_credit, 'debit_credit_indicator'] = 'H'
        
        # Estadísticas
        debit_count = mask_debit.sum()
        credit_count = mask_credit.sum()
        
        print(f"   ✅ Amount calculated for {len(df)} rows")
        print(f"   ✅ Debit indicators (D): {debit_count}")
        print(f"   ✅ Credit indicators (H): {credit_count}")
        print(f"   Sample amounts: {df['amount'].head(3).tolist()}")
        
        self.stats['amount_calculated'] = len(df)
        self.stats['indicators_created'] = debit_count + credit_count
        
        return df
    
    def create_indicator_from_debit_credit_pattern(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tiene amount, debit_amount, credit_amount pero no indicator
        """
        
        df['debit_credit_indicator'] = ''
        
        # D si debit != 0 y credit == 0
        mask_debit =  (df['credit_amount'] == 0)
        df.loc[mask_debit, 'debit_credit_indicator'] = 'D'
        
        # H si debit == 0 y credit != 0  
        mask_credit = (df['debit_amount'] == 0) & (df['credit_amount'] != 0)
        df.loc[mask_credit, 'debit_credit_indicator'] = 'H'
        
        debit_count = mask_debit.sum()
        credit_count = mask_credit.sum()
        
        
        self.stats['indicators_created'] = debit_count + credit_count
        return df

    def amount_only_create_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ESCENARIO 3: Tiene solo amount sin indicador ni debit/credit
        - Solo crea la columna debit_credit_indicator
        """
        print(f"💡 SCENARIO 3: Creating debit_credit_indicator from amount only")
        
        # Limpiar campo amount
        df['amount'] = df['amount'].apply(self._clean_numeric_value_with_zero_fill)
        
        # Crear indicador basado en el signo del amount
        df['debit_credit_indicator'] = ''
        
        # D para amounts positivos (debitos)
        mask_positive = df['amount'] > 0
        df.loc[mask_positive, 'debit_credit_indicator'] = 'D'
        
        # H para amounts negativos (creditos)
        mask_negative = df['amount'] < 0
        df.loc[mask_negative, 'debit_credit_indicator'] = 'H'
        
        # Los amounts cero quedan sin indicador (string vacío)
        
        positive_count = mask_positive.sum()
        negative_count = mask_negative.sum()
        zero_count = (df['amount'] == 0).sum()
        
        print(f"   ✅ Debit indicators (D) created: {positive_count} (positive amounts)")
        print(f"   ✅ Credit indicators (H) created: {negative_count} (negative amounts)")
        print(f"   ℹ️  Zero amounts (no indicator): {zero_count}")
        
        self.stats['indicators_created'] = positive_count + negative_count
        
        return df

    def _clean_existing_numeric_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos numéricos existentes SIN aplicar valores absolutos"""
        print(f"\n🔧 CLEANING EXISTING NUMERIC FIELDS")
        
        # Campos numéricos típicos en datos contables
        numeric_fields = ['amount', 'debit_amount', 'credit_amount', 'debit', 'credit', 
                         'debe', 'haber', 'importe', 'valor']
        
        for field in numeric_fields:
            if field in df.columns:
                print(f"   Cleaning field: {field}")
                original_sample = df[field].dropna().head(3).tolist()
                
                # Contar valores con paréntesis ANTES del procesamiento
                parentheses_count = df[field].astype(str).str.contains(r'\(', na=False).sum()
                
                # Aplicar limpieza numérica SIN valores absolutos
                df[field] = df[field].apply(self._clean_numeric_value_with_zero_fill)
                
                cleaned_sample = df[field].head(3).tolist()
                print(f"     Original: {original_sample}")
                print(f"     Cleaned:  {cleaned_sample}")
                
                # Contar zero-fills
                zero_count = (df[field] == 0.0).sum()
                self.stats['zero_filled_fields'] += zero_count
                self.stats['fields_cleaned'] += 1
                self.stats['parentheses_negatives_processed'] += parentheses_count

                if parentheses_count > 0:
                    print(f"     📌 Paréntesis procesados: {parentheses_count}")
                
                self.stats['fields_cleaned'] += 1
        
        return df

    def _clean_numeric_value_with_zero_fill(self, value) -> float:
        """
        Limpia un valor numérico individual SIN aplicar valores absolutos
        - Convierte a float si es posible
        - Maneja paréntesis como valores negativos
        - Devuelve 0.0 para valores inválidos o vacíos
        - CORREGIDO: Maneja correctamente formatos europeos como 25.000.00
        """
        if pd.isna(value) or value == '' or str(value).strip() == '':
            return 0.0
        
        try:
            # Si ya es numérico, devolverlo tal como está (sin abs)
            if isinstance(value, (int, float)):
                return float(value)
            
            # Convertir a string para procesamiento
            str_value = str(value).strip()
            if str_value == '':
                return 0.0
            
            # Detectar si tiene paréntesis (indica negativo)
            is_parentheses_negative = bool(re.search(r'\([^)]*\d+[^)]*\)', str_value))
            
            # Limpiar: mantener solo dígitos, puntos, comas y signos menos
            cleaned = re.sub(r'[^\d.,\-]', '', str_value)
            
            if cleaned:
                # Manejar comas y puntos decimales
                if ',' in cleaned and '.' in cleaned:
                    # Formato como 1,234.56 vs 1.234,56
                    if cleaned.rfind(',') < cleaned.rfind('.'):
                        cleaned = cleaned.replace(',', '')
                    else:
                        # Formato como 1.234,56
                        last_comma = cleaned.rfind(',')
                        cleaned = cleaned[:last_comma].replace(',', '').replace('.', '') + '.' + cleaned[last_comma+1:]
                elif ',' in cleaned:
                    # Solo comas - asumir decimal si hay 2 dígitos después de la última coma
                    parts = cleaned.split(',')
                    if len(parts[-1]) <= 2:  # Cambio: <= 2 en lugar de == 2
                        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]
                    else:
                        cleaned = cleaned.replace(',', '')
                elif '.' in cleaned:
                    # NUEVA LÓGICA: Solo puntos - formato europeo
                    dot_parts = cleaned.split('.')
                    if len(dot_parts) >= 2:
                        last_part = dot_parts[-1]
                        # Si hay múltiples puntos Y la última parte tiene 1-2 dígitos → formato europeo
                        if len(dot_parts) > 2 and len(last_part) <= 2 and last_part.isdigit():
                            # 25.000.00 → 25000.00
                            integer_part = ''.join(dot_parts[:-1])
                            cleaned = f"{integer_part}.{last_part}"
                        elif len(dot_parts) == 2 and len(last_part) > 2:
                            # 1.234567 → separador de miles solamente
                            cleaned = cleaned.replace('.', '')
                        # Si len(dot_parts) == 2 and len(last_part) <= 2: mantener como decimal normal
                
                # Extraer el primer número (ahora debería ser el limpio)
                first_num = re.search(r'-?\d+\.?\d*', cleaned)
                if first_num:
                    result = float(first_num.group())
                    # Si había paréntesis, hacer negativo
                    if is_parentheses_negative:
                        result = -result
                    return result
                    
                return 0.0
        except:
            return 0.0
    def _report_incomplete_scenarios(self, has_amount: bool, has_debit: bool, 
                                   has_credit: bool, has_indicator: bool):
        """Reporta escenarios que no pueden ser procesados"""
        print(f"⚠️  INCOMPLETE SCENARIO DETECTED:")
        print(f"   Current fields: amount={has_amount}, debit={has_debit}, credit={has_credit}, indicator={has_indicator}")
        print(f"   No additional calculations can be performed with current field combination")

    def _show_processing_summary(self, df: pd.DataFrame):
        """Muestra resumen del procesamiento realizado"""
        print(f"\n📊 PROCESSING SUMMARY:")
        print(f"   Final shape: {df.shape}")
        print(f"   Fields cleaned: {self.stats['fields_cleaned']}")
        print(f"   Amount calculations: {self.stats['amount_calculated']}")
        print(f"   Indicators created: {self.stats['indicators_created']}")
        
        # Mostrar columnas finales
        numeric_cols = [col for col in df.columns if col in ['amount', 'debit_amount', 'credit_amount', 'debit_credit_indicator']]
        if numeric_cols:
            print(f"   Final numeric columns: {numeric_cols}")
            
            # Muestra de datos finales
            if not df.empty:
                print(f"\n📋 SAMPLE FINAL DATA:")
                sample_df = df[numeric_cols].head(3)
                for idx, row in sample_df.iterrows():
                    print(f"   Row {idx}: {dict(row)}")

    def _show_calculation_samples(self, df: pd.DataFrame, is_debit: pd.Series, is_credit: pd.Series):
        """Muestra muestras de los cálculos realizados"""
        print(f"   Sample results:")
        sample_indices = df.index[:3]
        for idx in sample_indices:
            if idx < len(df):
                indicator = df.loc[idx, 'debit_credit_indicator']
                amount = df.loc[idx, 'amount']
                debit = df.loc[idx, 'debit_amount'] if 'debit_amount' in df.columns else 'N/A'
                credit = df.loc[idx, 'credit_amount'] if 'credit_amount' in df.columns else 'N/A'
                print(f"     Row {idx}: Indicator='{indicator}', Amount={amount}, Debit={debit}, Credit={credit}")


# Funciones de utilidad para usar directamente
def clean_numeric_field(series: pd.Series, field_name: str = "field") -> pd.Series:
    """Función utilitaria para limpiar una serie numérica"""
    processor = AccountingDataProcessor()
    print(f"Cleaning numeric field: {field_name}")
    return series.apply(processor._clean_numeric_value_with_zero_fill)

def calculate_amount_from_debit_credit(debit_series: pd.Series, credit_series: pd.Series) -> pd.Series:
    """Función utilitaria para calcular amount desde debit y credit SIN valores absolutos"""
    return debit_series - credit_series

def create_debit_credit_indicator(amount_series: pd.Series) -> pd.Series:
    """Función utilitaria para crear indicador desde amount"""
    indicator = pd.Series('', index=amount_series.index)
    indicator[amount_series > 0] = 'D'
    indicator[amount_series < 0] = 'H'
    return indicator

