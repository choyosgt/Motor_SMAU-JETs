# csv_transformer_integrated.py - Transformador CSV con limpieza numérica integrada
# MEJORADO: Incluye procesamiento automático de campos numéricos contables

import pandas as pd
import os
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
import logging

# Importar el procesador de datos contables
from accounting_data_processor import AccountingDataProcessor

logger = logging.getLogger(__name__)

class IntegratedCSVTransformer:
    """Transformador CSV con limpieza numérica automática integrada"""
    def _ensure_results_directory(self):
        """Crea la carpeta results si no existe"""
        results_dir = "results"
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            print(f"📁 Created directory: {results_dir}/")
        return results_dir
    
    def __init__(self, output_prefix: str = "transformed", sort_by_journal_id: bool = True,
                 apply_numeric_processing: bool = True):
        """
        Args:
            output_prefix: Prefijo para archivos de salida
            sort_by_journal_id: Si ordenar por journal_entry_id
            apply_numeric_processing: Si aplicar limpieza numérica automática
        """
        self.output_prefix = output_prefix
        self.sort_by_journal_id = sort_by_journal_id
        self.apply_numeric_processing = apply_numeric_processing
        self.results_dir = self._ensure_results_directory()

        # Inicializar procesador de datos contables
        self.accounting_processor = AccountingDataProcessor()
        
        self.transformation_stats = {
            'original_columns': 0,
            'transformed_columns': 0,
            'header_columns': 0,
            'detail_columns': 0,
            'rows_processed': 0,
            'numeric_processing_applied': False,
            'numeric_fields_processed': 0
        }

    def create_header_detail_csvs(self, df: pd.DataFrame, user_decisions: Dict, 
                                 standard_fields: List[str]) -> Dict[str, Any]:
        """
        Crea CSV separados de cabecera y detalle CON limpieza numérica integrada
        
        Args:
            df: DataFrame original
            user_decisions: Decisiones de mapeo {columna_original: {field_type, confidence}}
            standard_fields: Lista de campos estándar
            
        Returns:
            Dict con información de archivos creados y estadísticas
        """
        try:
            print(f"\n🔄 CREATING HEADER AND DETAIL CSV FILES WITH NUMERIC PROCESSING")
            print(f"-" * 60)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Reset stats
            self.transformation_stats['original_columns'] = len(df.columns)
            self.transformation_stats['rows_processed'] = len(df)
            
            # 1. Crear DataFrame con columnas renombradas
            transformed_df = df.copy()
            column_mapping = {}
            
            # Crear mapeo de columnas originales a campos estándar
            for column_name, decision in user_decisions.items():
                standard_field = decision['field_type']
                column_mapping[column_name] = standard_field
            
            # Renombrar columnas en el DataFrame
            transformed_df = transformed_df.rename(columns=column_mapping)
            print(f"📄 Columns mapped: {len(column_mapping)}")
            
            # 2. *** NUEVA FUNCIONALIDAD: Aplicar limpieza numérica ***
            if self.apply_numeric_processing:
                transformed_df, numeric_stats = self._apply_numeric_processing(transformed_df)
                self.transformation_stats['numeric_processing_applied'] = True
                self.transformation_stats['numeric_fields_processed'] = numeric_stats.get('fields_cleaned', 0)
            # 2.5. *** SEPARACIÓN DE CAMPOS DATETIME ***
            print(f"\n📅 SEPARATING DATETIME FIELDS")
            print(f"-" * 40)

            # Usar el AccountingDataProcessor para separar campos datetime
            transformed_df = self.accounting_processor.separate_datetime_fields(transformed_df)
            
            # 3. Ordenar por journal_entry_id si existe y está habilitado
            if self.sort_by_journal_id and 'journal_entry_id' in transformed_df.columns:
                # Ordenamiento seguro que maneja tipos mixtos
                try:
                    transformed_df = transformed_df.sort_values('journal_entry_id', ascending=True)
                except TypeError:
                    print("⚠️ Mixed data types in journal_entry_id, converting to string for sorting")
                    transformed_df['journal_entry_id'] = transformed_df['journal_entry_id'].astype(str)
                    transformed_df = transformed_df.sort_values('journal_entry_id', ascending=True)
            
            # 4. LÓGICA SIMPLE: Definir campos fijos para header y detail
            header_field_definitions = [
                'journal_entry_id', 'posting_date', 'fiscal_year', 'period_number', 
                'prepared_by', 'entry_date', 'entry_time', 'description'
            ]
            
            detail_field_definitions = [
                'journal_entry_id', 'line_number', 'line_description', 
                'gl_account_number', 'gl_account_name', 'amount', 'debit_credit_indicator', 'vendor_id'
            ]
            
            # 5. Filtrar campos disponibles
            available_header_fields = [field for field in header_field_definitions 
                                     if field in transformed_df.columns]
            available_detail_fields = [field for field in detail_field_definitions 
                                     if field in transformed_df.columns]
            
            print(f"📋 Header fields available: {available_header_fields}")
            print(f"📋 Detail fields available: {available_detail_fields}")
            
            # 6. Crear archivos CSV
            header_file = self._create_header_csv(transformed_df, available_header_fields, timestamp)
            detail_file = self._create_detail_csv(transformed_df, available_detail_fields, timestamp)
            
            # 7. Actualizar estadísticas
            self.transformation_stats['transformed_columns'] = len(column_mapping)
            self.transformation_stats['header_columns'] = len(available_header_fields)
            self.transformation_stats['detail_columns'] = len(available_detail_fields)
            
            # 8. Preparar resultado
            result = {
                'success': True,
                'header_file': header_file,
                'detail_file': detail_file,
                'header_columns': available_header_fields,
                'detail_columns': available_detail_fields,
                'transformation_stats': self.transformation_stats,
                'total_standard_fields_mapped': len(user_decisions),
                'unmapped_standard_fields': [
                    f for f in standard_fields 
                    if f not in [d['field_type'] for d in user_decisions.values()]
                ],
                'numeric_processing_stats': getattr(self, '_last_numeric_stats', {})
            }
            
            self._show_transformation_summary(result)
            return result
            
        except Exception as e:
            logger.error(f"Error in CSV transformation: {e}")
            print(f"Error creating transformed CSVs: {e}")
            return {'success': False, 'error': str(e)}
    
    def _apply_numeric_processing(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Aplica procesamiento numérico utilizando AccountingDataProcessor
        
        Returns:
            Tuple[DataFrame procesado, estadísticas del procesamiento]
        """
        print(f"\n💰 APPLYING NUMERIC PROCESSING TO TRANSFORMED DATA")
        print(f"-" * 50)
        
        # Identificar campos numéricos que pueden estar presentes después del mapeo
        potential_numeric_fields = [
            'amount', 'line_number', 'debit_amount', 'credit_amount',
            'fiscal_year', 'period_number', 'gl_account_number'
        ]
        
        # Verificar qué campos numéricos están disponibles
        available_numeric_fields = [field for field in potential_numeric_fields 
                                  if field in df.columns]
        
        if not available_numeric_fields:
            print("ℹ️ No numeric fields found for processing")
            return df, {}
        
        print(f"🔢 Numeric fields found: {available_numeric_fields}")
        
        # Aplicar el procesamiento completo del AccountingDataProcessor
        processed_df, processing_stats = self.accounting_processor.process_numeric_fields_and_calculate_amounts(df)
        
        # Guardar estadísticas para el reporte final
        self._last_numeric_stats = processing_stats
        
        return processed_df, processing_stats
    
    def _create_header_csv(self, df: pd.DataFrame, header_fields: List[str], timestamp: str) -> Optional[str]:
        """Crea CSV de cabecera usando lógica simple"""
        if not header_fields:
            print("⚠️ No header fields available")
            return None
        
        print(f"📄 CREATING HEADER CSV")
        
        # Si tenemos journal_entry_id, agrupar para obtener valores únicos
        if 'journal_entry_id' in header_fields and 'journal_entry_id' in df.columns:
            # Crear DataFrame de cabecera agrupado (datos únicos por journal_entry_id)
            header_df = df[header_fields].drop_duplicates(subset=['journal_entry_id'])
            
            # Ordenar por journal_entry_id
            if self.sort_by_journal_id:
                header_df = header_df.sort_values('journal_entry_id', ascending=True)
                
            print(f"   ✅ Header grouped by journal_entry_id: {len(header_df)} unique entries")
        else:
            # Si no hay journal_entry_id, usar todas las filas
            header_df = df[header_fields].copy()
            print(f"   ⚠️ No journal_entry_id for grouping: {len(header_df)} rows")
        
        # Guardar archivo
        header_file = os.path.join(self.results_dir, f"{self.output_prefix}_header_{timestamp}.csv")
    
        header_df.to_csv(header_file, index=False, encoding='utf-8')
        print(f"   ✅ Header CSV saved: {header_file}")
        print(f"   📊 Columns: {', '.join(header_fields)}")
        
        return header_file
    
    def _create_detail_csv(self, df: pd.DataFrame, detail_fields: List[str], timestamp: str) -> Optional[str]:
        """Crea CSV de detalle usando lógica simple"""
        if not detail_fields:
            print("⚠️ No detail fields available")
            return None
        
        print(f"📄 CREATING DETAIL CSV")
        
        # Crear DataFrame de detalle (todas las líneas)
        detail_df = df[detail_fields].copy()
        
        # Ordenar por journal_entry_id si existe
        if self.sort_by_journal_id and 'journal_entry_id' in detail_df.columns:
            detail_df = detail_df.sort_values('journal_entry_id', ascending=True)
            print(f"   ✅ Detail sorted by journal_entry_id")
        
        # Guardar archivo
        detail_file = os.path.join(self.results_dir, f"{self.output_prefix}_detail_{timestamp}.csv")
        detail_df.to_csv(detail_file, index=False, encoding='utf-8')
        print(f"   ✅ Detail CSV saved: {detail_file}")
        print(f"   📊 Rows: {len(detail_df)}, Columns: {', '.join(detail_fields)}")
        
        return detail_file
    
    def _show_transformation_summary(self, result: Dict[str, Any]):
        """Muestra resumen de la transformación realizada CON estadísticas numéricas"""
        print(f"\n📊 TRANSFORMATION SUMMARY:")
        stats = result['transformation_stats']
        print(f"   Original columns:      {stats['original_columns']}")
        print(f"   Transformed columns:   {stats['transformed_columns']}")
        print(f"   Header fields:         {stats['header_columns']}")
        print(f"   Detail fields:         {stats['detail_columns']}")
        print(f"   Rows processed:        {stats['rows_processed']}")
        
        # NUEVA INFORMACIÓN: Estadísticas numéricas
        if stats['numeric_processing_applied']:
            print(f"   💰 NUMERIC PROCESSING APPLIED:")
            print(f"   Numeric fields processed: {stats['numeric_fields_processed']}")
            
            numeric_stats = result.get('numeric_processing_stats', {})
            if numeric_stats:
                print(f"   Zero-filled fields:       {numeric_stats.get('zero_filled_fields', 0)}")
                print(f"   Debit/Credit calculated:  {numeric_stats.get('indicators_created', 0)}")
                print(f"   Amount signs adjusted:    {numeric_stats.get('amount_signs_adjusted', 0)}")
        else:
            print(f"   Numeric processing:    Not applied")
        
        files_created = 0
        if result.get('header_file'):
            files_created += 1
        if result.get('detail_file'):
            files_created += 1
        print(f"   Files created:         {files_created}")
        
        if result['unmapped_standard_fields']:
            print(f"   Unmapped fields:       {len(result['unmapped_standard_fields'])}")
            print(f"     {result['unmapped_standard_fields']}")
    
    def create_single_transformed_csv(self, df: pd.DataFrame, user_decisions: Dict, 
                                    suffix: str = "transformed") -> Dict[str, Any]:
        """
        Crea un único CSV transformado CON limpieza numérica
        
        Args:
            df: DataFrame original  
            user_decisions: Decisiones de mapeo
            suffix: Sufijo para el archivo
            
        Returns:
            Dict con información del archivo creado
        """
        try:
            print(f"\n📄 CREATING SINGLE TRANSFORMED CSV FILE WITH NUMERIC PROCESSING")
            print(f"-" * 55)
            
            # Crear mapeo de columnas
            column_mapping = {col: decision['field_type'] for col, decision in user_decisions.items()}
            
            # Aplicar mapeo
            transformed_df = df.rename(columns=column_mapping)
            
            # *** APLICAR LIMPIEZA NUMÉRICA ***
            if self.apply_numeric_processing:
                transformed_df, numeric_stats = self._apply_numeric_processing(transformed_df)
                numeric_processing_applied = True
            else:
                numeric_stats = {}
                numeric_processing_applied = False
            
            # Ordenar si es necesario
            if self.sort_by_journal_id and 'journal_entry_id' in transformed_df.columns:
                transformed_df = transformed_df.sort_values('journal_entry_id').reset_index(drop=True)
                print(f"   Sorted by journal_entry_id")
            
            # Guardar archivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.results_dir, f"{self.output_prefix}_{suffix}_{timestamp}.csv")
            
            transformed_df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"   ✅ File saved: {output_file} ({len(transformed_df)} rows, {len(transformed_df.columns)} columns)")
            
            result = {
                'success': True,
                'output_file': output_file,
                'rows': len(transformed_df),
                'columns': len(transformed_df.columns),
                'mapped_fields': len(user_decisions),
                'numeric_processing_applied': numeric_processing_applied,
                'numeric_processing_stats': numeric_stats
            }
            
            # Mostrar estadísticas numéricas si se aplicaron
            if numeric_processing_applied and numeric_stats:
                print(f"   💰 Numeric processing stats:")
                print(f"     Fields cleaned: {numeric_stats.get('fields_cleaned', 0)}")
                print(f"     Zero-filled: {numeric_stats.get('zero_filled_fields', 0)}")
                print(f"     Calculations: {numeric_stats.get('indicators', 0)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating single transformed CSV: {e}")
            return {'success': False, 'error': str(e)}

# ==============================
# FUNCIONES DE UTILIDAD MEJORADAS
# ==============================

def transform_and_split_csv_with_numeric_cleaning(csv_file: str, column_mapping: Dict[str, str], 
                                                 output_prefix: str = "transformed",
                                                 apply_numeric_processing: bool = True) -> Dict[str, Any]:
    """
    Función utilitaria para transformar y dividir CSV con limpieza numérica automática
    
    Args:
        csv_file: Ruta del archivo CSV original
        column_mapping: Mapeo {columna_original: campo_estándar}
        output_prefix: Prefijo para archivos de salida
        apply_numeric_processing: Si aplicar limpieza numérica
    """
    # Convertir mapeo simple a formato de decisiones
    user_decisions = {
        col: {'field_type': field, 'confidence': 1.0} 
        for col, field in column_mapping.items()
    }
    
    # Campos estándar típicos (simplificado)
    standard_fields = [
        'journal_entry_id', 'line_number', 'description', 'line_description',
        'posting_date', 'fiscal_year', 'period_number', 'gl_account_number',
        'amount', 'debit_credit_indicator',
        'prepared_by', 'entry_date', 'entry_time', 'gl_account_name', 'vendor_id'
    ]
    
    df = pd.read_csv(csv_file)
    transformer = IntegratedCSVTransformer(
        output_prefix=output_prefix,
        apply_numeric_processing=apply_numeric_processing
    )
    
    return transformer.create_header_detail_csvs(df, user_decisions, standard_fields)

def simple_csv_rename_with_numeric_cleaning(csv_file: str, column_mapping: Dict[str, str], 
                                          output_file: Optional[str] = None,
                                          apply_numeric_processing: bool = True) -> Dict[str, Any]:
    """
    Función utilitaria para renombrar columnas de CSV CON limpieza numérica
    
    Args:
        csv_file: Archivo de entrada
        column_mapping: Mapeo {columna_original: nuevo_nombre}  
        output_file: Archivo de salida (opcional)
        apply_numeric_processing: Si aplicar limpieza numérica
        
    Returns:
        Dict con información del procesamiento
    """
    df = pd.read_csv(csv_file)
    df_renamed = df.rename(columns=column_mapping)
    
    # Aplicar limpieza numérica si está habilitada
    if apply_numeric_processing:
        processor = AccountingDataProcessor()
        df_renamed, numeric_stats = processor.process_numeric_fields_and_calculate_amounts(df_renamed)
    else:
        numeric_stats = {}
    
    if not output_file:
        base_name = os.path.splitext(csv_file)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = "_cleaned" if apply_numeric_processing else "_renamed"
        output_file = f"{base_name}{suffix}_{timestamp}.csv"
    
    df_renamed.to_csv(output_file, index=False, encoding='utf-8')
    
    return {
        'success': True,
        'output_file': output_file,
        'rows': len(df_renamed),
        'columns': len(df_renamed.columns),
        'numeric_processing_applied': apply_numeric_processing,
        'numeric_stats': numeric_stats
    }

# ==============================
# CLASE DE RETROCOMPATIBILIDAD
# ==============================

class CSVTransformer(IntegratedCSVTransformer):
    """Alias para retrocompatibilidad - incluye limpieza numérica por defecto"""
    def __init__(self, output_prefix: str = "transformed", sort_by_journal_id: bool = True):
        super().__init__(
            output_prefix=output_prefix,
            sort_by_journal_id=sort_by_journal_id,
            apply_numeric_processing=True  # Activado por defecto
        )