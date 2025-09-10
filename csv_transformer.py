# csv_transformer_integrated.py - Transformador CSV con limpieza numérica integrada
# MEJORADO: Incluye procesamiento automático de campos numéricos contables
# Y garantiza todas las columnas de header y detail

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
        results_dir = "results"
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
        return results_dir
    
    def __init__(self, output_prefix: str = "transformed", sort_by_journal_id: bool = True,
                 apply_numeric_processing: bool = True):
        self.output_prefix = output_prefix
        self.sort_by_journal_id = sort_by_journal_id
        self.apply_numeric_processing = apply_numeric_processing
        self.results_dir = self._ensure_results_directory()
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
    def _ensure_all_columns(self, df: pd.DataFrame, required_fields: List[str]) -> pd.DataFrame:
        """Asegura que todas las columnas requeridas existan en el DataFrame, rellenando con vacías si falta alguna."""
        for col in required_fields:
            if col not in df.columns:
                df[col] = ""
        return df[required_fields]
    
    def create_header_detail_csvs(self, df: pd.DataFrame, user_decisions: Dict, 
                                standard_fields: List[str]) -> Dict[str, Any]:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.transformation_stats['original_columns'] = len(df.columns)
            self.transformation_stats['rows_processed'] = len(df)
            
            # Renombrar columnas según decisiones del usuario
            transformed_df = df.copy()
            column_mapping = {col: decision['field_type'] for col, decision in user_decisions.items()}
            transformed_df = transformed_df.rename(columns=column_mapping)
            
            # Aplicar limpieza numérica si está habilitada
            if self.apply_numeric_processing:
                transformed_df, numeric_stats = self._apply_numeric_processing(transformed_df)
                self.transformation_stats['numeric_processing_applied'] = True
                self.transformation_stats['numeric_fields_processed'] = numeric_stats.get('fields_cleaned', 0)
            
            # Separar campos datetime
            transformed_df = self.accounting_processor.separate_datetime_fields(transformed_df)
            
            # Ordenar si journal_entry_id está presente
            if self.sort_by_journal_id and 'journal_entry_id' in transformed_df.columns:
                try:
                    transformed_df = transformed_df.sort_values('journal_entry_id', ascending=True)
                except TypeError:
                    transformed_df['journal_entry_id'] = transformed_df['journal_entry_id'].astype(str)
                    transformed_df = transformed_df.sort_values('journal_entry_id', ascending=True)
            
            # Definir columnas header y detail según staging
            header_field_definitions = [
                'journal_entry_id', 'journal_id', 'entry_date', 'entry_time',
                'posting_date', 'reversal_date', 'effective_date', 'description',
                'reference_number', 'source', 'entry_type', 'recurring_entry',
                'manual_entry', 'adjustment_entry', 'prepared_by', 'approved_by',
                'approval_date', 'entry_status', 'total_debit_amount', 'total_credit_amount',
                'line_count', 'fiscal_year', 'period_number', 'user_defined_01', 
                'user_defined_02', 'user_defined_03'
            ]

            detail_field_definitions = [
                'journal_entry_id', 'line_number', 'gl_account_number', 'amount',
                'debit_credit_indicator', 'business_unit', 'cost_center', 'department',
                'project_code', 'location', 'line_description', 'reference_number',
                'customer_id', 'vendor_id', 'product_id', 'user_defined_01',
                'user_defined_02', 'user_defined_03'
            ]


            # Crear DataFrames separados para header y detail, asegurando todas las columnas
            header_df = self._ensure_all_columns(transformed_df, header_field_definitions)
            detail_df = self._ensure_all_columns(transformed_df, detail_field_definitions)

            available_header_fields = header_field_definitions.copy()
            available_detail_fields = detail_field_definitions.copy()

            # Crear archivos CSV separados
            header_file = self._create_header_csv(header_df, available_header_fields, timestamp)
            detail_file = self._create_detail_csv(detail_df, available_detail_fields, timestamp)
            
            # Actualizar estadísticas
            self.transformation_stats['transformed_columns'] = len(column_mapping)
            self.transformation_stats['header_columns'] = len(available_header_fields)
            self.transformation_stats['detail_columns'] = len(available_detail_fields)
            
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
            
            return result
            
        except Exception as e:
            logger.error(f"Error in CSV transformation: {e}")
            return {'success': False, 'error': str(e)}

    
    def _apply_numeric_processing(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        potential_numeric_fields = [
            'amount', 'line_number', 'debit_amount', 'credit_amount',
            'fiscal_year', 'period_number', 'gl_account_number'
        ]
        available_numeric_fields = [field for field in potential_numeric_fields if field in df.columns]
        if not available_numeric_fields:
            return df, {}
        processed_df, processing_stats = self.accounting_processor.process_numeric_fields_and_calculate_amounts(df)
        self._last_numeric_stats = processing_stats
        return processed_df, processing_stats
    
    def _create_header_csv(self, df: pd.DataFrame, header_fields: List[str], timestamp: str) -> Optional[str]:
        if not header_fields:
            return None
        if 'journal_entry_id' in header_fields and 'journal_entry_id' in df.columns:
            header_df = df[header_fields].drop_duplicates(subset=['journal_entry_id'])
            if self.sort_by_journal_id:
                header_df = header_df.sort_values('journal_entry_id', ascending=True)
        else:
            header_df = df[header_fields].copy()
        header_df = self._ensure_all_columns(df[header_fields], header_fields)
        header_file = os.path.join(self.results_dir, f"{self.output_prefix}_header_{timestamp}.csv")
        header_df.to_csv(header_file, index=False, encoding='utf-8')
        return header_file
    
    def _create_detail_csv(self, df: pd.DataFrame, detail_fields: List[str], timestamp: str) -> Optional[str]:
        if not detail_fields:
            return None
        detail_df = df[detail_fields].copy()
        if self.sort_by_journal_id and 'journal_entry_id' in detail_df.columns:
            detail_df = detail_df.sort_values('journal_entry_id', ascending=True)
        detail_df = self._ensure_all_columns(df[detail_fields], detail_fields)
        detail_file = os.path.join(self.results_dir, f"{self.output_prefix}_detail_{timestamp}.csv")
        detail_df.to_csv(detail_file, index=False, encoding='utf-8')
        return detail_file
    
    def create_single_transformed_csv(self, df: pd.DataFrame, user_decisions: Dict, 
                                    suffix: str = "transformed") -> Dict[str, Any]:
        try:
            column_mapping = {col: decision['field_type'] for col, decision in user_decisions.items()}
            transformed_df = df.rename(columns=column_mapping)
            if self.apply_numeric_processing:
                transformed_df, numeric_stats = self._apply_numeric_processing(transformed_df)
                numeric_processing_applied = True
            else:
                numeric_stats = {}
                numeric_processing_applied = False
            if self.sort_by_journal_id and 'journal_entry_id' in transformed_df.columns:
                transformed_df = transformed_df.sort_values('journal_entry_id').reset_index(drop=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.results_dir, f"{self.output_prefix}_{suffix}_{timestamp}.csv")
            transformed_df.to_csv(output_file, index=False, encoding='utf-8')
            result = {
                'success': True,
                'output_file': output_file,
                'rows': len(transformed_df),
                'columns': len(transformed_df.columns),
                'mapped_fields': len(user_decisions),
                'numeric_processing_applied': numeric_processing_applied,
                'numeric_processing_stats': numeric_stats
            }
            return result
        except Exception as e:
            logger.error(f"Error creating single transformed CSV: {e}")
            return {'success': False, 'error': str(e)}

# ==============================
# FUNCIONES DE UTILIDAD
# ==============================

def transform_and_split_csv_with_numeric_cleaning(csv_file: str, column_mapping: Dict[str, str], 
                                                 output_prefix: str = "transformed",
                                                 apply_numeric_processing: bool = True) -> Dict[str, Any]:
    user_decisions = {col: {'field_type': field, 'confidence': 1.0} for col, field in column_mapping.items()}
    standard_fields = [
        'journal_entry_id', 'line_number', 'description', 'line_description',
        'posting_date', 'fiscal_year', 'period_number', 'gl_account_number',
        'amount', 'debit_credit_indicator',
        'prepared_by', 'entry_date', 'entry_time', 'gl_account_name', 'vendor_id'
    ]
    df = pd.read_csv(csv_file)
    transformer = IntegratedCSVTransformer(output_prefix=output_prefix,
                                           apply_numeric_processing=apply_numeric_processing)
    return transformer.create_header_detail_csvs(df, user_decisions, standard_fields)

def simple_csv_rename_with_numeric_cleaning(csv_file: str, column_mapping: Dict[str, str], 
                                          output_file: Optional[str] = None,
                                          apply_numeric_processing: bool = True) -> Dict[str, Any]:
    df = pd.read_csv(csv_file)
    df_renamed = df.rename(columns=column_mapping)
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
            apply_numeric_processing=True
        )
