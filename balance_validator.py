# balance_validator.py 


import pandas as pd
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class BalanceValidator:
    """
    Validador reutilizable para balances contables basado solo en amount
    """
    
    def __init__(self, tolerance: float = 0.01):
        """
        Args:
            tolerance: Tolerancia para diferencias decimales (default 0.01)
        """
        self.tolerance = tolerance
        self.validation_stats = {
            'balance_checks_performed': 0,
            'total_entries_checked': 0,
            'balanced_entries': 0,
            'unbalanced_entries': 0,
            'total_amount_sum': 0.0
        }
    
    def perform_comprehensive_balance_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Realiza validación completa de balances basada solo en amount
        
        Args:
            df: DataFrame con datos contables (requiere campo 'amount')
            
        Returns:
            Dict con reporte completo de validación
        """
        try:
            print(f"\n⚖️ PERFORMING AMOUNT-BASED BALANCE VALIDATION")
            print(f"-" * 45)
            
            # Reset stats
            self.validation_stats = {key: 0 for key in self.validation_stats.keys()}
            
            balance_report = {
                'total_amount_sum': 0.0,
                'total_balance_difference': 0.0,
                'is_balanced': False,
                'entry_balance_check': [],
                'unbalanced_entries': [],
                'entries_count': 0,
                'balanced_entries_count': 0,
                'validation_details': {},
                'tolerance_used': self.tolerance
            }
            
            # Verificar campos requeridos
            required_fields = self._check_required_fields(df)
            if not required_fields['has_required_fields']:
                balance_report['validation_details'] = required_fields
                return balance_report
            
            # 1. Validación de total general (suma de amounts debe ser 0)
            total_validation = self._validate_total_balance(df)
            balance_report.update(total_validation)
            
            # 2. Validación por asiento (suma de amounts por journal_entry_id debe ser 0)
            if 'journal_entry_id' in df.columns:
                entry_validation = self._validate_entry_level_balance(df)
                balance_report.update(entry_validation)
            else:
                print("   No journal_entry_id found - skipping entry-level validation")
            
            # 3. Actualizar estadísticas
            self.validation_stats['balance_checks_performed'] = 1
            balance_report['validation_stats'] = self.validation_stats.copy()
            
            return balance_report
            
        except Exception as e:
            logger.error(f"Error in balance validation: {e}")
            print(f"Error performing balance validations: {e}")
            return balance_report
    
    def evaluate_journal_entry_id_candidate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Evalúa qué tan bueno es el journal_entry_id para agrupar datos contables
        """
        try:
            if 'journal_entry_id' not in df.columns:
                return {'quality_score': 0.0, 'error': 'No journal_entry_id column found'}
            
            # Normalizar columnas con sufijo "_numeric"
            rename_map = {}
            if 'amount_numeric' in df.columns and 'amount' not in df.columns:
                rename_map['amount_numeric'] = 'amount'
            
            if rename_map:
                df = df.rename(columns=rename_map)

            # Solo validar con amount
            if 'amount' in df.columns:
                return self._evaluate_journal_id_with_amount_only(df)
            else:
                return {'quality_score': 0.0, 'error': 'No amount field found'}
                
        except Exception as e:
            return {'quality_score': 0.0, 'error': f'Evaluation failed: {e}'}

    def _evaluate_journal_id_with_amount_only(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Evaluación basada en amount: valida que amount por asiento sume cero
        """
        try:
            # Agrupar por journal_entry_id y sumar amount
            grouped = df.groupby('journal_entry_id').agg({
                'amount': 'sum'
            }).reset_index()
            
            total_entries = len(grouped)
            if total_entries == 0:
                return {'quality_score': 0.0, 'error': 'No entries found'}

            # Contar cuántos asientos cuadran (suma ≈ 0)
            balanced_entries = (abs(grouped['amount']) < self.tolerance).sum()
            
            # Score basado en proporción de asientos balanceados
            quality_score = balanced_entries / total_entries

            return {
                'quality_score': quality_score,
                'entries_count': total_entries,
                'balanced_entries': int(balanced_entries),
                'unbalanced_entries': int(total_entries - balanced_entries),
                'validation_type': 'amount_only'
            }
            
        except Exception as e:
            return {'quality_score': 0.0, 'error': f'Amount validation failed: {e}'}

    def _check_required_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Verifica que exista el campo necesario para validación (solo amount)
        """
        has_amount = 'amount' in df.columns
        has_journal_id = 'journal_entry_id' in df.columns
        
        if not has_amount:
            print("⚠️ Cannot perform balance validation - missing amount field")
            return {
                'has_required_fields': False,
                'missing_fields': ['amount'],
                'available_fields': {
                    'amount': has_amount,
                    'journal_entry_id': has_journal_id
                }
            }
        
        return {
            'has_required_fields': True,
            'available_fields': {
                'amount': has_amount,
                'journal_entry_id': has_journal_id
            }
        }
    
    def _validate_total_balance(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida balance total del DataFrame (suma de amounts debe ser 0)
        """
        total_amount = df['amount'].sum()
        is_balanced = abs(total_amount) < self.tolerance
        
        print(f"📊 TOTAL BALANCE CHECK:")
        print(f"   Total Amount:  {total_amount:,.2f}")
        print(f"   Is Balanced:   {'✅ YES' if is_balanced else '❌ NO'}")
        print(f"   Tolerance:     {self.tolerance}")
        
        # Actualizar estadísticas
        self.validation_stats['total_amount_sum'] = total_amount
        
        return {
            'total_amount_sum': total_amount,
            'total_balance_difference': total_amount,  # Para compatibilidad
            'is_balanced': is_balanced
        }
    
    def _validate_entry_level_balance(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida balance por cada asiento contable (suma de amounts por asiento debe ser 0)
        """
        print(f"\n📋 ENTRY-LEVEL BALANCE CHECK:")
        
        # Agrupar por journal_entry_id
        grouped = df.groupby('journal_entry_id').agg({
            'amount': 'sum'
        }).reset_index()
        
        # Calcular balance por asiento (debe ser ≈ 0)
        grouped['balance_difference'] = grouped['amount']
        grouped['is_balanced'] = abs(grouped['balance_difference']) < self.tolerance
        
        entries_count = len(grouped)
        balanced_count = grouped['is_balanced'].sum()
        unbalanced_entries = grouped[~grouped['is_balanced']]
        
        print(f"   Total Entries: {entries_count}")
        print(f"   Balanced:      {balanced_count}")
        print(f"   Unbalanced:    {len(unbalanced_entries)}")
        
        # Reportar asientos desbalanceados
        self._report_unbalanced_entries(unbalanced_entries)
        
        # Actualizar estadísticas
        self.validation_stats['total_entries_checked'] = entries_count
        self.validation_stats['balanced_entries'] = balanced_count
        self.validation_stats['unbalanced_entries'] = len(unbalanced_entries)
        
        return {
            'entries_count': entries_count,
            'balanced_entries_count': balanced_count,
            'unbalanced_entries': unbalanced_entries.to_dict('records'),
            'entry_balance_check': grouped.to_dict('records')
        }

    def _report_unbalanced_entries(self, unbalanced_df: pd.DataFrame):
        """
        Reporta detalles de asientos desbalanceados
        MODIFICADO: Solo usa amount
        """
        if len(unbalanced_df) == 0:
            return
            
        print(f"\n❌ UNBALANCED ENTRIES DETAILS:")
        max_display = min(10, len(unbalanced_df))
        
        for idx, row in unbalanced_df.head(max_display).iterrows():
            journal_id = row['journal_entry_id']
            amount_sum = row['amount']
            difference = row['balance_difference']
            
            print(f"   Entry {journal_id}: Amount Sum = {amount_sum:,.2f} (diff: {difference:,.2f})")
        
        if len(unbalanced_df) > max_display:
            print(f"   ... and {len(unbalanced_df) - max_display} more unbalanced entries")

    def generate_balance_summary_report(self, balance_report: Dict[str, Any]) -> str:
        """
        Genera resumen textual del reporte de balance
        MODIFICADO: Solo usa amount
        """
        lines = []
        lines.append("⚖️ AMOUNT-BASED BALANCE VALIDATION SUMMARY")
        lines.append("=" * 45)
        
        # Balance general
        is_balanced = balance_report.get('is_balanced', False)
        total_amount = balance_report.get('total_amount_sum', 0)
        
        lines.append(f"Total Balance: {'✅ BALANCED' if is_balanced else '❌ UNBALANCED'}")
        lines.append(f"Total Amount:  {total_amount:,.2f}")
        lines.append(f"Should be:     0.00 (±{balance_report.get('tolerance_used', 0.01)})")
        
        # Balance por asiento
        entries_count = balance_report.get('entries_count', 0)
        if entries_count > 0:
            balanced_count = balance_report.get('balanced_entries_count', 0)
            unbalanced_count = entries_count - balanced_count
            lines.append("")
            lines.append("📋 Entry-Level Analysis:")
            lines.append(f"Total Entries:     {entries_count}")
            lines.append(f"Balanced Entries:  {balanced_count}")
            lines.append(f"Unbalanced:        {unbalanced_count}")
            lines.append(f"Balance Rate:      {balanced_count/entries_count*100:.1f}%")
        
        return "\n".join(lines)

    def get_balance_quality_score(self, df: pd.DataFrame) -> float:
        """
        Calcula un score de calidad del balance (0.0 a 1.0)
        MODIFICADO: Solo usa amount
        """
        try:
            validation_result = self.perform_comprehensive_balance_validation(df)
            
            # Score basado en balance total y por asientos
            total_balanced = 1.0 if validation_result.get('is_balanced', False) else 0.0
            
            entries_count = validation_result.get('entries_count', 0)
            if entries_count > 0:
                balanced_entries = validation_result.get('balanced_entries_count', 0)
                entry_balance_rate = balanced_entries / entries_count
                # 50% peso al balance total, 50% al balance por asientos
                return (total_balanced * 0.5) + (entry_balance_rate * 0.5)
            else:
                # Si no hay asientos, solo el balance total
                return total_balanced
                
        except Exception as e:
            logger.error(f"Error calculating balance quality score: {e}")
            return 0.0


# Funciones de utilidad para uso directo
def validate_dataframe_balance(df: pd.DataFrame, tolerance: float = 0.01) -> Dict[str, Any]:
    """
    Función utilitaria para validar balance de un DataFrame
    MODIFICADO: Solo requiere amount
    """
    validator = BalanceValidator(tolerance=tolerance)
    return validator.perform_comprehensive_balance_validation(df)

def check_entry_balance(amount_sum: float, tolerance: float = 0.01) -> bool:
    """
    Función utilitaria para verificar balance de un asiento individual
    """
    return abs(amount_sum) < tolerance

def get_unbalanced_entries(df: pd.DataFrame, tolerance: float = 0.01) -> pd.DataFrame:
    """
    Función utilitaria para obtener solo los asientos desbalanceados
    """
    if 'journal_entry_id' not in df.columns:
        raise ValueError("DataFrame must have 'journal_entry_id' column")
    
    if 'amount' not in df.columns:
        raise ValueError("DataFrame must have 'amount' column")
    
    grouped = df.groupby('journal_entry_id').agg({
        'amount': 'sum'
    }).reset_index()
    
    grouped['balance_difference'] = grouped['amount']
    grouped['is_balanced'] = abs(grouped['balance_difference']) < tolerance
    
    return grouped[~grouped['is_balanced']]

def calculate_balance_quality_score(df: pd.DataFrame, tolerance: float = 0.01) -> float:
    """
    Función utilitaria para calcular score de calidad del balance
    """
    validator = BalanceValidator(tolerance=tolerance)
    return validator.get_balance_quality_score(df)