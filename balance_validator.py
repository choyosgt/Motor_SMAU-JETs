# balance_validator.py - Módulo reutilizable para validaciones de balance contable
# Funciones para validar balances por asiento y totales generales

import pandas as pd
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class BalanceValidator:
    """Validador reutilizable para balances contables"""
    
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
            'unbalanced_entries': 0
        }
    
    def perform_comprehensive_balance_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Realiza validación completa de balances por asiento y totales
        
        Args:
            df: DataFrame con datos contables
            
        Returns:
            Dict con reporte completo de validación
        """
        try:
            print(f"\n⚖️ PERFORMING COMPREHENSIVE BALANCE VALIDATION")
            print(f"-" * 45)
            
            # Reset stats
            self.validation_stats = {key: 0 for key in self.validation_stats.keys()}
            
            balance_report = {
                'total_debit_sum': 0.0,
                'total_credit_sum': 0.0,
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
            
            # 1. Validación de totales generales
            total_validation = self._validate_total_balance(df)
            balance_report.update(total_validation)
            
            # 2. Validación por asiento (si existe journal_entry_id)
            if 'journal_entry_id' in df.columns:
                entry_validation = self._validate_entry_level_balance(df)
                balance_report.update(entry_validation)
            else:
                print("   No journal_entry_id found - skipping entry-level validation")
            
            # 3. Validación cruzada (si existe campo amount)
            if 'amount' in df.columns:
                cross_validation = self._validate_cross_balance(df)
                balance_report['cross_validation'] = cross_validation
            
            # 4. Actualizar estadísticas
            self.validation_stats['balance_checks_performed'] = 1
            balance_report['validation_stats'] = self.validation_stats.copy()
            
            return balance_report
            
        except Exception as e:
            logger.error(f"Error in balance validation: {e}")
            print(f"Error performing balance validations: {e}")
            return balance_report
    
    def evaluate_journal_entry_id_candidate(self, df: pd.DataFrame) -> Dict[str, Any]:
        '''
        Evalúa qué tan bueno es el journal_entry_id actual para agrupar datos contables
        Maneja tanto el caso con debit/credit como solo amount
        '''
        try:
            if 'journal_entry_id' not in df.columns:
                return {'quality_score': 0.0, 'error': 'No journal_entry_id column found'}
            
            # ✅ Normalizar columnas con sufijo "_numeric"
            rename_map = {}
            if 'amount_numeric' in df.columns and 'amount' not in df.columns:
                rename_map['amount_numeric'] = 'amount'
            if 'debit_amount_numeric' in df.columns and 'debit_amount' not in df.columns:
                rename_map['debit_amount_numeric'] = 'debit_amount'
            if 'credit_amount_numeric' in df.columns and 'credit_amount' not in df.columns:
                rename_map['credit_amount_numeric'] = 'credit_amount'
            
            if rename_map:
                df = df.rename(columns=rename_map)

            # CASO 1: CON DEBIT Y CREDIT - usar validación completa
            if 'debit_amount' in df.columns and 'credit_amount' in df.columns:
                return self._evaluate_journal_id_with_debit_credit(df)

            # CASO 2: SOLO CON AMOUNT - validación alternativa  
            elif 'amount' in df.columns:
                return self._evaluate_journal_id_with_amount_only(df)

            # CASO 3: SIN CAMPOS CONTABLES
            else:
                return {'quality_score': 0.0, 'error': 'No accounting fields found'}
                
        except Exception as e:
            return {'quality_score': 0.0, 'error': f'Evaluation failed: {e}'}


    def _evaluate_journal_id_with_debit_credit(self, df: pd.DataFrame) -> Dict[str, Any]:
        '''Evaluación completa usando debit/credit + amount'''
        try:
            # Usar validación existente por asientos
            entry_validation = self._validate_entry_level_balance(df)
            
            # Obtener balance rate
            entries_count = entry_validation.get('entries_count', 0)
            balanced_entries_count = entry_validation.get('balanced_entries_count', 0)
            balance_rate = balanced_entries_count / entries_count if entries_count > 0 else 0
            
            # Obtener cross validation score si existe amount
            cross_validation_score = 1.0
            if 'amount' in df.columns:
                cross_validation = self._validate_cross_balance(df)
                cross_validation_score = cross_validation.get('match_rate', 1.0)
            
            # FÓRMULA FINAL: 60% balance + 40% cross validation
            quality_score = min(1.0, balance_rate * 0.6 + cross_validation_score * 0.4)
            
            return {
                'quality_score': quality_score,
                'balance_rate': balance_rate,
                'cross_validation_rate': cross_validation_score,
                'entries_count': entries_count,
                'validation_type': 'debit_credit'
            }
            
        except Exception as e:
            return {'quality_score': 0.0, 'error': f'Debit/credit validation failed: {e}'}

    def _evaluate_journal_id_with_amount_only(self, df: pd.DataFrame) -> Dict[str, Any]:
        '''Evaluación alternativa: solo valida si amount por asiento suma cero'''
        try:
            # Agrupar por journal_entry_id y sumar amount
            grouped = df.groupby('journal_entry_id').agg({
                'amount': 'sum'
            }).reset_index()
            
            total_entries = len(grouped)
            if total_entries == 0:
                return {'quality_score': 0.0, 'error': 'No entries found'}

            # Contar cuántos asientos cuadran (suma = 0)
            balanced_entries = (grouped['amount'].round(2) == 0).sum()
            
            # Score basado en proporción de asientos balanceados
            quality_score = balanced_entries / total_entries

            return {
                'quality_score': quality_score,
                'entries_count': total_entries,
                'balanced_entries': int(balanced_entries),
                'unbalanced_entries': int(total_entries - balanced_entries),
                'validation_type': 'amount_zero_check'
            }
            
        except Exception as e:
            return {'quality_score': 0.0, 'error': f'Amount-only validation failed: {e}'}

    
    def _check_required_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Verifica que existan los campos necesarios para validación"""
        has_debit = 'debit_amount' in df.columns
        has_credit = 'credit_amount' in df.columns
        has_amount = 'amount' in df.columns
        has_journal_id = 'journal_entry_id' in df.columns
        
        if not (has_debit and has_credit):
            print("⚠️ Cannot perform balance validation - missing debit_amount or credit_amount")
            return {
                'has_required_fields': False,
                'missing_fields': [
                    field for field, exists in [
                        ('debit_amount', has_debit),
                        ('credit_amount', has_credit)
                    ] if not exists
                ],
                'available_fields': {
                    'debit_amount': has_debit,
                    'credit_amount': has_credit,
                    'amount': has_amount,
                    'journal_entry_id': has_journal_id
                }
            }
        
        return {
            'has_required_fields': True,
            'available_fields': {
                'debit_amount': has_debit,
                'credit_amount': has_credit,
                'amount': has_amount,
                'journal_entry_id': has_journal_id
            }
        }
    
    def _validate_total_balance(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Valida balance total del DataFrame"""
        total_debit = df['debit_amount'].sum()
        total_credit = df['credit_amount'].sum()
        total_difference = total_debit - total_credit
        is_balanced = abs(total_difference) < self.tolerance
        
        print(f"📊 TOTAL BALANCE CHECK:")
        print(f"   Total Debit:  {total_debit:,.2f}")
        print(f"   Total Credit: {total_credit:,.2f}")
        print(f"   Difference:   {total_difference:,.2f}")
        print(f"   Is Balanced:  {'✅ YES' if is_balanced else '❌ NO'}")
        
        return {
            'total_debit_sum': total_debit,
            'total_credit_sum': total_credit,
            'total_balance_difference': total_difference,
            'is_balanced': is_balanced
        }
    
    def _validate_entry_level_balance(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Valida balance por cada asiento contable"""
        print(f"\n📋 ENTRY-LEVEL BALANCE CHECK:")
        
        # Agrupar por journal_entry_id
        grouped = df.groupby('journal_entry_id').agg({
            'debit_amount': 'sum',
            'credit_amount': 'sum'
        }).reset_index()
        
        # Calcular diferencia por asiento
        grouped['balance_difference'] = grouped['debit_amount'] - grouped['credit_amount']
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
    
    def _validate_cross_balance(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validación cruzada usando campo amount"""
        print(f"\n🔄 CROSS-VALIDATION WITH AMOUNT FIELD:")
        
        # Calcular amount esperado desde debit - credit
        calculated_amount = df['debit_amount'] - df['credit_amount']
        actual_amount = df['amount']
        
        # Comparar
        differences = abs(calculated_amount - actual_amount)
        matches = differences < self.tolerance
        match_count = matches.sum()
        
        print(f"   Amount field matches debit-credit: {match_count}/{len(df)}")
        print(f"   Match rate: {match_count/len(df)*100:.1f}%")
        
        # Mostrar discrepancias significativas
        significant_diffs = differences[~matches]
        if len(significant_diffs) > 0:
            print(f"   Significant discrepancies found: {len(significant_diffs)}")
            print(f"   Max difference: {significant_diffs.max():.2f}")
            
            # Mostrar algunos ejemplos
            problem_rows = df[~matches].head(3)
            for idx, row in problem_rows.iterrows():
                expected = row['debit_amount'] - row['credit_amount']
                actual = row['amount']
                print(f"     Row {idx}: Expected={expected:.2f}, Actual={actual:.2f}")
        
        return {
            'total_rows': len(df),
            'matching_rows': match_count,
            'match_rate': match_count/len(df),
            'discrepancies': len(significant_diffs),
            'max_difference': significant_diffs.max() if len(significant_diffs) > 0 else 0.0,
            'significant_differences': significant_diffs.head(10).tolist() if len(significant_diffs) > 0 else []
        }
    
    def _report_unbalanced_entries(self, unbalanced_entries: pd.DataFrame):
        """Reporta detalles de asientos desbalanceados"""
        if len(unbalanced_entries) > 0:
            print(f"\n⚠️ UNBALANCED ENTRIES DETECTED:")
            display_count = min(10, len(unbalanced_entries))
            
            for _, entry in unbalanced_entries.head(display_count).iterrows():
                entry_id = entry['journal_entry_id']
                debit_sum = entry['debit_amount']
                credit_sum = entry['credit_amount']
                difference = entry['balance_difference']
                print(f"   Entry {entry_id}: Debit {debit_sum:,.2f} - Credit {credit_sum:,.2f} = {difference:,.2f}")
            
            if len(unbalanced_entries) > display_count:
                remaining = len(unbalanced_entries) - display_count
                print(f"   ... and {remaining} more unbalanced entries")
        else:
            print(f"   ✅ All entries are balanced!")
    
    def generate_balance_summary_report(self, balance_report: Dict[str, Any]) -> str:
        """Genera resumen textual del reporte de balance"""
        lines = []
        lines.append("⚖️ BALANCE VALIDATION SUMMARY")
        lines.append("=" * 35)
        
        # Balance general
        is_balanced = balance_report.get('is_balanced', False)
        lines.append(f"Total Balance: {'✅ BALANCED' if is_balanced else '❌ UNBALANCED'}")
        lines.append(f"Total Debit:   {balance_report.get('total_debit_sum', 0):,.2f}")
        lines.append(f"Total Credit:  {balance_report.get('total_credit_sum', 0):,.2f}")
        lines.append(f"Difference:    {balance_report.get('total_balance_difference', 0):,.2f}")
        
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
        
        # Validación cruzada
        cross_val = balance_report.get('cross_validation')
        if cross_val:
            lines.append("")
            lines.append("🔄 Cross-Validation:")
            lines.append(f"Amount field match rate: {cross_val['match_rate']*100:.1f}%")
            if cross_val['discrepancies'] > 0:
                lines.append(f"Discrepancies found:     {cross_val['discrepancies']}")
        
        return "\n".join(lines)

# Funciones de utilidad para uso directo
def validate_dataframe_balance(df: pd.DataFrame, tolerance: float = 0.01) -> Dict[str, Any]:
    """Función utilitaria para validar balance de un DataFrame"""
    validator = BalanceValidator(tolerance=tolerance)
    return validator.perform_comprehensive_balance_validation(df)

def check_entry_balance(debit_amount: float, credit_amount: float, tolerance: float = 0.01) -> bool:
    """Función utilitaria para verificar balance de un asiento individual"""
    difference = abs(debit_amount - credit_amount)
    return difference < tolerance

def get_unbalanced_entries(df: pd.DataFrame, tolerance: float = 0.01) -> pd.DataFrame:
    """Función utilitaria para obtener solo los asientos desbalanceados"""
    if 'journal_entry_id' not in df.columns:
        raise ValueError("DataFrame must have 'journal_entry_id' column")
    
    if not ('debit_amount' in df.columns and 'credit_amount' in df.columns):
        raise ValueError("DataFrame must have 'debit_amount' and 'credit_amount' columns")
    
    grouped = df.groupby('journal_entry_id').agg({
        'debit_amount': 'sum',
        'credit_amount': 'sum'
    }).reset_index()
    
    grouped['balance_difference'] = grouped['debit_amount'] - grouped['credit_amount']
    grouped['is_balanced'] = abs(grouped['balance_difference']) < tolerance
    
    return grouped[~grouped['is_balanced']]