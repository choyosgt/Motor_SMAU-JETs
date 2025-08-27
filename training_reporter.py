# training_reporter.py - Módulo reutilizable para generación de reportes de entrenamiento
# Funciones para crear reportes detallados de sesiones de training

import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class TrainingReporter:
    """Generador reutilizable de reportes de entrenamiento"""
    def _ensure_results_directory(self):
        """Crea la carpeta results si no existe"""
        results_dir = "results"
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            print(f"📁 Created directory: {results_dir}/")
        return results_dir
    
    def __init__(self, report_prefix: str = "training_report"):
        """
        Args:
            report_prefix: Prefijo para archivos de reporte
        """
        self.report_prefix = report_prefix
        self.report_sections = []
        self.results_dir = self._ensure_results_directory()
    
    def generate_comprehensive_training_report(
        self, 
        training_data: Dict[str, Any],
        output_file: Optional[str] = None
    ) -> str:
        """
        Genera reporte completo de sesión de entrenamiento
        
        Args:
            training_data: Datos de la sesión con keys:
                - csv_file, erp_hint, training_stats, user_decisions,
                - conflict_resolutions, balance_report, etc.
            output_file: Archivo de salida (opcional)
                
        Returns:
            Ruta del archivo de reporte generado
        """
        try:
            print(f"\n📊 GENERATING COMPREHENSIVE TRAINING REPORT")
            print(f"-" * 45)
            
            # Determinar archivo de salida
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = os.path.join(self.results_dir, f"{self.report_prefix}_{timestamp}.txt")
            else:
                # Si se proporciona output_file, asegurar que vaya a results/
                output_file = os.path.join(self.results_dir, os.path.basename(output_file))

            # Construir contenido del reporte
            report_content = self._build_report_content(training_data)
            
            # Guardar archivo
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            print(f"   ✅ Report saved: {output_file}")
            print(f"   📄 Sections included: {len(self.report_sections)}")
            
            return output_file
            
        except Exception as e:
            logger.error(f"Error generating training report: {e}")
            print(f"Could not generate report: {e}")
            return ""
    
    def _build_report_content(self, data: Dict[str, Any]) -> str:
        """Construye el contenido completo del reporte"""
        self.report_sections = []
        content_parts = []
        
        # 1. Header del reporte
        content_parts.append(self._create_report_header(data))
        self.report_sections.append("Header")
        
        # 2. Información de la sesión
        content_parts.append(self._create_session_info_section(data))
        self.report_sections.append("Session Info")
        
        # 3. Estadísticas de entrenamiento
        content_parts.append(self._create_statistics_section(data))
        self.report_sections.append("Statistics")
        
        # 4. Decisiones tomadas
        content_parts.append(self._create_decisions_section(data))
        self.report_sections.append("Decisions")
        
        # 5. Resolución de conflictos (si existe)
        if data.get('conflict_resolutions'):
            content_parts.append(self._create_conflicts_section(data))
            self.report_sections.append("Conflict Resolutions")
        
        # 6. Tabla de mapeo final
        content_parts.append(self._create_mapping_table_section(data))
        self.report_sections.append("Final Mapping Table")
        
        # 7. Reporte de balance (si existe)
        if data.get('balance_report'):
            content_parts.append(self._create_balance_section(data))
            self.report_sections.append("Balance Validation")
        
        # 8. Información de archivos CSV (si existe)
        if data.get('csv_info') or (data.get('header_file') and data.get('detail_file')):
            content_parts.append(self._create_csv_files_section(data))
            self.report_sections.append("Output Files")
        
        # 9. Patrones aprendidos (si existen)
        if data.get('learned_patterns'):
            content_parts.append(self._create_patterns_section(data))
            self.report_sections.append("Learned Patterns")
        
        return "\n\n".join(content_parts)
    
    def _create_report_header(self, data: Dict[str, Any]) -> str:
        """Crea el header del reporte"""
        training_mode = self._detect_training_mode(data)
        return f"""FIELD TRAINING SESSION REPORT - {training_mode.upper()}
{'=' * 60}

Generated: {datetime.now().isoformat()}
Training Mode: {training_mode}"""
    
    def _create_session_info_section(self, data: Dict[str, Any]) -> str:
        """Crea sección de información de la sesión"""
        lines = ["SESSION INFORMATION:"]
        lines.append(f"  CSV File: {data.get('csv_file', 'N/A')}")
        lines.append(f"  ERP Hint: {data.get('erp_hint', 'Auto-detect')}")
        
        # Detectar campos estándar
        standard_fields_count = 17  # default
        if 'training_stats' in data and 'standard_fields_count' in data['training_stats']:
            standard_fields_count = data['training_stats']['standard_fields_count']
        elif 'user_decisions' in data:
            unique_fields = set(d['field_type'] for d in data['user_decisions'].values())
            standard_fields_count = len(unique_fields)
        
        lines.append(f"  Standard Fields: {standard_fields_count}")
        lines.append(f"  Timestamp: {datetime.now().isoformat()}")
        
        return "\n".join(lines)
    
    def _create_statistics_section(self, data: Dict[str, Any]) -> str:
        """Crea sección de estadísticas"""
        lines = ["TRAINING STATISTICS:"]
        
        stats = data.get('training_stats', {})
        for key, value in stats.items():
            formatted_key = key.replace('_', ' ').title()
            lines.append(f"  {formatted_key}: {value}")
        
        return "\n".join(lines)
    
    def _create_decisions_section(self, data: Dict[str, Any]) -> str:
        """Crea sección de decisiones tomadas"""
        lines = ["MAPPING DECISIONS:"]
        
        decisions = data.get('user_decisions', {})
        if not decisions:
            lines.append("  No decisions recorded")
            return "\n".join(lines)
        
        # Agrupar por tipo de decisión si existe
        automatic_decisions = []
        manual_decisions = []
        conflict_decisions = []
        
        for column, decision in decisions.items():
            decision_type = decision.get('decision_type', 'unknown')
            confidence = decision.get('confidence', 0.0)
            field_type = decision.get('field_type', 'unknown')
            
            decision_line = f"  {column} → {field_type} (confidence: {confidence:.3f}, type: {decision_type})"
            
            if 'automatic' in decision_type.lower():
                automatic_decisions.append(decision_line)
            elif 'manual' in decision_type.lower():
                manual_decisions.append(decision_line)
            elif 'conflict' in decision_type.lower():
                conflict_decisions.append(decision_line)
            else:
                lines.append(decision_line)
        
        # Mostrar agrupadas
        if automatic_decisions:
            lines.append("  Automatic Decisions:")
            lines.extend(f"    {line[2:]}" for line in automatic_decisions)
        
        if manual_decisions:
            lines.append("  Manual Decisions:")
            lines.extend(f"    {line[2:]}" for line in manual_decisions)
        
        if conflict_decisions:
            lines.append("  Conflict Resolutions:")
            lines.extend(f"    {line[2:]}" for line in conflict_decisions)
        
        return "\n".join(lines)
    
    def _create_conflicts_section(self, data: Dict[str, Any]) -> str:
        """Crea sección de resolución de conflictos"""
        lines = ["CONFLICT RESOLUTIONS:"]
        
        conflicts = data.get('conflict_resolutions', {})
        if not conflicts:
            lines.append("  No conflicts to resolve")
            return "\n".join(lines)
        
        for field_type, resolution in conflicts.items():
            lines.append(f"  {field_type}:")
            lines.append(f"    Winner: {resolution.get('winner', 'N/A')}")
            lines.append(f"    Resolution type: {resolution.get('resolution_type', 'N/A')}")
            
            all_candidates = resolution.get('all_candidates', [])
            if all_candidates:
                lines.append(f"    All candidates: {all_candidates}")
        
        return "\n".join(lines)
    
    def _create_mapping_table_section(self, data: Dict[str, Any]) -> str:
        """Crea tabla de mapeo final"""
        lines = ["FINAL MAPPING TABLE:"]
        lines.append(f"{'Standard Field':<25} | {'Mapped Column':<30} | {'Confidence':<10}")
        lines.append(f"{'-'*25} | {'-'*30} | {'-'*10}")
        
        # Obtener campos estándar
        standard_fields = self._get_standard_fields_list(data)
        decisions = data.get('user_decisions', {})
        
        for standard_field in standard_fields:
            mapped_column = "No mapeado"
            confidence = "0.000"
            
            # Buscar mapeo para este campo estándar
            for column_name, decision in decisions.items():
                if decision['field_type'] == standard_field:
                    mapped_column = column_name
                    confidence = f"{decision['confidence']:.3f}"
                    break
            
            lines.append(f"{standard_field:<25} | {mapped_column:<30} | {confidence:<10}")
        
        return "\n".join(lines)
    
    def _create_balance_section(self, data: Dict[str, Any]) -> str:
        """Crea sección de validación de balance"""
        lines = ["BALANCE VALIDATION RESULTS:"]
        
        balance = data.get('balance_report', {})
        if not balance:
            lines.append("  No balance validation performed")
            return "\n".join(lines)
        
        # Balance total
        is_balanced = balance.get('is_balanced', False)
        lines.append(f"  Total Balance: {'✅ BALANCED' if is_balanced else '❌ UNBALANCED'}")
        lines.append(f"  Total Debit: {balance.get('total_debit_sum', 0):,.2f}")
        lines.append(f"  Total Credit: {balance.get('total_credit_sum', 0):,.2f}")
        lines.append(f"  Difference: {balance.get('total_balance_difference', 0):,.2f}")
        
        # Balance por asiento
        entries_count = balance.get('entries_count', 0)
        if entries_count > 0:
            balanced_count = balance.get('balanced_entries_count', 0)
            lines.append(f"  Entries Checked: {entries_count}")
            lines.append(f"  Balanced Entries: {balanced_count}")
            lines.append(f"  Unbalanced Entries: {entries_count - balanced_count}")
            
            # Mostrar algunos asientos desbalanceados
            unbalanced = balance.get('unbalanced_entries', [])
            if unbalanced and len(unbalanced) > 0:
                lines.append("  Unbalanced Entry Examples:")
                for entry in unbalanced[:5]:  # Primeros 5
                    entry_id = entry.get('journal_entry_id', 'N/A')
                    debit = entry.get('debit_amount', 0)
                    credit = entry.get('credit_amount', 0)
                    diff = entry.get('balance_difference', 0)
                    lines.append(f"    Entry {entry_id}: Debit {debit:,.2f} - Credit {credit:,.2f} = {diff:,.2f}")
        
        return "\n".join(lines)
    
    def _create_csv_files_section(self, data: Dict[str, Any]) -> str:
        """Crea sección de archivos CSV generados"""
        lines = ["OUTPUT FILES CREATED:"]
        
        # Archivos directos
        if data.get('header_file'):
            lines.append(f"  Header CSV: {data['header_file']}")
        
        if data.get('detail_file'):
            lines.append(f"  Detail CSV: {data['detail_file']}")
        
        # Información de CSV si existe
        csv_info = data.get('csv_info', {})
        if csv_info:
            if csv_info.get('header_columns'):
                lines.append(f"  Header columns: {', '.join(csv_info['header_columns'])}")
            
            if csv_info.get('detail_columns'):
                lines.append(f"  Detail columns: {', '.join(csv_info['detail_columns'])}")
        
        return "\n".join(lines)
    
    def _create_patterns_section(self, data: Dict[str, Any]) -> str:
        """Crea sección de patrones aprendidos"""
        lines = ["LEARNED PATTERNS:"]
        
        patterns = data.get('learned_patterns', {})
        if not patterns:
            lines.append("  No new patterns learned")
            return "\n".join(lines)
        
        # Nuevos sinónimos
        new_synonyms = data.get('new_synonyms', {})
        if new_synonyms:
            lines.append("  New Synonyms:")
            for field_type, synonyms in new_synonyms.items():
                lines.append(f"    {field_type}: {synonyms}")
        
        # Nuevos patrones regex
        new_regex = data.get('new_regex_patterns', {})
        if new_regex:
            lines.append("  New Regex Patterns:")
            for field_type, pattern_info in new_regex.items():
                if isinstance(pattern_info, dict) and 'regex' in pattern_info:
                    lines.append(f"    {field_type}: {pattern_info['regex']}")
                else:
                    lines.append(f"    {field_type}: {pattern_info}")
        
        return "\n".join(lines)
    
    def _detect_training_mode(self, data: Dict[str, Any]) -> str:
        """Detecta el modo de entrenamiento usado"""
        # Buscar pistas en los datos
        if 'training_mode' in data:
            return data['training_mode']
        
        decisions = data.get('user_decisions', {})
        if not decisions:
            return "Unknown"
        
        # Analizar tipos de decisión
        decision_types = [d.get('decision_type', '') for d in decisions.values()]
        
        if any('automatic' in dt.lower() for dt in decision_types):
            return "Automatic"
        elif any('manual' in dt.lower() for dt in decision_types):
            return "Manual Confirmation"
        else:
            return "Interactive"
    
    def _get_standard_fields_list(self, data: Dict[str, Any]) -> List[str]:
        """Obtiene lista de campos estándar"""
        # Campos estándar por defecto
        default_fields = [
            'journal_entry_id', 'line_number', 'description', 'line_description',
            'posting_date', 'fiscal_year', 'period_number', 'gl_account_number',
            'amount', 'debit_amount', 'credit_amount', 'debit_credit_indicator',
            'prepared_by', 'entry_date', 'entry_time', 'gl_account_name', 'vendor_id'
        ]
        
        # Intentar obtener de los datos
        if 'standard_fields' in data:
            return data['standard_fields']
        
        # O extraer de decisiones
        decisions = data.get('user_decisions', {})
        if decisions:
            mapped_fields = set(d['field_type'] for d in decisions.values())
            # Combinar campos mapeados con estándar para mostrar completo
            all_fields = set(default_fields) | mapped_fields
            return sorted(all_fields)
        
        return default_fields

# Funciones de utilidad para uso directo
def generate_simple_report(csv_file: str, user_decisions: Dict, 
                         training_stats: Dict, output_file: Optional[str] = None) -> str:
    """
    Función utilitaria para generar reporte simple
    
    Args:
        csv_file: Archivo CSV procesado
        user_decisions: Decisiones de mapeo
        training_stats: Estadísticas del entrenamiento
        output_file: Archivo de salida opcional
    """
    training_data = {
        'csv_file': csv_file,
        'user_decisions': user_decisions,
        'training_stats': training_stats
    }
    
    reporter = TrainingReporter()
    return reporter.generate_comprehensive_training_report(training_data, output_file)

def create_mapping_summary_table(user_decisions: Dict, standard_fields: List[str]) -> str:
    """
    Función utilitaria para crear solo la tabla de mapeo
    
    Returns:
        String con tabla formateada
    """
    lines = []
    lines.append(f"{'Standard Field':<25} | {'Mapped Column':<30} | {'Confidence':<10}")
    lines.append(f"{'-'*25} | {'-'*30} | {'-'*10}")
    
    for standard_field in standard_fields:
        mapped_column = "No mapeado"
        confidence = "0.000"
        
        for column_name, decision in user_decisions.items():
            if decision['field_type'] == standard_field:
                mapped_column = column_name
                confidence = f"{decision['confidence']:.3f}"
                break
        
        lines.append(f"{standard_field:<25} | {mapped_column:<30} | {confidence:<10}")
    
    return "\n".join(lines)