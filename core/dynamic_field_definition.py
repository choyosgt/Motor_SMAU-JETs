# core/dynamic_field_definition.py
"""
Definición robusta de campos dinámicos
Optimizado para Spyder y uso interactivo
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict
import logging

# Configurar logging para Spyder
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Constantes del sistema (inmutables)
VALID_DATA_TYPES = frozenset({
    "text", "numeric", "date", "alphanumeric", "boolean",
    "email", "url", "phone", "currency", "percentage"
})

@dataclass
class SynonymData:
    """Datos estructurados de un sinónimo"""
    name: str
    confidence_boost: float = 0.0
    language: str = "es"
    description: str = ""
    deprecated: bool = False
    added_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def __post_init__(self):
        """Validación después de inicialización"""
        if not self.name or not self.name.strip():
            raise ValueError("Synonym name cannot be empty")
        if not (0 <= self.confidence_boost <= 1):
            raise ValueError("Confidence boost must be between 0 and 1")
        self.name = self.name.strip()
        self.language = self.language.lower()

@dataclass  
class ValidationRules:
    """Reglas de validación para campos"""
    pattern: Optional[str] = None
    required: bool = False
    custom_function: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    
    def __post_init__(self):
        """Validación de reglas"""
        if self.pattern:
            try:
                re.compile(self.pattern)
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {e}")
        
        if (self.min_length is not None and self.max_length is not None and 
            self.min_length > self.max_length):
            raise ValueError("min_length cannot be greater than max_length")
        
        if (self.min_value is not None and self.max_value is not None and 
            self.min_value > self.max_value):
            raise ValueError("min_value cannot be greater than max_value")

class DynamicFieldDefinition:
    """
    Definición completa de un campo dinámico
    Optimizada para uso en Spyder con validación robusta
    """
    
    def __init__(self, code: str, name: str, data_type: str, 
                 description: str = "", validation: ValidationRules = None, 
                 active: bool = True, priority: int = 0, default_value: Any = None,
                 synonyms_by_erp: Dict[str, List[SynonymData]] = None,
                 metadata: Dict = None):
        """
        Inicializa una definición de campo dinámico
        
        Args:
            code: Código único del campo
            name: Nombre descriptivo
            data_type: Tipo de datos
            description: Descripción opcional
            validation: Reglas de validación
            active: Si el campo está activo
            priority: Prioridad del campo
            default_value: Valor por defecto
            synonyms_by_erp: Sinónimos organizados por ERP
            metadata: Metadatos adicionales
        """
        
        # Validación inmediata de parámetros críticos
        self._validate_basic_inputs(code, name, data_type)
        
        # Asignación de propiedades
        self.code = code.strip().lower()
        self.name = name.strip()
        self.description = description.strip() if description else ""
        self.data_type = data_type.lower()
        self.validation = validation or ValidationRules()
        self.active = bool(active)
        self.priority = int(priority)
        self.default_value = default_value
        self.metadata = metadata or {}
        
        # Sinónimos organizados por ERP
        self.synonyms_by_erp = synonyms_by_erp or {}
        
        # Metadatos de control
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.version = 1
        
        # Cache interno para optimización
        self._cache = {}
        
        # Validación final
        self._validate_complete_definition()
    
    def _validate_basic_inputs(self, code: str, name: str, data_type: str):
        """Validación básica de inputs críticos"""
        if not code or not isinstance(code, str):
            raise ValueError("Code must be a non-empty string")
        
        if not re.match(r'^[a-z][a-z0-9_]*$', code.strip().lower()):
            raise ValueError("Code must start with letter and contain only lowercase letters, numbers, and underscores")
        
        if not name or not isinstance(name, str):
            raise ValueError("Name must be a non-empty string")
        
        if data_type not in VALID_DATA_TYPES:
            raise ValueError(f"Invalid data_type: {data_type}. Must be one of {VALID_DATA_TYPES}")
    
    def _validate_complete_definition(self):
        """Validación completa de la definición"""
        errors = []
        
        # Validar que hay al menos un sinónimo
        # total_synonyms = sum(len(synonyms) for synonyms in self.synonyms_by_erp.values())
        # if total_synonyms == 0:
        #    logger.warning(f"Field '{self.code}' has no synonyms defined")
        
        # Validar estructura de sinónimos
        for erp_system, synonyms in self.synonyms_by_erp.items():
            if not erp_system or not erp_system.strip():
                errors.append("ERP system name cannot be empty")
            
            for i, synonym in enumerate(synonyms):
                if not isinstance(synonym, SynonymData):
                    errors.append(f"Synonym {i} in ERP {erp_system} must be SynonymData instance")
        
        if errors:
            raise ValueError(f"Definition validation failed: {'; '.join(errors)}")
    
    def add_synonym(self, erp_system: str, synonym_name: str, 
                   confidence_boost: float = 0.0, language: str = "es",
                   description: str = None) -> bool:
        """
        Añade un sinónimo con validación completa
        Retorna True si se añadió exitosamente
        """
        try:
            if not erp_system or not synonym_name:
                raise ValueError("ERP system and synonym name are required")
            
            # Inicializar ERP si no existe
            if erp_system not in self.synonyms_by_erp:
                self.synonyms_by_erp[erp_system] = []
            
            # Verificar duplicados
            existing_names = [s.name for s in self.synonyms_by_erp[erp_system]]
            if synonym_name in existing_names:
                logger.warning(f"Synonym '{synonym_name}' already exists for {erp_system}")
                return False
            
            # Crear sinónimo
            synonym = SynonymData(
                name=synonym_name.strip(),
                confidence_boost=float(confidence_boost),
                language=language.lower(),
                description=description or f"Synonym for {self.name}"
            )
            
            self.synonyms_by_erp[erp_system].append(synonym)
            self._update_metadata()
            self._clear_cache()
            
            logger.debug(f"Added synonym '{synonym_name}' to field '{self.code}' for ERP '{erp_system}'")
            return True
            
        except Exception as e:
            logger.error(f"Error adding synonym: {e}")
            return False
    
    def remove_synonym(self, erp_system: str, synonym_name: str) -> bool:
        """Elimina un sinónimo específico"""
        try:
            if erp_system not in self.synonyms_by_erp:
                return False
            
            original_length = len(self.synonyms_by_erp[erp_system])
            self.synonyms_by_erp[erp_system] = [
                s for s in self.synonyms_by_erp[erp_system] 
                if s.name != synonym_name
            ]
            
            removed = len(self.synonyms_by_erp[erp_system]) < original_length
            
            if removed:
                self._update_metadata()
                self._clear_cache()
                logger.debug(f"Removed synonym '{synonym_name}' from field '{self.code}'")
            
            return removed
            
        except Exception as e:
            logger.error(f"Error removing synonym: {e}")
            return False
    
    def get_synonyms_for_erp(self, erp_system: str, include_deprecated: bool = False) -> List[str]:
        """Obtiene sinónimos para un ERP específico con cache"""
        cache_key = f"synonyms_{erp_system}_{include_deprecated}"
        
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        if erp_system not in self.synonyms_by_erp:
            result = []
        else:
            result = [
                s.name for s in self.synonyms_by_erp[erp_system]
                if include_deprecated or not s.deprecated
            ]
        
        self._cache[cache_key] = result
        return result
    
    def get_all_synonyms(self, include_deprecated: bool = False) -> List[str]:
        """Obtiene todos los sinónimos de todos los ERPs"""
        cache_key = f"all_synonyms_{include_deprecated}"
        
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        all_synonyms = set()
        for synonyms in self.synonyms_by_erp.values():
            for synonym in synonyms:
                if include_deprecated or not synonym.deprecated:
                    all_synonyms.add(synonym.name)
        
        result = list(all_synonyms)
        self._cache[cache_key] = result
        return result
    
    def get_confidence_for_erp(self, erp_system: str) -> float:
        """Calcula confianza promedio para un ERP específico"""
        if erp_system not in self.synonyms_by_erp:
            return 0.0
        
        synonyms = self.synonyms_by_erp[erp_system]
        if not synonyms:
            return 0.0
        
        total_confidence = sum(s.confidence_boost for s in synonyms)
        return total_confidence / len(synonyms)
    
    def to_dict(self) -> Dict:
        """Convierte la definición a diccionario para serialización"""
        # Convertir sinónimos a formato serializable
        synonyms_dict = {}
        for erp_system, synonyms in self.synonyms_by_erp.items():
            synonyms_dict[erp_system] = [asdict(s) for s in synonyms]
        
        return {
            "code": self.code,
            "name": self.name,
            "description": self.description,
            "data_type": self.data_type,
            "validation": asdict(self.validation),
            "active": self.active,
            "priority": self.priority,
            "default_value": self.default_value,
            "synonyms": synonyms_dict,
            "metadata": self.metadata,
            "timestamps": {
                "created_at": self.created_at.isoformat(),
                "updated_at": self.updated_at.isoformat(),
                "version": self.version
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DynamicFieldDefinition':
        """Crea instancia desde diccionario con manejo robusto de errores"""
        try:
            # Procesar validación
            validation_data = data.get("validation", {})
            validation = ValidationRules(**{k: v for k, v in validation_data.items() if v is not None})
            
            # Procesar sinónimos
            synonyms_by_erp = {}
            synonyms_data = data.get("synonyms", {})
            
            for erp_system, synonyms_list in synonyms_data.items():
                synonyms_by_erp[erp_system] = []
                for syn_data in synonyms_list:
                    if isinstance(syn_data, dict):
                        synonyms_by_erp[erp_system].append(SynonymData(**syn_data))
                    else:
                        # Compatibilidad con formato simple
                        synonyms_by_erp[erp_system].append(SynonymData(name=str(syn_data)))
            
            # Crear instancia con orden correcto de parámetros
            instance = cls(
                code=data["code"],
                name=data["name"],
                data_type=data["data_type"],
                description=data.get("description", ""),
                validation=validation,
                active=data.get("active", True),
                priority=data.get("priority", 0),
                default_value=data.get("default_value"),
                synonyms_by_erp=synonyms_by_erp,
                metadata=data.get("metadata", {})
            )
            
            # Restaurar timestamps si existen
            timestamps = data.get("timestamps", {})
            if "created_at" in timestamps:
                instance.created_at = datetime.fromisoformat(timestamps["created_at"])
            if "updated_at" in timestamps:
                instance.updated_at = datetime.fromisoformat(timestamps["updated_at"])
            if "version" in timestamps:
                instance.version = timestamps["version"]
            
            return instance
            
        except Exception as e:
            raise ValueError(f"Error creating DynamicFieldDefinition from dict: {e}")
    
    def clone(self, new_code: str = None) -> 'DynamicFieldDefinition':
        """Clona la definición opcionalmente con nuevo código"""
        data = self.to_dict()
        if new_code:
            data["code"] = new_code
        
        cloned = self.from_dict(data)
        cloned.created_at = datetime.now()
        cloned.updated_at = datetime.now()
        cloned.version = 1
        
        return cloned
    
    def is_valid(self) -> bool:
        """Verifica si la definición es válida"""
        try:
            self._validate_complete_definition()
            return True
        except ValueError:
            return False
    
    def get_statistics(self) -> Dict:
        """Obtiene estadísticas de la definición"""
        total_synonyms = sum(len(synonyms) for synonyms in self.synonyms_by_erp.values())
        avg_confidence = 0.0
        
        if total_synonyms > 0:
            total_confidence = sum(
                sum(s.confidence_boost for s in synonyms)
                for synonyms in self.synonyms_by_erp.values()
            )
            avg_confidence = total_confidence / total_synonyms
        
        return {
            "code": self.code,
            "name": self.name,
            "data_type": self.data_type,
            "is_valid": self.is_valid(),
            "is_active": self.active,
            "total_synonyms": total_synonyms,
            "erp_systems": len(self.synonyms_by_erp),
            "average_confidence": round(avg_confidence, 3),
            "version": self.version,
            "last_updated": self.updated_at.isoformat()
        }
    
    def _update_metadata(self):
        """Actualiza metadatos de control"""
        self.updated_at = datetime.now()
        self.version += 1
    
    def _clear_cache(self):
        """Limpia el cache interno"""
        self._cache.clear()
    
    def __str__(self) -> str:
        return f"DynamicFieldDefinition(code='{self.code}', name='{self.name}', type='{self.data_type}')"
    
    def __repr__(self) -> str:
        return self.__str__()
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, DynamicFieldDefinition):
            return False
        return self.code == other.code and self.version == other.version
    
    def __hash__(self) -> int:
        return hash((self.code, self.version))

# Funciones de utilidad para Spyder
def create_field_definition(code: str, name: str, data_type: str = "text", 
                          description: str = "", **kwargs) -> DynamicFieldDefinition:
    """
    Función de conveniencia para crear definiciones rápidamente en Spyder
    """
    return DynamicFieldDefinition(
        code=code,
        name=name,
        data_type=data_type,
        description=description or f'Field: {name}',
        **kwargs
    )

def create_sample_field_definitions() -> Dict[str, DynamicFieldDefinition]:
    """
    Crea definiciones de campos de ejemplo ACTUALIZADAS
    Incluye nuevos campos gl_account_name y vendor_id, nombres actualizados
    """
    fields = {}
    
    # CAMPO 1: Journal Entry ID (actualizado de journal_id)
    journal_entry_id = create_field_definition(
        code="journal_entry_id",
        name="ID del Asiento Contable",
        data_type="numeric",
        description="Identificador único del asiento contable"
    )
    journal_entry_id.add_synonym("Generic_ES", "Asiento", 0.9)
    journal_entry_id.add_synonym("Generic_ES", "NumAsiento", 0.95)
    journal_entry_id.add_synonym("Generic_ES", "ID_Asiento", 0.9)
    journal_entry_id.add_synonym("SAP", "BELNR", 0.95)
    journal_entry_id.add_synonym("Oracle", "je_header_id", 0.9)
    journal_entry_id.add_synonym("Navision", "document_no", 0.8)
    fields["journal_entry_id"] = journal_entry_id
    
    # CAMPO 2: Line Number (actualizado de journal_id_line_number)
    line_number = create_field_definition(
        code="line_number",
        name="Número de Línea del Asiento",
        data_type="numeric",
        description="Número secuencial de línea dentro del asiento"
    )
    line_number.add_synonym("Generic_ES", "Linea", 0.9)
    line_number.add_synonym("Generic_ES", "NumLinea", 0.95)
    line_number.add_synonym("Generic_ES", "LineaAsiento", 0.9)
    line_number.add_synonym("SAP", "BUZEI", 0.95)
    line_number.add_synonym("Oracle", "je_line_num", 0.9)
    fields["line_number"] = line_number
    
    # CAMPO 3: Description (actualizado de je_header_description)
    description = create_field_definition(
        code="description",
        name="Descripción del Encabezado",
        data_type="text",
        description="Descripción general del asiento contable"
    )
    description.add_synonym("Generic_ES", "Concepto", 0.9)
    description.add_synonym("Generic_ES", "ConceptoAsiento", 0.95)
    description.add_synonym("Generic_ES", "DescripcionCabecera", 0.9)
    description.add_synonym("SAP", "BKTXT", 0.9)
    description.add_synonym("Oracle", "description", 0.8)
    fields["description"] = description
    
    # CAMPO 4: Line Description (actualizado de je_line_description)
    line_description = create_field_definition(
        code="line_description",
        name="Descripción de la Línea",
        data_type="text",
        description="Descripción específica de la línea del asiento"
    )
    line_description.add_synonym("Generic_ES", "DescripcionLinea", 0.9)
    line_description.add_synonym("Generic_ES", "DetalleLinea", 0.8)
    line_description.add_synonym("SAP", "SGTXT", 0.9)
    line_description.add_synonym("Oracle", "line_description", 0.8)
    fields["line_description"] = line_description
    
    # CAMPO 5: Posting Date (actualizado de effective_date)
    posting_date = create_field_definition(
        code="posting_date",
        name="Fecha de Contabilización",
        data_type="date",
        description="Fecha efectiva de contabilización del asiento"
    )
    posting_date.add_synonym("Generic_ES", "Fecha", 0.9)
    posting_date.add_synonym("Generic_ES", "FechaAsiento", 0.95)
    posting_date.add_synonym("Generic_ES", "FechaContabilizacion", 0.9)
    posting_date.add_synonym("SAP", "BUDAT", 0.95)
    posting_date.add_synonym("Oracle", "effective_date", 0.9)
    posting_date.add_synonym("Navision", "posting_date", 0.9)
    fields["posting_date"] = posting_date
    
    # CAMPO 6: Fiscal Year
    fiscal_year = create_field_definition(
        code="fiscal_year",
        name="Año Fiscal",
        data_type="numeric",
        description="Año fiscal del ejercicio contable"
    )
    fiscal_year.add_synonym("Generic_ES", "Año", 0.8)
    fiscal_year.add_synonym("Generic_ES", "AñoFiscal", 0.95)
    fiscal_year.add_synonym("Generic_ES", "Ejercicio", 0.9)
    fiscal_year.add_synonym("SAP", "GJAHR", 0.95)
    fiscal_year.add_synonym("Oracle", "period_year", 0.8)
    fields["fiscal_year"] = fiscal_year
    
    # CAMPO 7: Period Number (actualizado de period)
    period_number = create_field_definition(
        code="period_number",
        name="Número de Período",
        data_type="numeric",
        description="Número del período contable (mes)"
    )
    period_number.add_synonym("Generic_ES", "Periodo", 0.9)
    period_number.add_synonym("Generic_ES", "Mes", 0.8)
    period_number.add_synonym("Generic_ES", "PeriodoContable", 0.95)
    period_number.add_synonym("SAP", "MONAT", 0.95)
    period_number.add_synonym("Oracle", "period_num", 0.9)
    fields["period_number"] = period_number
    
    # CAMPO 8: GL Account Number
    gl_account_number = create_field_definition(
        code="gl_account_number",
        name="Número de Cuenta Contable",
        data_type="alphanumeric",
        description="Código de la cuenta del plan contable"
    )
    gl_account_number.add_synonym("Generic_ES", "Cuenta", 0.9)
    gl_account_number.add_synonym("Generic_ES", "CuentaContable", 0.95)
    gl_account_number.add_synonym("Generic_ES", "CodigoCuenta", 0.9)
    gl_account_number.add_synonym("SAP", "HKONT", 0.95)
    gl_account_number.add_synonym("Oracle", "account", 0.8)
    gl_account_number.add_synonym("Navision", "g_l_account_no", 0.9)
    fields["gl_account_number"] = gl_account_number
    
    # CAMPO 9: GL Account Name (NUEVO)
    gl_account_name = create_field_definition(
        code="gl_account_name",
        name="Nombre de la Cuenta Contable",
        data_type="text",
        description="Descripción o nombre de la cuenta contable"
    )
    gl_account_name.add_synonym("Generic_ES", "NombreCuenta", 0.95)
    gl_account_name.add_synonym("Generic_ES", "DescripcionCuenta", 0.9)
    gl_account_name.add_synonym("Generic_ES", "DenominacionCuenta", 0.8)
    gl_account_name.add_synonym("SAP", "TXT50", 0.9)
    gl_account_name.add_synonym("Oracle", "account_description", 0.8)
    gl_account_name.add_synonym("Navision", "account_name", 0.8)
    fields["gl_account_name"] = gl_account_name
    
    # CAMPO 10: Amount
    amount = create_field_definition(
        code="amount",
        name="Importe",
        data_type="currency",
        description="Importe monetario del movimiento"
    )
    amount.add_synonym("Generic_ES", "Importe", 0.95)
    amount.add_synonym("Generic_ES", "Saldo", 0.9)
    amount.add_synonym("Generic_ES", "Total", 0.8)
    amount.add_synonym("SAP", "DMBTR", 0.95)
    amount.add_synonym("Oracle", "entered_amount", 0.8)
    fields["amount"] = amount
    
    # CAMPO 11: Debit Amount (actualizado de amount_debit)
    debit_amount = create_field_definition(
        code="debit_amount",
        name="Importe Debe",
        data_type="currency",
        description="Importe en el debe del asiento"
    )
    debit_amount.add_synonym("Generic_ES", "Debe", 0.95)
    debit_amount.add_synonym("Generic_ES", "ImporteDebe", 0.9)
    debit_amount.add_synonym("Generic_ES", "Debito", 0.8)
    debit_amount.add_synonym("SAP", "SOLLBETRAG", 0.9)
    debit_amount.add_synonym("Oracle", "entered_dr", 0.9)
    debit_amount.add_synonym("Navision", "debit_amount", 0.9)
    fields["debit_amount"] = debit_amount
    
    # CAMPO 12: Credit Amount (actualizado de amount_credit)
    credit_amount = create_field_definition(
        code="credit_amount",
        name="Importe Haber",
        data_type="currency",
        description="Importe en el haber del asiento"
    )
    credit_amount.add_synonym("Generic_ES", "Haber", 0.95)
    credit_amount.add_synonym("Generic_ES", "ImporteHaber", 0.9)
    credit_amount.add_synonym("Generic_ES", "Credito", 0.8)
    credit_amount.add_synonym("SAP", "HABENBETRAG", 0.9)
    credit_amount.add_synonym("Oracle", "entered_cr", 0.9)
    credit_amount.add_synonym("Navision", "credit_amount", 0.9)
    fields["credit_amount"] = credit_amount
    
    # CAMPO 13: Debit Credit Indicator (actualizado de amount_credit_debit_indicator)
    debit_credit_indicator = create_field_definition(
        code="debit_credit_indicator",
        name="Indicador Debe/Haber",
        data_type="text",
        description="Indicador de si es debe (D) o haber (H)"
    )
    debit_credit_indicator.add_synonym("Generic_ES", "IndicadorDH", 0.9)
    debit_credit_indicator.add_synonym("Generic_ES", "DebeHaber", 0.8)
    debit_credit_indicator.add_synonym("SAP", "SHKZG", 0.95)
    debit_credit_indicator.add_synonym("Oracle", "dc_indicator", 0.8)
    fields["debit_credit_indicator"] = debit_credit_indicator
    
    # CAMPO 14: Vendor ID 
    vendor_id = create_field_definition(
        code="vendor_id",
        name="ID del Proveedor/Tercero",
        data_type="alphanumeric",
        description="Identificador del proveedor o tercero"
    )
    vendor_id.add_synonym("Generic_ES", "Proveedor", 0.9)
    vendor_id.add_synonym("Generic_ES", "IDProveedor", 0.95)
    vendor_id.add_synonym("Generic_ES", "CodigoProveedor", 0.9)
    vendor_id.add_synonym("Generic_ES", "Tercero", 0.8)
    vendor_id.add_synonym("SAP", "LIFNR", 0.95)
    vendor_id.add_synonym("Oracle", "vendor_id", 0.9)
    vendor_id.add_synonym("Navision", "vendor_no", 0.8)
    fields["vendor_id"] = vendor_id
    
    # CAMPO 15: Prepared By 
    prepared_by = create_field_definition(
        code="prepared_by",
        name="Preparado Por",
        data_type="text",
        description="Usuario que preparó el asiento"
    )
    prepared_by.add_synonym("Generic_ES", "Usuario", 0.8)
    prepared_by.add_synonym("Generic_ES", "PreparadoPor", 0.95)
    prepared_by.add_synonym("Generic_ES", "CreadoPor", 0.9)
    prepared_by.add_synonym("SAP", "USNAM", 0.9)
    prepared_by.add_synonym("Oracle", "created_by", 0.8)
    fields["prepared_by"] = prepared_by
    
    # CAMPO 16: Entry Date 
    entry_date = create_field_definition(
        code="entry_date",
        name="Fecha de Entrada",
        data_type="date",
        description="Fecha de introducción del asiento en el sistema"
    )
    entry_date.add_synonym("Generic_ES", "FechaEntrada", 0.95)
    entry_date.add_synonym("Generic_ES", "FechaCreacion", 0.9)
    entry_date.add_synonym("Generic_ES", "FechaCaptura", 0.8)
    entry_date.add_synonym("SAP", "CPUDT", 0.9)
    entry_date.add_synonym("Oracle", "creation_date", 0.8)
    fields["entry_date"] = entry_date
    
    # CAMPO 17: Entry Time 
    entry_time = create_field_definition(
        code="entry_time",
        name="Hora de Entrada",
        data_type="text",
        description="Hora de introducción del asiento en el sistema"
    )
    entry_time.add_synonym("Generic_ES", "HoraEntrada", 0.95)
    entry_time.add_synonym("Generic_ES", "HoraCreacion", 0.9)
    entry_time.add_synonym("SAP", "CPUTM", 0.9)
    entry_time.add_synonym("Oracle", "creation_time", 0.8)
    fields["entry_time"] = entry_time
    
    # CAMPO 18: Document Number
    document_number = create_field_definition(
        code="document_number",
        name="Número de Documento",
        data_type="alphanumeric",
        description="Número de documento de referencia"
    )
    document_number.add_synonym("Generic_ES", "NumDoc", 0.9)
    document_number.add_synonym("Generic_ES", "NumDocumento", 0.95)
    document_number.add_synonym("Generic_ES", "Documento", 0.8)
    document_number.add_synonym("SAP", "XBLNR", 0.95)
    document_number.add_synonym("Oracle", "reference", 0.8)
    fields["document_number"] = document_number
    
    print(f"✓ Created {len(fields)} updated field definitions with new fields")
    return fields

# Test rápido para Spyder
def test_field_definitions():
    """Test rápido para verificar funcionamiento en Spyder"""
    print("🧪 Testing UPDATED Field Definitions...")
    
    try:
        # Crear definiciones de ejemplo
        fields = create_sample_field_definitions()
        
        print(f"✓ Created {len(fields)} field definitions")
        
        # Mostrar estadísticas
        for code, field_def in fields.items():
            stats = field_def.get_statistics()
            print(f"  - {stats['name']}: {stats['total_synonyms']} synonyms, {stats['erp_systems']} ERPs")
        
        # Test específico de nuevos campos
        if 'gl_account_name' in fields:
            print(f"✓ New field 'gl_account_name' created successfully")
        if 'vendor_id' in fields:
            print(f"✓ New field 'vendor_id' created successfully")
        
        # Test de serialización
        test_field = fields["journal_entry_id"]
        dict_data = test_field.to_dict()
        restored_field = DynamicFieldDefinition.from_dict(dict_data)
        
        print(f"✓ Serialization test passed")
        print(f"✓ All tests completed successfully")
        
        return fields
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Ejecutar test si se ejecuta directamente
    test_field_definitions()