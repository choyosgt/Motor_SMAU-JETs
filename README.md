# Sistema de Mapeo de Campos Contables

Un sistema inteligente para la detección y mapeo automático de campos en archivos CSV contables, con soporte para múltiples ERPs y entrenamiento adaptativo.

## 🚀 Características Principales

- **Detección Automática de ERP**: Identifica automáticamente el sistema ERP de origen (SAP, ContaPlus, etc.)
- **Mapeo Inteligente de Campos**: Mapea columnas CSV a campos contables estándar
- **Entrenamiento Adaptativo**: Aprende de las correcciones del usuario para mejorar futuras detecciones
- **Validación de Balances**: Verifica automáticamente que los asientos contables estén balanceados
- **Múltiples Trainers**: Diferentes modos de entrenamiento según necesidades
- **Configuración YAML Dinámica**: Sistema de configuración flexible y extensible

## 📋 Requisitos del Sistema

### Dependencias Python
```bash
pip install pandas>=1.3.0
pip install pyyaml>=6.0
pip install openpyxl>=3.0.9  # Para archivos Excel
pip install xlrd>=2.0.1      # Para Excel legacy
```

### Versión de Python
- Python 3.7 o superior
- Funciona perfectamente con Spyder IDE

## 🛠️ Instalación

### Opción 1: Instalación Rápida para Spyder
```bash
# Clonar el repositorio
git clone [URL_DEL_REPOSITORIO]
cd sistema-mapeo-campos

# Ejecutar configuración automática
python spyder_setup.py
```

### Opción 2: Instalación Manual
```bash
# 1. Crear estructura de directorios
mkdir -p {config,data,logs,reports,backups,temp}

# 2. Instalar dependencias
pip install pandas pyyaml openpyxl xlrd

# 3. Verificar instalación
python -c "from core.field_detector import FieldDetector; print('✅ Instalación exitosa')"
```

## 📁 Estructura del Proyecto

```
sistema-mapeo-campos/
├── core/                           # Módulos principales del sistema
│   ├── __init__.py
│   ├── field_detector.py           # Detector de campos principal
│   ├── field_mapper.py             # Mapeador de campos
│   ├── dynamic_field_loader.py     # Cargador de campos dinámicos
│   └── dynamic_field_definition.py # Definiciones de campos
├── config/                         # Archivos de configuración
│   ├── system_config.yaml          # Configuración general del sistema
│   ├── dynamic_fields_config.yaml  # Campos dinámicos y sinónimos
│   └── pattern_learning_config.yaml # Patrones aprendidos
├── data/                          # Datos de ejemplo y pruebas
│   ├── ejemplo_sap_02.csv
│   ├── ejemplo_contaplus.csv
│   └── ejemplo_generic.csv
├── trainers/                      # Scripts de entrenamiento
│   ├── manual_confirmation_trainer.py
│   ├── automatic_confirmation_trainer.py
│   └── complete_enhanced_trainer.py
├── reports/                       # Reportes generados
├── logs/                         # Archivos de log
└── temp/                         # Archivos temporales
```

## 🎯 Uso de los Trainers

### 1. Manual Confirmation Trainer
**Entrenamiento con confirmación manual obligatoria para todas las detecciones**

```bash
# Uso interactivo
python manual_confirmation_trainer.py data/ejemplo_sap_02.csv SAP

# Análisis batch (sin interacción)
python manual_confirmation_trainer.py --batch data/ejemplo_sap_02.csv SAP
```

**Características:**
- ✅ **Confirmación manual obligatoria** para todas las detecciones
- ✅ Muestra alternativas inteligentes para decisiones de baja confianza
- ✅ Aprendizaje automático de sinónimos
- ✅ Generación de patrones regex precisos
- ✅ 17 campos estándar soportados

### 2. Automatic Confirmation Trainer
**Entrenamiento automático sin confirmación manual**

```bash
# Entrenamiento automático
python automatic_confirmation_trainer.py data/ejemplo_contaplus.csv ContaPlus
```

**Características:**
- 🤖 **Decisiones automáticas** basadas en confianza
- 🔄 **Módulos reutilizables** para procesamiento
- ⚖️ **Validación de balances** integrada
- 📊 **Reportes detallados** automáticos

### 3. Complete Enhanced Trainer
**Trainer completo con funcionalidades avanzadas**

```bash
# Entrenamiento completo
python complete_enhanced_trainer.py data/ejemplo_sap_02.csv SAP

# Con umbral de confianza personalizado
python complete_enhanced_trainer.py data/ejemplo_sap_02.csv SAP --confidence 0.8
```

## 📊 Campos Contables Soportados

El sistema reconoce 17 campos contables estándar:

1. `journal_entry_id` - ID del asiento contable
2. `line_number` - Número de línea
3. `description` - Descripción general
4. `line_description` - Descripción de línea
5. `posting_date` - Fecha de contabilización
6. `fiscal_year` - Año fiscal
7. `period_number` - Número de período
8. `gl_account_number` - Número de cuenta contable
9. `amount` - Importe general
10. `debit_amount` - Importe debe
11. `credit_amount` - Importe haber
12. `debit_credit_indicator` - Indicador debe/haber
13. `prepared_by` - Preparado por
14. `entry_date` - Fecha de entrada
15. `entry_time` - Hora de entrada
16. `gl_account_name` - Nombre de cuenta contable
17. `vendor_id` - ID de proveedor

## 🔧 Configuración

### Configuración del Sistema (`config/system_config.yaml`)
```yaml
system_configuration:
  min_confidence_threshold: 0.15
  exact_match_threshold: 0.95
  partial_match_threshold: 0.7
  auto_reload_enabled: true
  log_level: INFO
```

### Campos Dinámicos (`config/dynamic_fields_config.yaml`)
```yaml
field_definitions:
  dynamic_fields:
    cost_center:
      name: "Centro de Coste"
      description: "Centro de coste o departamento"
      data_type: "alphanumeric"
      active: true
      synonyms:
        SAP:
          - name: "KOSTL"
            confidence_boost: 0.9
        ContaPlus:
          - name: "CentroCosto"
            confidence_boost: 0.9
```

## 💡 Ejemplos de Uso

### Ejemplo 1: Análisis Básico
```python
from core.field_detector import FieldDetector
import pandas as pd

# Cargar datos
df = pd.read_csv('data/ejemplo_sap_02.csv')

# Crear detector
detector = FieldDetector()

# Detectar ERP automáticamente
erp = detector.auto_detect_erp(df)
print(f"ERP detectado: {erp}")

# Obtener resumen de detección
summary = detector.get_detection_summary(df)
print(f"Tasa de detección: {summary['detection_rate_percent']:.1f}%")
```

### Ejemplo 2: Mapeo Manual de Campos
```python
from core.field_mapper import FieldMapper

# Crear mapper
mapper = FieldMapper()

# Buscar mapeo para una columna
result = mapper.find_field_mapping("NumAsiento", "SAP")
if result:
    field_type, confidence = result
    print(f"Campo: {field_type}, Confianza: {confidence:.2f}")
```

### Ejemplo 3: Entrenamiento Rápido
```bash
# Para archivos SAP
python manual_confirmation_trainer.py data/ejemplo_sap_02.csv SAP

# Para archivos ContaPlus
python manual_confirmation_trainer.py data/ejemplo_contaplus.csv ContaPlus

# Para archivos genéricos (auto-detecta ERP)
python manual_confirmation_trainer.py data/ejemplo_generic.csv
```

## 📈 Interpretación de Resultados

### Métricas de Confianza
- **> 0.8**: Alta confianza - mapeo muy probable
- **0.5 - 0.8**: Confianza media - revisar sugerencias
- **< 0.5**: Baja confianza - requiere confirmación manual

### Tasa de Detección
- **> 80%**: Excelente compatibilidad con el ERP
- **60-80%**: Buena compatibilidad, algunas mejoras posibles
- **< 60%**: Requiere entrenamiento adicional o configuración personalizada

## 🔍 Troubleshooting

### Problemas Comunes

#### Error: "Core modules not found"
```bash
# Verificar instalación
python -c "import sys; print(sys.path)"
python spyder_setup.py  # Reconfigurar entorno
```

#### Error: "YAML configuration not found"
```bash
# Recrear configuraciones por defecto
python spyder_setup.py
```

#### Baja tasa de detección
1. Verificar formato del archivo CSV
2. Usar el trainer manual para entrenar el sistema
3. Revisar configuración de sinónimos en `dynamic_fields_config.yaml`

#### Problemas con encoding
```python
# Al cargar CSVs con caracteres especiales
df = pd.read_csv('archivo.csv', encoding='utf-8')
# o
df = pd.read_csv('archivo.csv', encoding='latin1')
```

## 📊 Validación de Balances

El sistema incluye validación automática de balances contables:

- ✅ **Balance total**: Suma de débitos = Suma de créditos
- ✅ **Balance por asiento**: Cada asiento está individualmente balanceado
- ✅ **Detección de anomalías**: Identifica asientos desbalanceados

## 🚀 Para Desarrolladores

### Ejecutar Tests
```bash
# Test completo del sistema
python test_notebook_spyder.py

# Test de módulos específicos
python quick_start_spyder.py
```

### Contribuir al Proyecto
1. Fork del repositorio
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -am 'Add nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

### Estructura de Commits
```
feat: nueva funcionalidad
fix: corrección de bug
docs: actualización documentación
refactor: refactorización de código
test: nuevos tests
```

## 📝 Changelog

### v2.1.2 (Actual)
- ✅ Soporte para 17 campos contables estándar
- ✅ Trainer de confirmación manual mejorado
- ✅ Validación de balances automática
- ✅ Persistencia de patrones en YAML
- ✅ Mejoras en detección de ERP

### v2.0.0
- 🔄 Refactorización completa del sistema
- 🆕 Sistema de configuración YAML
- 🆕 Múltiples trainers especializados
- 🆕 Módulos reutilizables

## 🆘 Soporte

### Contacto
- 📧 Email: [carolina.hoyos@es.gt.com]

## CREAR FEATURES 

```bash
python features.py
```

## ENTRENAR MODELO

```bash
python entrenamiento.py
```

## PREDICCIÓN DE LINEAS LIBRO DIARIO

```bash
python test_model.py --model-dir modelo --file data/raw/Ejemplo2.txt --out predicciones/predicciones_ej2.csv
python test_model.py --model-dir modelo --file data/raw/Ejemplo3.csv --out predicciones/predicciones_ej3.csv
python test_model.py --model-dir modelo --file data/raw/ejemplo4.1.txt --out predicciones/predicciones_ej4.1.csv
python test_model.py --model-dir modelo --file data/raw/ejemplo4.2.txt --out predicciones/predicciones_ej4.2.csv
python test_model.py --model-dir modelo --file data/raw/Ejemplo5.txt --out predicciones/predicciones_ej5.csv
python test_model.py --model-dir modelo --file data/raw/Ejemplo6.xlsx --out predicciones/predicciones_ej6.csv
python test_model.py --model-dir modelo --file data/raw/Ejemplo8.xlsx --out predicciones/predicciones_ej8.csv
python test_model.py --model-dir modelo --file data/raw/Ejemplo9.xlsx --out predicciones/predicciones_ej9.csv
python test_model.py --model-dir modelo --file data/raw/Ejemplo10.csv --out predicciones/predicciones_ej10.csv
```

## PROCESAMIENTO DE LINEAS LIBRO DIARIO Y ESTRUCTURA

```bash
python procesador_predicciones.py predicciones/predicciones_ej2.csv --salida resultados/resultado_ej2.csv
python procesador_predicciones.py predicciones/predicciones_ej3.csv --salida resultados/resultado_ej3.csv
python procesador_predicciones.py predicciones/predicciones_ej4.1.csv --salida resultados/resultado_ej4.1.csv
python procesador_predicciones.py predicciones/predicciones_ej4.2.csv --salida resultados/resultado_ej4.2.csv
python procesador_predicciones.py predicciones/predicciones_ej5.csv --salida resultados/resultado_ej5.csv
python procesador_predicciones.py predicciones/predicciones_ej6.csv --salida resultados/resultado_ej6.csv
python procesador_predicciones.py predicciones/predicciones_ej8.csv --salida resultados/resultado_ej8.csv
python procesador_predicciones.py predicciones/predicciones_ej9.csv --salida resultados/resultado_ej9.csv
python procesador_predicciones.py predicciones/predicciones_ej10.csv --salida resultados/resultado_ej10.csv
```


---

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo `LICENSE` para detalles.


---

**¡Importante!** Este sistema está en desarrollo activo. Para obtener la versión más reciente y actualizaciones, consulta regularmente el repositorio.
