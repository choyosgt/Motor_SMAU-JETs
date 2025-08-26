# spyder_setup.py
"""
Configuración específica para Spyder IDE
Ejecutar este archivo para configurar el entorno de desarrollo
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime

def setup_spyder_environment():
    """Configura el entorno específicamente para Spyder"""
    print("🕷️ CONFIGURANDO ENTORNO PARA SPYDER")
    print("=" * 50)
    
    # 1. Configurar directorio de trabajo
    project_root = Path(__file__).parent.absolute()
    os.chdir(str(project_root))
    print(f"✓ Directorio de trabajo: {project_root}")
    
    # 2. Configurar Python path
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    print(f"✓ Python path configurado")
    
    # 3. Crear estructura de directorios
    directories = [
        'config',
        'data', 
        'logs',
        'backups',
        'reports',
        'temp'
    ]
    
    for directory in directories:
        dir_path = project_root / directory
        dir_path.mkdir(exist_ok=True)
        
        # Crear __init__.py para directorios de Python
        if directory in ['config']:
            init_file = dir_path / '__init__.py'
            if not init_file.exists():
                init_file.write_text("# Auto-generated __init__.py\n")
    
    print(f"✓ Estructura de directorios creada")
    
    # 4. Verificar dependencias
    print(f"\n📦 VERIFICANDO DEPENDENCIAS")
    print("-" * 30)
    
    dependencies = {
        'pandas': 'pip install pandas',
        'pyyaml': 'pip install pyyaml',
        'openpyxl': 'pip install openpyxl',  # Para Excel
        'xlrd': 'pip install xlrd',  # Para Excel legacy
    }
    
    missing_deps = []
    
    for dep, install_cmd in dependencies.items():
        try:
            __import__(dep)
            print(f"✓ {dep}")
        except ImportError:
            print(f"❌ {dep} - {install_cmd}")
            missing_deps.append((dep, install_cmd))
    
    if missing_deps:
        print(f"\n⚠️ Para instalar dependencias faltantes:")
        for dep, cmd in missing_deps:
            print(f"  {cmd}")
    
    # 5. Crear archivos de configuración por defecto
    create_default_configs(project_root)
    
    # 6. Crear datos de ejemplo
    create_sample_data_for_spyder(project_root)
    
    print(f"\n✅ CONFIGURACIÓN COMPLETADA")
    print(f"📍 Directorio del proyecto: {project_root}")
    print(f"📅 Configurado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return project_root

def create_default_configs(project_root: Path):
    """Crea archivos de configuración por defecto"""
    print(f"\n⚙️ CREANDO CONFIGURACIONES POR DEFECTO")
    print("-" * 40)
    
    config_dir = project_root / 'config'
    
    # 1. Configuración del sistema
    system_config = config_dir / 'system_config.yaml'
    if not system_config.exists():
        system_config_content = """# Configuración del sistema para Spyder
system_configuration:
  # Umbrales de confianza
  min_confidence_threshold: 0.15
  exact_match_threshold: 0.95
  partial_match_threshold: 0.7
  
  # Configuración de desarrollo
  auto_reload_enabled: true
  default_reload_interval: 30
  log_level: INFO
  
  # Cache optimizado para desarrollo
  max_cache_size: 1000
  cache_ttl_seconds: 3600
  
  # Campos core
  core_fields:
    journal_entry_number: "Número de Asiento"
    posting_date: "Fecha Contable"
    account_code: "Cuenta Contable"
    amount_debit: "Importe Debe"
    amount_credit: "Importe Haber"
    description: "Descripción"
    cost_center: "Centro de Coste"
    project_code: "Código de Proyecto"
"""
        system_config.write_text(system_config_content, encoding='utf-8')
        print(f"✓ {system_config.name}")
    
    # 2. Configuración de campos dinámicos mínima
    fields_config = config_dir / 'dynamic_fields_config.yaml'
    if not fields_config.exists():
        fields_config_content = """# Configuración mínima de campos dinámicos
system:
  version: "2.0.0"
  auto_reload: true

field_definitions:
  dynamic_fields:
    cost_center:
      name: "Centro de Coste"
      description: "Centro de coste o departamento"
      data_type: "alphanumeric"
      active: true
      synonyms:
        Generic_ES:
          - name: "CentroCosto"
            confidence_boost: 0.9
          - name: "CC"
            confidence_boost: 0.7
        ContaPlus:
          - name: "CentroCosto"
            confidence_boost: 0.9
        SAP:
          - name: "KOSTL"
            confidence_boost: 0.9
"""
        fields_config.write_text(fields_config_content, encoding='utf-8')
        print(f"✓ {fields_config.name}")
    
    # 3. Validadores básicos
    validators_file = config_dir / 'custom_field_validators.py'
    if not validators_file.exists():
        validators_content = '''# Validadores personalizados básicos
import pandas as pd
import re

def validate_cost_center(series: pd.Series) -> float:
    """Valida códigos de centro de coste"""
    if series.empty:
        return 0.0
    
    clean_series = series.dropna().astype(str)
    if len(clean_series) == 0:
        return 0.0
    
    valid_count = 0
    for value in clean_series:
        if re.match(r'^[A-Z0-9]{2,10}, value.upper()):
            valid_count += 1
        elif len(value) >= 2:
            valid_count += 0.5
    
    return min(valid_count / len(clean_series), 1.0)

# Registro de validadores
AVAILABLE_VALIDATORS = {
    'validate_cost_center': validate_cost_center
}
'''
        validators_file.write_text(validators_content, encoding='utf-8')
        print(f"✓ {validators_file.name}")

def create_sample_data_for_spyder(project_root: Path):
    """Crea datos de ejemplo optimizados para Spyder"""
    print(f"\n📊 CREANDO DATOS DE EJEMPLO")
    print("-" * 30)
    
    data_dir = project_root / 'data'
    
    # Datos de ejemplo ContaPlus
    contaplus_data = pd.DataFrame({
        'NumAsiento': [1, 2, 3, 4, 5],
        'FechaAsiento': ['01/01/2024', '02/01/2024', '03/01/2024', '04/01/2024', '05/01/2024'],
        'CuentaContable': ['4300001', '7000001', '5720001', '4300002', '6230001'],
        'CentroCosto': ['CC001', 'CC002', 'CC001', 'CC003', 'CC002'],
        'Debe': [100.50, 0.0, 25.75, 150.25, 0.0],
        'Haber': [0.0, 100.50, 25.75, 0.0, 150.25],
        'ConceptoAsiento': ['Compra material', 'Venta producto', 'Pago factura', 'Compra servicios', 'Cobro cliente']
    })
    
    contaplus_file = data_dir / 'ejemplo_contaplus.csv'
    contaplus_data.to_csv(contaplus_file, index=False)
    print(f"✓ {contaplus_file.name}")
    
    # Datos de ejemplo SAP
    sap_data = pd.DataFrame({
        'BELNR': [1000001, 1000002, 1000003],
        'BUDAT': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'HKONT': ['0004300001', '0007000001', '0005720001'],
        'KOSTL': ['CC001', 'CC002', 'CC001'],
        'SOLLBETRAG': [100.50, 0.0, 25.75],
        'HABENBETRAG': [0.0, 100.50, 25.75],
        'SGTXT': ['Purchase material', 'Sales product', 'Invoice payment']
    })
    
    sap_file = data_dir / 'ejemplo_sap.csv'
    sap_data.to_csv(sap_file, index=False)
    print(f"✓ {sap_file.name}")

def create_spyder_quick_start():
    """Crea script de inicio rápido para Spyder"""
    quick_start_content = '''# quick_start_spyder.py
"""
Script de inicio rápido para Spyder
Ejecutar este archivo para probar el sistema
"""

import sys
import os
from pathlib import Path
import pandas as pd

# Configurar entorno
project_root = Path(__file__).parent
os.chdir(str(project_root))
sys.path.insert(0, str(project_root))

print("🚀 INICIO RÁPIDO - SISTEMA DINÁMICO")
print("=" * 40)

# Importar sistema
try:
    from core.field_detector import create_detector, test_field_detector
    from core.field_mapper import create_field_mapper, test_field_mapper
    from core.dynamic_field_loader import create_field_loader, test_field_loader
    
    print("✓ Módulos importados correctamente")
    
    # Crear detector
    detector = create_detector()
    print("✓ Detector creado")
    
    # Cargar datos de ejemplo
    df = pd.read_csv('data/ejemplo_contaplus.csv')
    print(f"✓ Datos cargados: {df.shape}")
    
    # Realizar detección
    summary = detector.get_detection_summary(df)
    print(f"✓ Detección completada: {summary['detection_rate_percent']:.1f}%")
    
    # Mostrar resultados
    print("\\n📊 RESULTADOS:")
    for field_type, column_name in summary['detected_fields'].items():
        print(f"  ✓ {field_type}: {column_name}")
    
    print("\\n💡 VARIABLES DISPONIBLES:")
    print("  - detector: Detector principal")
    print("  - df: DataFrame de ejemplo")
    print("  - summary: Resumen de detección")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\\n🔧 CONFIGURACIÓN BÁSICA:")
    print("  1. Ejecutar: python spyder_setup.py")
    print("  2. Verificar que todos los archivos están en su lugar")
    print("  3. Instalar dependencias faltantes")
'''
    
    quick_start_file = Path('quick_start_spyder.py')
    quick_start_file.write_text(quick_start_content, encoding='utf-8')
    print(f"✓ {quick_start_file.name}")

def create_spyder_test_notebook():
    """Crea notebook de pruebas para Spyder"""
    test_notebook_content = '''# test_notebook_spyder.py
"""
Notebook de pruebas para Spyder
Ejecutar sección por sección para probar diferentes componentes
"""

# %% Configuración inicial
import sys
import os
from pathlib import Path
import pandas as pd

# Configurar entorno
project_root = Path.cwd()
sys.path.insert(0, str(project_root))

print("🧪 NOTEBOOK DE PRUEBAS")
print("=" * 30)

# %% Test 1: Importación de módulos
print("\\n1️⃣ IMPORTACIÓN DE MÓDULOS")
print("-" * 25)

try:
    from core.dynamic_field_definition import create_sample_field_definitions
    from core.dynamic_field_loader import create_field_loader
    from core.field_mapper import create_field_mapper
    from core.field_detector import create_detector
    
    print("✓ Todos los módulos importados correctamente")
    modules_ok = True
except Exception as e:
    print(f"❌ Error importando módulos: {e}")
    modules_ok = False

# %% Test 2: Creación de componentes
if modules_ok:
    print("\\n2️⃣ CREACIÓN DE COMPONENTES")
    print("-" * 27)
    
    # Crear loader
    loader = create_field_loader()
    print("✓ Field Loader creado")
    
    # Crear mapper
    mapper = create_field_mapper()
    print("✓ Field Mapper creado")
    
    # Crear detector
    detector = create_detector()
    print("✓ Field Detector creado")

# %% Test 3: Análisis de datos
if modules_ok:
    print("\\n3️⃣ ANÁLISIS DE DATOS")
    print("-" * 20)
    
    # Cargar datos
    try:
        df = pd.read_csv('data/ejemplo_contaplus.csv')
        print(f"✓ Datos cargados: {df.shape}")
        print(f"  Columnas: {list(df.columns)}")
        
        # Detectar ERP
        erp = detector.auto_detect_erp(df)
        print(f"✓ ERP detectado: {erp}")
        
        # Realizar detección
        summary = detector.get_detection_summary(df)
        print(f"✓ Detección: {summary['detection_rate_percent']:.1f}%")
        
        # Mostrar resultados
        print("\\n📊 Campos detectados:")
        for field_type, column_name in summary['detected_fields'].items():
            print(f"  ✓ {field_type}: {column_name}")
        
    except Exception as e:
        print(f"❌ Error en análisis: {e}")

# %% Test 4: Validadores personalizados
print("\\n4️⃣ VALIDADORES PERSONALIZADOS")
print("-" * 32)

try:
    from config.custom_field_validators import test_validators
    test_validators()
except Exception as e:
    print(f"❌ Error en validadores: {e}")

# %% Test 5: Configuración dinámica
print("\\n5️⃣ CONFIGURACIÓN DINÁMICA")
print("-" * 28)

if modules_ok:
    # Estadísticas del loader
    stats = loader.get_statistics()
    print(f"✓ Campos activos: {stats['active_fields']}")
    print(f"✓ Sinónimos totales: {stats['total_synonyms']}")
    
    # Estadísticas del mapper
    mapper_stats = mapper.get_mapping_statistics()
    print(f"✓ Tipos de campo: {mapper_stats['total_field_types']}")
    print(f"✓ Sistemas ERP: {mapper_stats['erp_systems']}")

print("\\n✅ PRUEBAS COMPLETADAS")
print("💡 Todas las variables están disponibles para uso interactivo")
'''
    
    test_notebook_file = Path('test_notebook_spyder.py')
    test_notebook_file.write_text(test_notebook_content, encoding='utf-8')
    print(f"✓ {test_notebook_file.name}")

def create_requirements_txt():
    """Crea archivo requirements.txt"""
    requirements_content = """# Dependencias del Sistema Dinámico de Detección de Campos
# Instalar con: pip install -r requirements.txt

# Dependencias principales
pandas>=1.5.0
numpy>=1.21.0

# Configuración YAML
PyYAML>=6.0

# Soporte para Excel
openpyxl>=3.0.0
xlrd>=2.0.0

# Dependencias opcionales para funcionalidades avanzadas
# Descomenta las que necesites:

# Para análisis semántico (opcional)
# scikit-learn>=1.0.0
# sentence-transformers>=2.0.0

# Para bases de datos (opcional)
# sqlalchemy>=1.4.0
# psycopg2-binary>=2.9.0  # PostgreSQL
# pymysql>=1.0.0  # MySQL

# Para validaciones avanzadas (opcional)
# python-dateutil>=2.8.0
# regex>=2022.0.0

# Para logging mejorado (opcional)
# colorlog>=6.0.0

# Para testing (desarrollo)
# pytest>=7.0.0
# pytest-cov>=4.0.0
"""
    
    requirements_file = Path('requirements.txt')
    requirements_file.write_text(requirements_content, encoding='utf-8')
    print(f"✓ {requirements_file.name}")

def create_gitignore():
    """Crea archivo .gitignore"""
    gitignore_content = """# Archivos de Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Entornos virtuales
venv/
env/
ENV/
env.bak/
venv.bak/

# Archivos de Spyder
.spyderproject
.spyproject

# Archivos de logs
logs/
*.log

# Archivos temporales
temp/
tmp/
*.tmp
*.bak
*.swp
*.swo
*~

# Datos sensibles
data/confidential/
data/private/
*.csv.backup
*.xlsx.backup

# Backups automáticos
backups/
*.backup

# Archivos de configuración local
config/local_*.yaml
config/local_*.yml
config/local_*.json

# Reportes generados
reports/
*.pdf
*.html

# Archivos de sistema
.DS_Store
Thumbs.db
*.lnk

# Archivos de IDE
.vscode/
.idea/
*.iml

# Archivos de prueba
test_output/
test_data/
"""
    
    gitignore_file = Path('.gitignore')
    gitignore_file.write_text(gitignore_content, encoding='utf-8')
    print(f"✓ {gitignore_file.name}")

def main():
    """Función principal de configuración"""
    print("🕷️ SPYDER SETUP - SISTEMA DINÁMICO DE CAMPOS")
    print("=" * 60)
    
    # Configurar entorno
    project_root = setup_spyder_environment()
    
    # Crear archivos adicionales
    print(f"\n📝 CREANDO ARCHIVOS ADICIONALES")
    print("-" * 35)
    
    create_spyder_quick_start()
    create_spyder_test_notebook()
    create_requirements_txt()
    create_gitignore()
    
    print(f"\n🎉 CONFIGURACIÓN COMPLETADA")
    print("=" * 60)
    print(f"📂 Directorio del proyecto: {project_root}")
    print(f"📅 Configurado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n🚀 PRÓXIMOS PASOS:")
    print(f"  1. Ejecutar: python quick_start_spyder.py")
    print(f"  2. Abrir test_notebook_spyder.py en Spyder")
    print(f"  3. Ejecutar secciones paso a paso")
    print(f"  4. Modificar config/dynamic_fields_config.yaml según necesidades")
    print(f"  5. Añadir tus propios archivos CSV en data/")
    
    print(f"\n📚 ARCHIVOS CREADOS:")
    print(f"  • quick_start_spyder.py - Inicio rápido")
    print(f"  • test_notebook_spyder.py - Notebook de pruebas")
    print(f"  • requirements.txt - Dependencias")
    print(f"  • .gitignore - Control de versiones")
    print(f"  • config/* - Configuración del sistema")
    print(f"  • data/* - Datos de ejemplo")
    
    return project_root

if __name__ == "__main__":
    main()