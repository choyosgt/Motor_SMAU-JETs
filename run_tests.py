#!/usr/bin/env python3
# run_tests.py - Script para ejecutar tests básicos del sistema

import sys
from pathlib import Path

# Añadir directorio del proyecto al path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

if __name__ == "__main__":
    try:
        from tests.test_basic_functionality import run_all_tests
        result = run_all_tests()
        
        # Código de salida basado en resultado
        if result.failures or result.errors:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except ImportError as e:
        print(f"❌ Error importando tests: {e}")
        print("Asegúrate de que el archivo esté en tests/test_basic_functionality.py")
        sys.exit(2)
    except Exception as e:
        print(f"❌ Error ejecutando tests: {e}")
        sys.exit(3)
