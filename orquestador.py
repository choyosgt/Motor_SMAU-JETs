#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orquestador de Pipeline de Procesamiento de Datos Contables
============================================================

Este script orquesta el flujo completo desde un archivo data-pre hasta
generar un CSV listo para el automatic trainer.

Flujo del Pipeline:
1. Tomar archivo raw de data-pre
2. Procesarlo con el modelo de clasificación
3. Post-procesar las predicciones
4. Aplicar automatic trainer para generar CSVs finales

Autor: Sistema de Mapeo de Campos Contables
Fecha: 2025
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path
import subprocess
import json
import pandas as pd
from typing import Dict, List, Optional, Tuple

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """
    Orquestador principal del pipeline de procesamiento
    
    Características:
    - Ejecuta el pipeline completo de procesamiento de datos
    - Normaliza automáticamente el separador CSV a coma (,)
    - Maneja diferentes formatos de entrada (CSV, TXT, XLSX)
    - Genera reportes detallados de ejecución
    - Validación robusta en cada paso
    """
    
    def __init__(self, config_path: str = "config/pipeline_config.json"):
        """
        Inicializa el orquestador
        
        Args:
            config_path: Ruta al archivo de configuración del pipeline
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results_dir = Path("results")
        self.temp_dir = Path("temp")
        self.predictions_dir = Path("predicciones")
        
        # Crear directorios necesarios
        self._create_directories()
        
        # Estado del pipeline
        self.pipeline_status = {
            'start_time': None,
            'end_time': None,
            'steps_completed': [],
            'current_step': None,
            'errors': []
        }
    
    def _load_config(self) -> Dict:
        """Carga la configuración del pipeline"""
        default_config = {
            'model_dir': 'modelo',
            'data_pre_dir': 'data_pre',
            'automatic_trainer_confidence': 0.75,
            'erp_detection': 'auto',
            'cleanup_temp_files': True
        }
        
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    loaded_config = json.load(f)
                default_config.update(loaded_config)
            except Exception as e:
                logger.warning(f"Error loading config: {e}. Using defaults.")
        
        return default_config
    
    def _create_directories(self):
        """Crea los directorios necesarios para el pipeline"""
        dirs = [self.results_dir, self.temp_dir, self.predictions_dir]
        for dir_path in dirs:
            dir_path.mkdir(exist_ok=True)
            logger.debug(f"Directory ensured: {dir_path}")
    
    def _execute_command(self, command: List[str], description: str) -> Tuple[bool, str]:
        """
        Ejecuta un comando del sistema y captura la salida
        
        Args:
            command: Lista con el comando y argumentos
            description: Descripción del paso para logging
            
        Returns:
            Tupla (success, output)
        """
        logger.info(f"Executing: {description}")
        logger.debug(f"Command: {' '.join(command)}")
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"✅ {description} completed successfully")
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            error_msg = f"❌ {description} failed: {e.stderr}"
            logger.error(error_msg)
            self.pipeline_status['errors'].append(error_msg)
            return False, e.stderr
        except Exception as e:
            error_msg = f"❌ Unexpected error in {description}: {str(e)}"
            logger.error(error_msg)
            self.pipeline_status['errors'].append(error_msg)
            return False, str(e)
    
    def step1_validate_input(self, input_file: str) -> bool:
        """
        Paso 1: Validar archivo de entrada
        """
        self.pipeline_status['current_step'] = 'validate_input'
        logger.info("=" * 60)
        logger.info("STEP 1: Validating input file")
        logger.info("=" * 60)
        
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            return False
        
        # Validar extensión
        valid_extensions = ['.csv', '.txt', '.xlsx', '.xls']
        file_ext = Path(input_file).suffix.lower()
        if file_ext not in valid_extensions:
            logger.error(f"Invalid file extension: {file_ext}. Supported: {valid_extensions}")
            return False
        
        # Validar tamaño
        file_size = os.path.getsize(input_file) / (1024 * 1024)  # MB
        if file_size > 100:
            logger.warning(f"Large file detected ({file_size:.2f} MB). Processing may take time.")
        
        logger.info(f"✅ Input file validated: {input_file}")
        logger.info(f"   Extension: {file_ext}")
        logger.info(f"   Size: {file_size:.2f} MB")
        
        self.pipeline_status['steps_completed'].append('validate_input')
        return True
    
    def step2_run_model_prediction(self, input_file: str) -> Optional[str]:
        """
        Paso 2: Ejecutar predicción del modelo
        """
        self.pipeline_status['current_step'] = 'model_prediction'
        logger.info("=" * 60)
        logger.info("STEP 2: Running model prediction")
        logger.info("=" * 60)
        
        # Generar nombre de archivo de salida
        input_name = Path(input_file).stem
        output_file = self.predictions_dir / f"predictions_{input_name}_{self.timestamp}.csv"
        
        command = [
            sys.executable,
            "test_model.py",
            "--model-dir", self.config['model_dir'],
            "--file", input_file,
            "--out", str(output_file)
        ]
        
        success, output = self._execute_command(
            command,
            "Model prediction"
        )
        
        if success and output_file.exists():
            logger.info(f"✅ Predictions saved to: {output_file}")
            self.pipeline_status['steps_completed'].append('model_prediction')
            return str(output_file)
        else:
            logger.error("Model prediction failed")
            return None
    
    def step3_process_predictions(self, predictions_file: str) -> Optional[str]:
        """
        Paso 3: Procesar predicciones para estructurar datos
        """
        self.pipeline_status['current_step'] = 'process_predictions'
        logger.info("=" * 60)
        logger.info("STEP 3: Processing predictions")
        logger.info("=" * 60)
        
        # Generar nombre de archivo de salida
        output_file = self.temp_dir / f"processed_{self.timestamp}.csv"
        
        command = [
            sys.executable,
            "procesador_predicciones.py",
            predictions_file,
            "--salida", str(output_file)
        ]
        
        success, output = self._execute_command(
            command,
            "Predictions processing"
        )
        
        if success and output_file.exists():
            logger.info(f"✅ Processed data saved to: {output_file}")
            self.pipeline_status['steps_completed'].append('process_predictions')
            return str(output_file)
        else:
            logger.error("Predictions processing failed")
            return None
    
    def step4_run_automatic_trainer(self, processed_file: str, erp_hint: Optional[str] = None) -> Dict:
        """
        Paso 4: Ejecutar automatic trainer para generar CSVs finales
        """
        self.pipeline_status['current_step'] = 'automatic_trainer'
        logger.info("=" * 60)
        logger.info("STEP 4: Running automatic trainer")
        logger.info("=" * 60)
        
        # Determinar ERP hint
        if erp_hint is None and self.config['erp_detection'] != 'auto':
            erp_hint = self.config['erp_detection']
        
        # Construir comando
        command = [sys.executable, "automatic_confirmation_trainer.py", processed_file]
        if erp_hint:
            command.append(erp_hint)
            logger.info(f"Using ERP hint: {erp_hint}")
        
        success, output = self._execute_command(
            command,
            "Automatic trainer"
        )
        
        result = {
            'success': success,
            'header_file': None,
            'detail_file': None,
            'report_file': None
        }
        
        if success:
            # Buscar archivos generados
            for line in output.split('\n'):
                if 'Header CSV saved:' in line:
                    result['header_file'] = line.split(':')[-1].strip()
                elif 'Detail CSV saved:' in line:
                    result['detail_file'] = line.split(':')[-1].strip()
                elif 'Training report saved' in line:
                    result['report_file'] = line.split(':')[-1].strip()
            
            logger.info(f"✅ Automatic trainer completed")
            if result['header_file']:
                logger.info(f"   Header: {result['header_file']}")
            if result['detail_file']:
                logger.info(f"   Detail: {result['detail_file']}")
            if result['report_file']:
                logger.info(f"   Report: {result['report_file']}")
            
            self.pipeline_status['steps_completed'].append('automatic_trainer')
        else:
            logger.error("Automatic trainer failed")
        
        return result
    
    def step5_cleanup(self):
        """
        Paso 5: Limpiar archivos temporales (opcional)
        """
        if not self.config.get('cleanup_temp_files', True):
            logger.info("Cleanup skipped (configured)")
            return
        
        self.pipeline_status['current_step'] = 'cleanup'
        logger.info("=" * 60)
        logger.info("STEP 5: Cleaning up temporary files")
        logger.info("=" * 60)
        
        try:
            temp_files = list(self.temp_dir.glob(f"*{self.timestamp}*"))
            for temp_file in temp_files:
                temp_file.unlink()
                logger.debug(f"Deleted: {temp_file}")
            
            logger.info(f"✅ Cleaned up {len(temp_files)} temporary files")
            self.pipeline_status['steps_completed'].append('cleanup')
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")
    
    def generate_summary_report(self, results: Dict) -> str:
        """
        Genera un reporte resumen del pipeline
        """
        report_file = self.results_dir / f"pipeline_report_{self.timestamp}.txt"
        
        with open(report_file, 'w') as f:
            f.write("=" * 70 + "\n")
            f.write("PIPELINE EXECUTION REPORT\n")
            f.write("=" * 70 + "\n\n")
            
            f.write(f"Timestamp: {self.timestamp}\n")
            f.write(f"Start Time: {self.pipeline_status['start_time']}\n")
            f.write(f"End Time: {self.pipeline_status['end_time']}\n")
            
            duration = (self.pipeline_status['end_time'] - self.pipeline_status['start_time']).total_seconds()
            f.write(f"Total Duration: {duration:.2f} seconds\n\n")
            
            f.write("STEPS COMPLETED:\n")
            for step in self.pipeline_status['steps_completed']:
                f.write(f"  ✅ {step}\n")
            
            if self.pipeline_status['errors']:
                f.write("\nERRORS:\n")
                for error in self.pipeline_status['errors']:
                    f.write(f"  ❌ {error}\n")
            
            f.write("\nOUTPUT FILES:\n")
            if results.get('header_file'):
                f.write(f"  - Header CSV: {results['header_file']}\n")
            if results.get('detail_file'):
                f.write(f"  - Detail CSV: {results['detail_file']}\n")
            if results.get('report_file'):
                f.write(f"  - Training Report: {results['report_file']}\n")
            
            f.write("\nCONFIGURATION:\n")
            for key, value in self.config.items():
                f.write(f"  - {key}: {value}\n")
        
        return str(report_file)
    
    def run_pipeline(self, input_file: str, erp_hint: Optional[str] = None) -> Dict:
        """
        Ejecuta el pipeline completo
        
        Args:
            input_file: Archivo de entrada (data-pre)
            erp_hint: Hint del ERP (opcional)
            
        Returns:
            Diccionario con resultados del pipeline
        """
        logger.info("🚀 Starting Pipeline Orchestrator")
        logger.info(f"Input file: {input_file}")
        
        self.pipeline_status['start_time'] = datetime.now()
        results = {
            'success': False,
            'header_file': None,
            'detail_file': None,
            'report_file': None,
            'pipeline_report': None
        }
        
        try:
            # Paso 1: Validar entrada
            if not self.step1_validate_input(input_file):
                raise ValueError("Input validation failed")
            
            # Paso 2: Predicción del modelo
            predictions_file = self.step2_run_model_prediction(input_file)
            if not predictions_file:
                raise RuntimeError("Model prediction failed")
            
            # Paso 3: Procesar predicciones
            processed_file = self.step3_process_predictions(predictions_file)
            if not processed_file:
                raise RuntimeError("Predictions processing failed")
            
            # Paso 4: Automatic trainer
            trainer_results = self.step4_run_automatic_trainer(processed_file, erp_hint)
            results.update(trainer_results)
            
            if not trainer_results['success']:
                raise RuntimeError("Automatic trainer failed")
            
            # Paso 5: Limpieza
            self.step5_cleanup()
            
            results['success'] = True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.pipeline_status['errors'].append(str(e))
        
        finally:
            self.pipeline_status['end_time'] = datetime.now()
            self.pipeline_status['current_step'] = None
            
            # Generar reporte resumen
            results['pipeline_report'] = self.generate_summary_report(results)
            
            # Mostrar resumen
            self._print_summary(results)
        
        return results
    
    def _print_summary(self, results: Dict):
        """Imprime resumen del pipeline"""
        print("\n" + "=" * 70)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 70)
        
        if results['success']:
            print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
            print("\n📄 OUTPUT FILES:")
            if results.get('header_file'):
                print(f"   • Header CSV: {results['header_file']}")
            if results.get('detail_file'):
                print(f"   • Detail CSV: {results['detail_file']}")
            if results.get('report_file'):
                print(f"   • Training Report: {results['report_file']}")
            print(f"   • Pipeline Report: {results['pipeline_report']}")
        else:
            print("❌ PIPELINE FAILED")
            if self.pipeline_status['errors']:
                print("\n⚠️ ERRORS:")
                for error in self.pipeline_status['errors']:
                    print(f"   • {error}")
        
        duration = (self.pipeline_status['end_time'] - self.pipeline_status['start_time']).total_seconds()
        print(f"\n⏱️ Total execution time: {duration:.2f} seconds")
        print("=" * 70)


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description='Orquestador de Pipeline de Procesamiento de Datos Contables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python orquestador.py data_pre/ejemplo.csv
  python orquestador.py data_pre/ejemplo.csv --erp SAP
  python orquestador.py data_pre/ejemplo.csv --config custom_config.json
  python orquestador.py data_pre/ejemplo.csv --no-cleanup
        """
    )
    
    parser.add_argument(
        'input_file',
        help='Archivo de entrada desde data-pre (CSV, TXT, XLSX)'
    )
    
    parser.add_argument(
        '--erp',
        dest='erp_hint',
        help='Hint del sistema ERP (SAP, ContaPlus, etc.)',
        default=None
    )
    
    parser.add_argument(
        '--config',
        help='Archivo de configuración del pipeline',
        default='config/pipeline_config.json'
    )
    
    parser.add_argument(
        '--no-cleanup',
        action='store_true',
        help='No eliminar archivos temporales'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Mostrar salida detallada'
    )
    
    args = parser.parse_args()
    
    # Configurar nivel de logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Crear orquestador
    orchestrator = PipelineOrchestrator(config_path=args.config)
    
    # Actualizar configuración si se especifica no-cleanup
    if args.no_cleanup:
        orchestrator.config['cleanup_temp_files'] = False
    
    # Ejecutar pipeline
    results = orchestrator.run_pipeline(
        input_file=args.input_file,
        erp_hint=args.erp_hint
    )
    
    # Retornar código de salida apropiado
    sys.exit(0 if results['success'] else 1)


if __name__ == "__main__":
    main()