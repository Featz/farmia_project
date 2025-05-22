#!/usr/bin/env python
# run_kafka_to_raw.py
import os
import sys
import traceback

# Añadir el directorio raíz del proyecto al PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from farmia_engine.main_orchestrators import execute_landing_to_raw

def run_kafka_ingestion_to_raw():
    """
    Punto de entrada para ejecutar la ingesta de datos desde Kafka a la capa Raw.
    Este script está específicamente diseñado para procesar el dataset de telemetría de sensores.
    """
    print("======================================================================")
    print("INICIANDO SCRIPT: KAFKA -> RAW (SENSOR TELEMETRY)")
    print("======================================================================")
    
    try:
        # La ruta a config.json se resolverá por defecto a "config.json" en el directorio
        # desde donde se ejecuta este script (idealmente, el raíz del proyecto).
        execute_landing_to_raw(config_path_override=None)
        
        print("======================================================================")
        print("SCRIPT: KAFKA -> RAW FINALIZADO EXITOSAMENTE")
        print("======================================================================")
        
    except Exception as e:
        print("======================================================================")
        print("ERROR EN LA EJECUCIÓN DEL SCRIPT: KAFKA -> RAW")
        print("======================================================================")
        print(f"Error: {str(e)}")
        print("\nTraceback completo:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_kafka_ingestion_to_raw() 