# scripts/run_local_landing_to_raw.py
import sys
import os

# --- Añadir el directorio raíz del proyecto a sys.path ---
# Esto permite que Python encuentre el paquete 'farmia_engine' cuando
# este script se ejecuta desde dentro del directorio 'scripts/' o desde
# el directorio raíz del proyecto, y 'farmia_engine' no está instalado formalmente.

# Ruta al archivo actual (scripts/run_local_landing_to_raw.py)
current_file_path = os.path.abspath(__file__)
# Ruta al directorio 'scripts/'
scripts_dir = os.path.dirname(current_file_path)
# Ruta al directorio raíz del proyecto (un nivel arriba de 'scripts/')
project_root = os.path.dirname(scripts_dir)

# Añadir el directorio raíz del proyecto al inicio de sys.path
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- Fin de la configuración de sys.path ---

# Ahora podemos importar desde farmia_engine
try:
    from farmia_engine.main_orchestrators import execute_landing_to_raw
except ImportError as e:
    print(f"Error: No se pudo importar 'farmia_engine'. Asegúrate de que estás ejecutando este script")
    print(f"desde el directorio raíz del proyecto ('{project_root}'), o que 'farmia_engine' está instalado.")
    print(f"Detalle del error de importación: {e}")
    sys.exit(1)

def run_local_ingestion_to_raw():
    """
    Punto de entrada para ejecutar la ingesta local de Landing a Raw.
    """
    print("======================================================================")
    print("INICIANDO SCRIPT LOCAL: LANDING -> RAW")
    print("======================================================================")
    
    # La ruta a config.json se resolverá por defecto a "config.json" en el directorio
    # desde donde se ejecuta este script (idealmente, el raíz del proyecto).
    # Si config.json estuviera en otro lugar, se podría usar config_path_override.
    # Ejemplo: config_file = os.path.join(project_root, "config.json")
    # execute_landing_to_raw(config_path_override=config_file)
    
    execute_landing_to_raw(config_path_override=None) 
    
    print("======================================================================")
    print("SCRIPT LOCAL: LANDING -> RAW FINALIZADO")
    print("======================================================================")

if __name__ == "__main__":
    run_local_ingestion_to_raw()