# scripts/run_local_raw_to_bronze.py
import sys
import os

# --- Añadir el directorio raíz del proyecto a sys.path ---
current_file_path = os.path.abspath(__file__)
scripts_dir = os.path.dirname(current_file_path)
project_root = os.path.dirname(scripts_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- Fin de la configuración de sys.path ---

try:
    from farmia_engine.main_orchestrators import execute_raw_to_bronze
except ImportError as e:
    print(f"Error: No se pudo importar 'farmia_engine'. Asegúrate de que estás ejecutando este script")
    print(f"desde el directorio raíz del proyecto ('{project_root}'), o que 'farmia_engine' está instalado.")
    print(f"Detalle del error de importación: {e}")
    sys.exit(1)

def run_local_promotion_to_bronze():
    """
    Punto de entrada para ejecutar la promoción local de Raw a Bronze.
    """
    print("======================================================================")
    print("INICIANDO SCRIPT LOCAL: RAW -> BRONZE")
    print("======================================================================")
    
    # Al igual que con el script anterior, config.json se buscará por defecto
    # en el directorio de ejecución.
    # config_file = os.path.join(project_root, "config.json")
    # execute_raw_to_bronze(config_path_override=config_file)

    execute_raw_to_bronze(config_path_override=None)
    
    print("======================================================================")
    print("SCRIPT LOCAL: RAW -> BRONZE FINALIZADO")
    print("======================================================================")

if __name__ == "__main__":
    run_local_promotion_to_bronze()