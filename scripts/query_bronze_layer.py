# scripts/query_bronze_layer.py
import sys
import os
import argparse # Para manejar argumentos de línea de comandos
import traceback

# --- Añadir el directorio raíz del proyecto a sys.path ---
current_file_path = os.path.abspath(__file__)
scripts_dir = os.path.dirname(current_file_path)
project_root = os.path.dirname(scripts_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- Fin de la configuración de sys.path ---

try:
    # Asumimos que utils está dentro de tu paquete farmia_engine
    from farmia_engine import utils 
    from pyspark.sql.utils import AnalysisException
    from delta.tables import DeltaTable # Para operaciones específicas de Delta como el historial
except ImportError as e:
    print(f"Error: No se pudo importar 'farmia_engine.utils' o 'delta.tables'. Asegúrate de que:")
    print(f"1. Estás ejecutando este script desde el directorio raíz del proyecto ('{project_root}').")
    print(f"2. O que el paquete 'farmia_engine' está instalado y 'delta-spark' está en tu requirements.txt y entorno Python.")
    print(f"Detalle del error de importación: {e}")
    sys.exit(1)

def query_bronze_dataset(dataset_name_key: str):
    """
    Lee, muestra información y valida una tabla Delta en la capa bronze.
    Este script está diseñado para ejecución local.
    """
    print(f"\n======================================================================")
    print(f"CONSULTANDO CAPA BRONZE (DELTA) PARA EL DATASET: {dataset_name_key}")
    print(f"======================================================================")

    entorno_actual = "local" # Forzamos entorno local para este script de consulta
    spark = None # Inicializar spark a None para el bloque finally

    try:
        # Obtener SparkSession (utils.py se encarga de configurarla para local con soporte Delta)
        spark = utils.get_spark_session(env_type=entorno_actual, app_name=f"QueryBronzeLayer_{dataset_name_key}")
        
        config_general = utils.load_app_config(entorno_actual)

        dataset_cfg = config_general.get("dataset_configs", {}).get(dataset_name_key)
        if not dataset_cfg:
            print(f"ERROR: Configuración para el dataset '{dataset_name_key}' no encontrada en config.json.")
            return

        bronze_cfg = dataset_cfg.get("bronze_config")
        if not bronze_cfg or not bronze_cfg.get("enabled", False):
            print(f"ERROR: Configuración Bronze para '{dataset_name_key}' no encontrada o no habilitada en config.json.")
            return

        local_env_cfg_base = config_general.get("local_env_config")
        if not local_env_cfg_base:
            print(f"ERROR: 'local_env_config' no encontrada en config.json.")
            return
            
        all_paths = utils.construct_full_paths_for_dataset(
            local_env_cfg_base, 
            None,  # adls_base_cfg no es relevante para rutas locales
            dataset_cfg, 
            dataset_name_key, 
            entorno_actual
        )

        bronze_delta_path = all_paths.get("bronze_target_path")
        # El nombre completo de la tabla (ej. db.table) es más para Databricks o si tienes Hive Metastore local.
        # Para este script de validación local, leer por path es más directo.
        # bronze_table_full_name = f"{bronze_cfg.get('bronze_database_name', 'default')}.{bronze_cfg.get('bronze_table_name', 'tabla_desconocida')}"


        if not bronze_delta_path:
            print(f"ERROR: No se pudo determinar la ruta 'bronze_target_path' para el dataset '{dataset_name_key}'.")
            return

        print(f"\nIntentando leer tabla Delta desde la ruta: {bronze_delta_path}\n")

        is_delta_table_check = False
        try:
            if DeltaTable.isDeltaTable(spark, bronze_delta_path):
                print(f"Confirmado: La ruta '{bronze_delta_path}' es una tabla Delta.")
                is_delta_table_check = True
            else:
                if os.path.exists(bronze_delta_path) and not os.listdir(bronze_delta_path):
                     print(f"INFORMACIÓN: La ruta '{bronze_delta_path}' existe pero está vacía.")
                else:
                     print(f"ADVERTENCIA: La ruta '{bronze_delta_path}' existe pero no parece ser una tabla Delta (según DeltaTable.isDeltaTable).")
                # Se intentará leer de todas formas; spark.read.format("delta") fallará si no lo es.
        except Exception as e_is_delta: # Captura errores si la ruta no existe, etc.
            print(f"ADVERTENCIA: No se pudo verificar si '{bronze_delta_path}' es una tabla Delta. Error: {e_is_delta}")
            print(f"             Asegúrate de que el proceso 'main_promote_raw_to_bronze.py' se haya ejecutado para este dataset.")


        df = None
        try:
            df = spark.read.format("delta").load(bronze_delta_path)
        except AnalysisException as e:
            if "Path does not exist" in str(e) or "is not a Delta table" in str(e).lower():
                print(f"ERROR: La ruta especificada no existe o no es una tabla Delta: {bronze_delta_path}")
                print(f"       Asegúrate de que el proceso 'main_promote_raw_to_bronze.py' se haya ejecutado correctamente.")
                return
            else: # Otro tipo de AnalysisException
                print(f"ERROR AL LEER TABLA DELTA (AnalysisException): {e}")
                traceback.print_exc()
                return
        except Exception as e_general_read: # Otras excepciones durante la lectura
            print(f"ERROR GENERAL AL LEER TABLA DELTA: {e_general_read}")
            traceback.print_exc()
            return

        if df.rdd.isEmpty():
            print(f"INFORMACIÓN: La tabla Delta '{dataset_name_key}' en la capa bronze está VACÍA (0 registros).")
            print(f"         Ruta consultada: {bronze_delta_path}")
            # Intentar imprimir el esquema incluso si está vacía, Delta lo permite.
            print(f"\n--- Esquema de la Tabla Delta '{dataset_name_key}' (vacía) en Bronze ---")
            df.printSchema()
            return


        print(f"--- Esquema de la Tabla Delta '{dataset_name_key}' en Bronze ---")
        df.printSchema()

        print(f"\n--- Muestra de Datos (primeras 10 filas) de la Tabla Delta '{dataset_name_key}' en Bronze ---")
        df.show(10, truncate=False)

        record_count = df.count()
        print(f"\n--- Conteo de Registros ---")
        print(f"La tabla Delta '{dataset_name_key}' en la capa bronze contiene {record_count} registros.")

        if is_delta_table_check: # Solo intentar mostrar historial si se confirmó que es Delta
            try:
                print(f"\n--- Historial de la Tabla Delta '{dataset_name_key}' ---")
                delta_table_history = DeltaTable.forPath(spark, bronze_delta_path).history()
                delta_table_history.select("version", "timestamp", "operation", "operationParameters", "operationMetrics").show(truncate=False)
            except Exception as e_history:
                print(f"ADVERTENCIA: No se pudo obtener el historial de la tabla Delta. Error: {e_history}")
        
    except Exception as e:
        print(f"ERROR GENERAL durante la consulta a la capa bronze para '{dataset_name_key}': {e}")
        traceback.print_exc()
    finally:
        if spark:
            print("\nDeteniendo SparkSession de consulta...")
            spark.stop()
            if hasattr(utils, '_spark_session_global') and utils._spark_session_global is not None:
                 utils._spark_session_global = None 
        print(f"======================================================================")
        print(f"CONSULTA A CAPA BRONZE PARA {dataset_name_key} FINALIZADA")
        print(f"======================================================================")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script para consultar y validar tablas Delta en la capa Bronze local.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "dataset_name", 
        type=str, 
        help="El nombre clave del dataset a consultar (ej. 'sales_online_csv') tal como está definido en config.json."
    )
    args = parser.parse_args()

    query_bronze_dataset(args.dataset_name)