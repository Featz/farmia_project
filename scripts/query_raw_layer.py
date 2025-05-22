# scripts/query_raw_layer.py
import sys
import os
import argparse 
import traceback

# --- Añadir el directorio raíz del proyecto a sys.path ---
current_file_path = os.path.abspath(__file__)
scripts_dir = os.path.dirname(current_file_path)
project_root = os.path.dirname(scripts_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- Fin de la configuración de sys.path ---

try:
    from farmia_engine import utils 
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.functions import col # Importar col para validaciones opcionales
except ImportError as e:
    print(f"Error: No se pudo importar 'farmia_engine.utils'. Asegúrate de que:")
    print(f"1. Estás ejecutando este script desde el directorio raíz del proyecto ('{project_root}').")
    print(f"2. O que el paquete 'farmia_engine' está instalado en tu entorno Python.")
    print(f"Detalle del error de importación: {e}")
    sys.exit(1)

def query_raw_dataset(dataset_name_key: str):
    """
    Lee, muestra información y valida un dataset en la capa raw.
    Intenta leer Parquet por defecto, pero puede ajustarse si la config indica otro formato.
    Este script está diseñado para ejecución local.
    """
    print(f"\n======================================================================")
    print(f"CONSULTANDO CAPA RAW PARA EL DATASET: {dataset_name_key}")
    print(f"======================================================================")

    entorno_actual = "local" 
    spark = None 

    try:
        spark = utils.get_spark_session(env_type=entorno_actual, app_name=f"QueryRawLayer_{dataset_name_key}")
        config_general = utils.load_app_config(entorno_actual)

        dataset_cfg = config_general.get("dataset_configs", {}).get(dataset_name_key)
        if not dataset_cfg:
            print(f"ERROR: Configuración para el dataset '{dataset_name_key}' no encontrada en config.json.")
            return

        local_env_cfg_base = config_general.get("local_env_config")
        if not local_env_cfg_base:
            print(f"ERROR: 'local_env_config' no encontrada en config.json.")
            return

        all_paths = utils.construct_full_paths_for_dataset(
            local_env_cfg_base, 
            None, 
            dataset_cfg, 
            dataset_name_key, 
            entorno_actual
        )

        raw_data_path = None
        dataset_type = dataset_cfg.get("type")
        # Por defecto, este script consulta Parquet, pero podríamos leer el formato de raw_source_format
        # de la sección bronze_config si este script fuera más genérico.
        # Por ahora, nos enfocamos en las rutas Parquet de la capa raw.
        
        if dataset_type == "csv": # El output de landing->raw para CSV es Parquet en "raw_target_path"
            raw_data_path = all_paths.get("raw_target_path")
            print(f"INFO: El dataset '{dataset_name_key}' (tipo {dataset_type}) se espera en formato Parquet en la capa raw.")
        elif dataset_type == "kafka_avro_payload_to_raw_parquet_files":
            raw_data_path = all_paths.get("raw_target_parquet_files_path")
            print(f"INFO: El dataset '{dataset_name_key}' (tipo {dataset_type}) se espera en formato Parquet en la capa raw.")
        else:
            # Intento genérico si el tipo no coincide con los anteriores explícitamente
            raw_data_path = all_paths.get("raw_target_path") # Común para batch
            if not raw_data_path: # Fallback para streams que escriben Parquet
                raw_data_path = all_paths.get("raw_target_parquet_files_path")
            
            if raw_data_path:
                print(f"ADVERTENCIA: Tipo de dataset '{dataset_type}' no manejado explícitamente para la ruta RAW. "
                      f"Intentando con: {raw_data_path}. Se asumirá formato Parquet.")
            else:
                print(f"ERROR: No se pudo determinar la ruta RAW para el dataset '{dataset_name_key}' con tipo '{dataset_type}'.")
                print(f"       Revisa la config del dataset y las claves de ruta en utils.construct_full_paths_for_dataset.")
                print(f"       Paths disponibles en all_paths: {all_paths.keys()}")
                return

        if not raw_data_path:
            print(f"ERROR: La ruta RAW para el dataset '{dataset_name_key}' es None o no está configurada correctamente para su tipo.")
            return

        print(f"\nIntentando leer datos Parquet desde: {raw_data_path}\n")

        try:
            # Este script está enfocado en Parquet, pero podrías adaptarlo para leer otros formatos si fuera necesario
            df = spark.read.parquet(raw_data_path) 
        except AnalysisException as e:
            if "Path does not exist" in str(e) or "Unable to infer schema" in str(e) or "is not a Parquet file" in str(e):
                print(f"ERROR: La ruta especificada no existe, está vacía o no contiene archivos Parquet válidos: {raw_data_path}")
                print(f"       Asegúrate de que el pipeline correspondiente (ej. landing-to-raw) se haya ejecutado correctamente.")
                return
            else:
                print(f"ERROR AL LEER PARQUET DESDE RAW: {e}")
                traceback.print_exc()
                return
        
        if df.rdd.isEmpty():
            print(f"INFORMACIÓN: El dataset '{dataset_name_key}' en la capa raw ({raw_data_path}) está VACÍO (0 registros).")
            try:
                # Intentar imprimir el esquema incluso si está vacío (Parquet puede tener _common_metadata)
                print(f"\n--- Esquema del Dataset '{dataset_name_key}' (vacío) en Raw ---")
                df.printSchema()
            except: pass # Fallar silenciosamente si no se puede obtener esquema de un DF vacío sin datos
            return

        print(f"--- Esquema del Dataset '{dataset_name_key}' en Raw ---")
        df.printSchema()

        print(f"\n--- Muestra de Datos (primeras 10 filas) del Dataset '{dataset_name_key}' en Raw ---")
        df.show(10, truncate=False)

        record_count = df.count()
        print(f"\n--- Conteo de Registros ---")
        print(f"El dataset '{dataset_name_key}' en la capa raw contiene {record_count} registros.")

    except Exception as e:
        print(f"ERROR GENERAL durante la consulta a la capa raw para '{dataset_name_key}': {e}")
        traceback.print_exc()
    finally:
        if spark:
            print("\nDeteniendo SparkSession de consulta...")
            spark.stop()
            if hasattr(utils, '_spark_session_global') and utils._spark_session_global is not None:
                 utils._spark_session_global = None 
        print(f"======================================================================")
        print(f"CONSULTA A CAPA RAW PARA {dataset_name_key} FINALIZADA")
        print(f"======================================================================")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script para consultar y validar datos Parquet en la capa Raw local.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "dataset_name", 
        type=str, 
        help="El nombre clave del dataset a consultar (ej. 'sales_online_csv' o 'sensor_telemetry_kafka') tal como está definido en config.json."
    )
    args = parser.parse_args()

    query_raw_dataset(args.dataset_name)