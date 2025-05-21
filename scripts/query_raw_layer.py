# scripts/query_raw_layer.py
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
except ImportError as e:
    print(f"Error: No se pudo importar 'farmia_engine.utils'. Asegúrate de que:")
    print(f"1. Estás ejecutando este script desde el directorio raíz del proyecto ('{project_root}').")
    print(f"2. O que el paquete 'farmia_engine' está instalado en tu entorno Python.")
    print(f"Detalle del error de importación: {e}")
    sys.exit(1)

def query_raw_dataset(dataset_name_key: str):
    """
    Lee, muestra información y valida un dataset Parquet en la capa raw.
    Este script está diseñado para ejecución local.
    """
    print(f"\n======================================================================")
    print(f"CONSULTANDO CAPA RAW PARA EL DATASET: {dataset_name_key}")
    print(f"======================================================================")

    entorno_actual = "local" # Forzamos entorno local para este script de consulta
    spark = None # Inicializar spark a None para el bloque finally

    try:
        # Obtener SparkSession (utils.py se encarga de configurarla para local)
        spark = utils.get_spark_session(env_type=entorno_actual, app_name=f"QueryRawLayer_{dataset_name_key}")
        
        # Cargar configuración general
        config_general = utils.load_app_config(entorno_actual) # config_path_override es opcional

        dataset_cfg = config_general.get("dataset_configs", {}).get(dataset_name_key)
        if not dataset_cfg:
            print(f"ERROR: Configuración para el dataset '{dataset_name_key}' no encontrada en config.json.")
            return

        local_env_cfg_base = config_general.get("local_env_config")
        if not local_env_cfg_base:
            print(f"ERROR: 'local_env_config' no encontrada en config.json.")
            return

        # Construir la ruta al directorio raw del dataset usando la lógica de utils
        # Para el entorno local, adls_base_cfg no es necesario para construct_full_paths_for_dataset
        all_paths = utils.construct_full_paths_for_dataset(
            local_env_cfg_base, 
            None,  # adls_base_cfg
            dataset_cfg, 
            dataset_name_key, 
            entorno_actual
        )

        raw_parquet_path = all_paths.get("raw_target_path")

        if not raw_parquet_path:
            print(f"ERROR: No se pudo determinar la ruta 'raw_target_path' para el dataset '{dataset_name_key}'.")
            return

        print(f"\nIntentando leer datos Parquet desde: {raw_parquet_path}\n")

        try:
            df = spark.read.parquet(raw_parquet_path)
        except AnalysisException as e:
            if "Path does not exist" in str(e) or "Unable to infer schema" in str(e):
                print(f"ERROR: La ruta especificada no existe, está vacía o no contiene archivos Parquet válidos: {raw_parquet_path}")
                print(f"       Asegúrate de que el proceso 'main_ingest_landing_to_raw.py' se haya ejecutado correctamente para este dataset.")
                return
            else:
                print(f"ERROR AL LEER PARQUET: {e}")
                traceback.print_exc()
                return
        
        if df.rdd.isEmpty():
            print(f"INFORMACIÓN: El dataset '{dataset_name_key}' en la capa raw está vacío (0 registros).")
            print(f"         Ruta consultada: {raw_parquet_path}")
            df.printSchema() # Aun así, imprimir el esquema si se pudo inferir de un directorio vacío con _common_metadata
            return


        print(f"--- Esquema del Dataset '{dataset_name_key}' en Raw ---")
        df.printSchema()

        print(f"\n--- Muestra de Datos (primeras 10 filas) del Dataset '{dataset_name_key}' en Raw ---")
        df.show(10, truncate=False)

        record_count = df.count()
        print(f"\n--- Conteo de Registros ---")
        print(f"El dataset '{dataset_name_key}' en la capa raw contiene {record_count} registros.")
        
        # Puedes añadir más validaciones aquí según tus necesidades:
        # Ejemplo: Chequear nulos en columnas que no deberían tenerlos
        # print("\n--- Chequeo de Nulos (ejemplo para 'order_id') ---")
        # if "order_id" in df.columns:
        #     null_counts = df.where(col("order_id").isNull()).count()
        #     print(f"Registros con 'order_id' nulo: {null_counts}")
        # else:
        #     print("Columna 'order_id' no encontrada para chequeo de nulos.")

        # Ejemplo: Resumen estadístico (puede ser verboso)
        # print("\n--- Resumen Estadístico ---")
        # df.summary().show()


    except Exception as e:
        print(f"ERROR GENERAL durante la consulta a la capa raw para '{dataset_name_key}': {e}")
        traceback.print_exc()
    finally:
        if spark: # Solo intentar detener si la sesión de Spark fue creada
            print("\nDeteniendo SparkSession de consulta...")
            spark.stop()
            # Es buena práctica resetear la global en utils si este script es el "dueño" de la sesión
            # para que otras ejecuciones (o pruebas) puedan crear una nueva si es necesario.
            if hasattr(utils, '_spark_session_global') and utils._spark_session_global is not None:
                 utils._spark_session_global = None 
        print(f"======================================================================")
        print(f"CONSULTA A CAPA RAW PARA {dataset_name_key} FINALIZADA")
        print(f"======================================================================")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script para consultar y validar datos Parquet en la capa Raw local.",
        formatter_class=argparse.RawTextHelpFormatter # Para mejor formato de ayuda
    )
    parser.add_argument(
        "dataset_name", 
        type=str, 
        help="El nombre clave del dataset a consultar (ej. 'sales_online_csv') tal como está definido en config.json."
    )
    
    # Ejemplo de cómo podrías añadir más argumentos en el futuro:
    # parser.add_argument(
    #     "-n", "--num_rows", 
    #     type=int, 
    #     default=10,
    #     help="Número de filas a mostrar en la muestra (default: 10)."
    # )

    args = parser.parse_args()

    query_raw_dataset(args.dataset_name)