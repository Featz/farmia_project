# farmia_engine/main_orchestrators.py
import os
import traceback # Para imprimir la traza completa en caso de error

# Usar importaciones relativas si estos módulos están en el mismo paquete (farmia_engine)
from . import utils 
from . import landing_to_raw_processors as ltr_processors
from . import raw_to_bronze_processors as rtb_processors

# No es necesario definir _spark y _dbutils aquí a nivel de módulo,
# ya que utils.py maneja su propia instancia global de sesión de Spark (_spark_session_global)
# y la instancia de dbutils (_dbutils_global).

def execute_landing_to_raw(config_path_override=None):
    """
    Orquesta la ingesta de datos desde la capa Landing a la capa Raw.
    Lee la configuración, itera sobre los datasets definidos y llama al procesador correspondiente.
    """
    entorno_actual = "local" # Default, se actualiza por utils._detectar_entorno()
    active_streams = []  # Para futuros streams continuos si se implementan

    print("INFO: Iniciando proceso Landing -> Raw.")
    try:
        entorno_actual = utils._detectar_entorno()
        spark_session = utils.get_spark_session(entorno_actual, app_name="FarmIA_Ingest_Landing_To_Raw")
        
        config_general = utils.load_app_config(entorno_actual, config_path_override=config_path_override)

        datasets_a_procesar = config_general.get("datasets_to_ingest_to_raw", [])
        if not datasets_a_procesar:
            print("ADVERTENCIA (L->R): No hay datasets en 'datasets_to_ingest_to_raw'. Verificando todas las 'dataset_configs'.")
            datasets_a_procesar = list(config_general.get("dataset_configs", {}).keys())
            if not datasets_a_procesar:
                print("ADVERTENCIA (L->R): No se encontraron datasets para procesar.")
                return

        for nombre_dataset in datasets_a_procesar:
            dataset_cfg = config_general.get("dataset_configs", {}).get(nombre_dataset)
            if not dataset_cfg:
                print(f"ADVERTENCIA (L->R): Configuración para dataset '{nombre_dataset}' no encontrada. Omitiendo.")
                continue

            print(f"--- Iniciando L->R para dataset: {nombre_dataset} ---")
            tipo_dataset = dataset_cfg.get("type")

            env_cfg_base = config_general.get("databricks_env_config") if entorno_actual == "databricks" else config_general.get("local_env_config")
            adls_base_cfg = config_general.get("adls_config_base") if entorno_actual == "databricks" else None

            if not env_cfg_base:
                raise ValueError(f"Configuración de entorno base ('databricks_env_config' o 'local_env_config') no encontrada en config_general.")


            all_paths = utils.construct_full_paths_for_dataset(
                env_cfg_base, 
                adls_base_cfg, 
                dataset_cfg, 
                nombre_dataset, 
                entorno_actual
            )

            source_path = all_paths.get("source_path")
            raw_target_path = all_paths.get("raw_target_path")
            archive_path = all_paths.get("archive_path")
            
            if not all([source_path, raw_target_path, archive_path]):
                print(f"ERROR (L->R): Rutas esenciales (source, raw_target, archive) incompletas para {nombre_dataset}. Omitiendo.")
                print(f"  Paths recibidos: {all_paths}")
                continue

            autoloader_landing_paths = None
            if entorno_actual == "databricks":
                autoloader_landing_paths = {
                    "autoloader_schema_location": all_paths.get("autoloader_landing_schema_location"),
                    "autoloader_checkpoint_location": all_paths.get("autoloader_landing_checkpoint_location")
                }
                if not all(autoloader_landing_paths.values()):
                    print(f"ERROR (L->R): Rutas de Autoloader para landing incompletas para {nombre_dataset}. Omitiendo.")
                    continue
            
            try:
                if tipo_dataset == "csv":
                    ltr_processors.process_csv_dataset(
                        spark_session, 
                        source_path,
                        raw_target_path,
                        archive_path,
                        dataset_cfg, 
                        entorno_actual, 
                        autoloader_landing_paths 
                    )
                # elif tipo_dataset == "json":
                #     ltr_processors.process_json_dataset(...) 
                else:
                    print(f"ADVERTENCIA (L->R): Tipo de dataset '{tipo_dataset}' no soportado para '{nombre_dataset}'. Omitiendo.")
            
            except Exception as e_dataset:
                 print(f"ERROR (L->R) procesando el dataset '{nombre_dataset}': {e_dataset}")
                 traceback.print_exc()

        print("INFO: Proceso Landing -> Raw finalizado.")

    except Exception as e_main:
        print(f"ERROR CRÍTICO en ejecución principal (Landing -> Raw): {e_main}")
        traceback.print_exc()
    finally:
        spark_to_stop = utils._spark_session_global 
        if entorno_actual == "local" and spark_to_stop is not None and not active_streams:
            print("Deteniendo SparkSession local (Landing -> Raw).")
            spark_to_stop.stop()
            utils._spark_session_global = None


def execute_raw_to_bronze(config_path_override=None):
    """
    Orquesta la promoción de datos desde la capa Raw a la capa Bronze.
    Lee la configuración, itera sobre los datasets definidos y llama al procesador correspondiente.
    """
    entorno_actual = "local" # Default
    active_streams_bronze = [] 

    print("INFO: Iniciando proceso Raw -> Bronze.")
    try:
        entorno_actual = utils._detectar_entorno()
        spark_session = utils.get_spark_session(entorno_actual, app_name="FarmIA_Promote_Raw_To_Bronze")
        
        config_general = utils.load_app_config(entorno_actual, config_path_override=config_path_override)

        datasets_a_promover = config_general.get("datasets_to_promote_to_bronze", [])
        if not datasets_a_promover:
            print("ADVERTENCIA (R->B): No hay datasets configurados en 'datasets_to_promote_to_bronze'.")
            return

        for nombre_dataset in datasets_a_promover:
            dataset_cfg = config_general.get("dataset_configs", {}).get(nombre_dataset)
            if not dataset_cfg or not dataset_cfg.get("bronze_config", {}).get("enabled", False):
                print(f"ADVERTENCIA (R->B): Configuración Bronze para '{nombre_dataset}' no encontrada o no habilitada. Omitiendo.")
                continue

            print(f"--- Iniciando promoción RAW -> BRONZE para dataset: {nombre_dataset} ---")
            
            bronze_cfg = dataset_cfg["bronze_config"]
            
            env_cfg_base = config_general.get("databricks_env_config") if entorno_actual == "databricks" else config_general.get("local_env_config")
            adls_base_cfg = config_general.get("adls_config_base") if entorno_actual == "databricks" else None

            if not env_cfg_base:
                raise ValueError(f"Configuración de entorno base ('databricks_env_config' o 'local_env_config') no encontrada en config_general.")

            all_paths = utils.construct_full_paths_for_dataset(
                env_cfg_base, 
                adls_base_cfg, 
                dataset_cfg, 
                nombre_dataset, 
                entorno_actual
            )

            raw_source_path = all_paths.get("raw_target_path") # La salida de L->R es la entrada aquí
            bronze_target_path = all_paths.get("bronze_target_path")
            
            bronze_db_name = bronze_cfg.get("bronze_database_name", "default") # Default cambiado
            bronze_table_name_only = bronze_cfg.get("bronze_table_name", nombre_dataset.replace("/","_") + "_bronze")
            bronze_table_full_name = f"{bronze_db_name}.{bronze_table_name_only}" if entorno_actual == "databricks" else None

            autoloader_raw_paths = None
            if entorno_actual == "databricks" and bronze_cfg.get("autoloader_for_raw_source", False):
                autoloader_raw_paths = {
                    "autoloader_raw_schema_location": all_paths.get("autoloader_raw_schema_location"),
                    "autoloader_raw_checkpoint_location": all_paths.get("autoloader_raw_checkpoint_location")
                }
                if not all(autoloader_raw_paths.values()): # Chequeo robusto
                    print(f"ERROR (R->B): Rutas de Autoloader para fuente raw incompletas para {nombre_dataset}. Omitiendo.")
                    continue
            
            if not raw_source_path or not bronze_target_path:
                print(f"ERROR (R->B): Rutas (raw_source o bronze_target) incompletas para {nombre_dataset}. Omitiendo.")
                print(f"  Paths recibidos: {all_paths}")
                continue

            try:
                # Aquí llamarías al procesador específico para el tipo de datos de raw si fuera necesario,
                # pero como raw es siempre Parquet (por ahora), el procesador es más genérico.
                rtb_processors.process_dataset_raw_to_bronze(
                    spark_session, 
                    raw_source_path, 
                    bronze_target_path, 
                    bronze_table_full_name, 
                    dataset_cfg, 
                    entorno_actual,
                    autoloader_raw_paths
                )
            
            except Exception as e_dataset:
                 print(f"ERROR (R->B) promoviendo el dataset '{nombre_dataset}' a Bronze: {e_dataset}")
                 traceback.print_exc()
        
        print("INFO: Proceso Raw -> Bronze finalizado.")

    except Exception as e_main:
        print(f"ERROR CRÍTICO en la promoción Raw -> Bronze: {e_main}")
        traceback.print_exc()
    finally:
        spark_to_stop = utils._spark_session_global 
        if entorno_actual == "local" and spark_to_stop is not None and not active_streams_bronze:
            print("Deteniendo SparkSession local (Raw -> Bronze).")
            spark_to_stop.stop()
            utils._spark_session_global = None

# Bloque opcional para permitir la ejecución directa de este archivo
# (útil para pruebas locales rápidas del orquestador, pero no para el paquete wheel final)
# if __name__ == "__main__":
#     print("INFO: Ejecutando orquestador de Landing a Raw...")
#     execute_landing_to_raw()
#     print("\nINFO: Ejecutando orquestador de Raw a Bronze...")
#     execute_raw_to_bronze()