# farmia_engine/main_orchestrators.py
import os
import traceback
from . import utils 
from . import landing_to_raw_processors as ltr_processors
from . import raw_to_bronze_processors as rtb_processors
from . import streaming_to_raw_processors as str_processors #


def execute_landing_to_raw(config_path_override=None):
    """
    Orquesta la ingesta de datos desde la capa Landing a la capa Raw (Batch/Autoloader).
    """
    entorno_actual = "local"
    active_streams_ltr = [] # Para Autoloader que es un stream, aunque con trigger(availableNow=True)

    print("INFO: Iniciando proceso Landing -> Raw.")
    try:
        entorno_actual = utils._detectar_entorno()
        spark_session = utils.get_spark_session(entorno_actual, app_name="FarmIA_Ingest_Landing_To_Raw")
        config_general = utils.load_app_config(entorno_actual, config_path_override=config_path_override)

        datasets_a_procesar = config_general.get("datasets_to_ingest_to_raw", [])
        if not datasets_a_procesar:
            print("ADVERTENCIA (L->R): No hay datasets en 'datasets_to_ingest_to_raw'. Intentando procesar todos en 'dataset_configs'.")
            datasets_a_procesar = list(config_general.get("dataset_configs", {}).keys())
            if not datasets_a_procesar:
                print("ADVERTENCIA (L->R): No se encontraron datasets para procesar.")
                return

        for nombre_dataset_key in datasets_a_procesar:
            dataset_cfg = config_general.get("dataset_configs", {}).get(nombre_dataset_key)
            if not dataset_cfg:
                print(f"ADVERTENCIA (L->R): Configuración para dataset '{nombre_dataset_key}' no encontrada. Omitiendo.")
                continue
            
            # Filtrar para no procesar aquí los de Kafka si tienen un tipo muy específico
            # que no maneja landing_to_raw_processors
            if dataset_cfg.get("type", "").startswith("kafka_"):
                print(f"INFO (L->R): Omitiendo dataset '{nombre_dataset_key}' (tipo: {dataset_cfg.get('type')}) en el orquestador landing-to-raw batch.")
                continue

            print(f"--- Iniciando L->R para dataset: {nombre_dataset_key} ---")
            tipo_dataset_batch = dataset_cfg.get("type") # ej. "csv"

            env_cfg_base = config_general.get("databricks_env_config") if entorno_actual == "databricks" else config_general.get("local_env_config")
            adls_base_cfg = config_general.get("adls_config_base") if entorno_actual == "databricks" else None
            if not env_cfg_base: raise ValueError(f"Configuración de entorno base no encontrada.")
            if entorno_actual == "databricks" and not adls_base_cfg: raise ValueError(f"Configuración 'adls_config_base' no encontrada para Databricks.")
            if entorno_actual == "databricks" and adls_base_cfg and \
               (not adls_base_cfg.get("storage_account_name") or adls_base_cfg.get("storage_account_name").startswith("<")):
                try:
                    dynamic_account_name = spark_session.conf.get("adls.account.name")
                    if dynamic_account_name: adls_base_cfg["storage_account_name"] = dynamic_account_name
                    else: raise ValueError("spark.conf.get('adls.account.name') vacío y no hay config fija.")
                except Exception as e_acc:
                    raise ValueError(f"No se pudo obtener 'storage_account_name' para Databricks: {e_acc}")


            all_paths = utils.construct_full_paths_for_dataset(
                env_cfg_base, adls_base_cfg, dataset_cfg, nombre_dataset_key, entorno_actual
            )

            source_path = all_paths.get("source_path")
            raw_target_path = all_paths.get("raw_target_path") # Para Parquet
            archive_path = all_paths.get("archive_path")
            
            if not all([source_path, raw_target_path, archive_path]):
                print(f"ERROR (L->R): Rutas esenciales incompletas para {nombre_dataset_key}. Omitiendo. Paths: {all_paths}")
                continue

            autoloader_landing_paths = None
            if entorno_actual == "databricks":
                autoloader_landing_paths = {
                    "autoloader_schema_location": all_paths.get("autoloader_landing_schema_location"),
                    "autoloader_checkpoint_location": all_paths.get("autoloader_landing_checkpoint_location")
                }
                if not all(autoloader_landing_paths.values()):
                    print(f"ERROR (L->R): Rutas Autoloader landing incompletas para {nombre_dataset_key}. Omitiendo.")
                    continue
            
            try:
                if tipo_dataset_batch == "csv":
                    ltr_processors.process_csv_dataset(
                        spark_session, source_path, raw_target_path, archive_path,
                        dataset_cfg, entorno_actual, autoloader_landing_paths 
                    )
                # Añadir elif para otros tipos de archivos batch (JSON, AVRO directo de landing, etc.)
                # elif tipo_dataset_batch == "json":
                #     ltr_processors.process_json_dataset(...) 
                else:
                    print(f"ADVERTENCIA (L->R): Tipo de dataset batch '{tipo_dataset_batch}' no soportado para '{nombre_dataset_key}'. Omitiendo.")
            
            except Exception as e_dataset:
                 print(f"ERROR (L->R) procesando el dataset '{nombre_dataset_key}': {e_dataset}")
                 traceback.print_exc()

        print("INFO: Proceso Landing -> Raw finalizado.")

    except Exception as e_main:
        print(f"ERROR CRÍTICO en ejecución principal (Landing -> Raw): {e_main}")
        traceback.print_exc()
    finally:
        # La gestión de streams activos (active_streams_ltr) y parada de Spark
        # se maneja al final de la función o por el script que llama si es un stream continuo.
        # Para Autoloader con trigger(availableNow=True), el stream termina, por lo que podemos parar.
        spark_to_stop = utils._spark_session_global 
        if entorno_actual == "local" and spark_to_stop is not None and not active_streams_ltr:
            print("Deteniendo SparkSession local (Landing -> Raw).")
            spark_to_stop.stop()
            utils._spark_session_global = None


def execute_raw_to_bronze(config_path_override=None):
    """
    Orquesta la promoción de datos desde la capa Raw a la capa Bronze (Batch o Streaming desde Raw).
    """
    entorno_actual = "local"
    active_streams_rtb = [] 
    print("INFO: Iniciando proceso Raw -> Bronze.")
    try:
        entorno_actual = utils._detectar_entorno()
        spark_session = utils.get_spark_session(entorno_actual, app_name="FarmIA_Promote_Raw_To_Bronze")
        config_general = utils.load_app_config(entorno_actual, config_path_override=config_path_override)

        adls_base_cfg = config_general.get("adls_config_base")
        if entorno_actual == "databricks":
            if not adls_base_cfg: raise ValueError("'adls_config_base' no encontrada para Databricks.")
            if (not adls_base_cfg.get("storage_account_name") or adls_base_cfg.get("storage_account_name").startswith("<")):
                try:
                    dynamic_account_name = spark_session.conf.get("adls.account.name")
                    if dynamic_account_name: adls_base_cfg["storage_account_name"] = dynamic_account_name
                    else: raise ValueError("spark.conf.get('adls.account.name') vacío y no hay config fija.")
                except Exception as e_acc:
                    raise ValueError(f"No se pudo obtener 'storage_account_name' para Databricks: {e_acc}")
        
        env_cfg_base = config_general.get("databricks_env_config") if entorno_actual == "databricks" else config_general.get("local_env_config")
        if not env_cfg_base: raise ValueError(f"Configuración de entorno base no encontrada.")

        datasets_a_promover = config_general.get("datasets_to_promote_to_bronze", [])
        if not datasets_a_promover:
            print("ADVERTENCIA (R->B): No hay datasets en 'datasets_to_promote_to_bronze'.")
            return

        for nombre_dataset_key in datasets_a_promover:
            dataset_cfg = config_general.get("dataset_configs", {}).get(nombre_dataset_key)
            if not dataset_cfg or not dataset_cfg.get("bronze_config", {}).get("enabled", False):
                print(f"ADVERTENCIA (R->B): Config Bronze para '{nombre_dataset_key}' no encontrada o no habilitada. Omitiendo.")
                continue
            
            # Omitir si este dataset está pensado para ser procesado por un orquestador de streaming diferente
            # para la etapa Raw->Bronze (ej. si el raw es de Kafka y el bronze también es streaming)
            if dataset_cfg.get("type", "").startswith("kafka_"): 
                 # A menos que este orquestador también maneje la promoción de streams de raw (delta/avro) a bronze (delta)
                 # Por ahora, este orquestador se enfoca en batch raw -> bronze.
                 # Si 'autoloader_for_raw_source' está en bronze_config, entonces sí lo procesa como stream.
                if not dataset_cfg.get("bronze_config",{}).get("autoloader_for_raw_source", False) and \
                   dataset_name_key_in_streaming_raw_to_bronze(config_general, nombre_dataset_key):
                    print(f"INFO (R->B): Dataset '{nombre_dataset_key}' parece ser un stream Raw->Bronze. Omitiendo en el orquestador batch Raw->Bronze, a menos que autoloader_for_raw_source sea true.")
                    continue


            print(f"--- Iniciando promoción RAW -> BRONZE para dataset: {nombre_dataset_key} ---")
            bronze_cfg = dataset_cfg["bronze_config"]
            all_paths = utils.construct_full_paths_for_dataset(
                env_cfg_base, adls_base_cfg, dataset_cfg, nombre_dataset_key, entorno_actual
            )

            # La fuente para este proceso es el target de la capa RAW del proceso anterior (Parquet o Delta)
            raw_source_path = all_paths.get("raw_target_path") # Para Parquet batch
            if bronze_cfg.get("raw_source_format") == "delta": # Si raw fue escrito como Delta (ej. Kafka->Raw Delta)
                raw_source_path = all_paths.get("raw_target_delta_path", raw_source_path) # Usar el path delta si existe
            
            bronze_target_path = all_paths.get("bronze_target_delta_path") # Siempre es delta path para bronze
            
            bronze_db_name = bronze_cfg.get("bronze_database_name", "default")
            bronze_table_name_only = bronze_cfg.get("bronze_table_name", nombre_dataset_key.replace("/","_") + "_bronze")
            bronze_table_full_name = f"{bronze_db_name}.{bronze_table_name_only}" if entorno_actual == "databricks" else None

            autoloader_raw_paths = None
            if entorno_actual == "databricks" and bronze_cfg.get("autoloader_for_raw_source", False):
                autoloader_raw_paths = {
                    "autoloader_raw_schema_location": all_paths.get("autoloader_raw_schema_location"),
                    "autoloader_raw_checkpoint_location": all_paths.get("autoloader_raw_checkpoint_location")
                }
                if not all(autoloader_raw_paths.values()):
                    print(f"ERROR (R->B): Rutas Autoloader para raw incompletas para {nombre_dataset_key}. Omitiendo.")
                    continue
            
            if not raw_source_path or not bronze_target_path:
                print(f"ERROR (R->B): Rutas (raw_source o bronze_target) incompletas para {nombre_dataset_key}. Omitiendo.")
                continue

            try:
                rtb_processors.process_dataset_raw_to_bronze(
                    spark_session, raw_source_path, bronze_target_path, bronze_table_full_name, 
                    dataset_cfg, entorno_actual, autoloader_raw_paths
                )
            except Exception as e_dataset:
                 print(f"ERROR (R->B) promoviendo el dataset '{nombre_dataset_key}' a Bronze: {e_dataset}")
                 traceback.print_exc()
        
        print("INFO: Proceso Raw -> Bronze finalizado.")

    except Exception as e_main:
        print(f"ERROR CRÍTICO en la promoción Raw -> Bronze: {e_main}")
        traceback.print_exc()
    finally:
        spark_to_stop = utils._spark_session_global 
        if entorno_actual == "local" and spark_to_stop is not None and not active_streams_rtb: # Asumiendo active_streams_rtb para este
            print("Deteniendo SparkSession local (Raw -> Bronze).")
            spark_to_stop.stop()
            utils._spark_session_global = None

def dataset_name_key_in_streaming_raw_to_bronze(config_general, nombre_dataset_key):
    """Helper para verificar si un dataset está en la lista de streaming raw-to-bronze."""
    datasets_streaming_r_to_b = config_general.get("datasets_to_stream_raw_to_bronze", [])
    return nombre_dataset_key in datasets_streaming_r_to_b


def execute_kafka_avro_payload_to_raw_stream(config_path_override=None): # Nombre de función actualizado
    entorno_actual = "local"
    active_kafka_streams = []
    print("INFO: Iniciando proceso Streaming Kafka (Avro Payload) -> Raw (Parquet Files).")

    try:
        entorno_actual = "local" 
        print(f"INFO: Ejecutando en modo '{entorno_actual}' forzado para Kafka.")
        spark_session = utils.get_spark_session(entorno_actual, app_name="FarmIA_Stream_KafkaAvro_To_RawParquet")
        config_general = utils.load_app_config(entorno_actual, config_path_override=config_path_override)
        env_cfg_base = config_general.get("local_env_config")
        if not env_cfg_base: raise ValueError(f"Configuración 'local_env_config' no encontrada.")
        
        datasets_a_streamear = config_general.get("active_streaming_pipelines", [])

        for nombre_dataset_key in datasets_a_streamear:
            dataset_cfg = config_general.get("dataset_configs", {}).get(nombre_dataset_key)
            
            # Actualizar el tipo esperado
            if not dataset_cfg or dataset_cfg.get("type") != "kafka_avro_payload_to_raw_parquet_files":
                continue

            print(f"--- Iniciando stream Kafka(Avro Payload)->Raw(Parquet) para: {nombre_dataset_key} ---")
            
            all_paths = utils.construct_full_paths_for_dataset(
                env_cfg_base, None, dataset_cfg, nombre_dataset_key, entorno_actual
            )

            kafka_opts = dataset_cfg.get("kafka_options")
            avro_schema_str = dataset_cfg.get("value_avro_schema_string")
            # Usar las claves correctas para Parquet
            raw_parquet_path = all_paths.get("raw_target_parquet_files_path") 
            raw_checkpoint = all_paths.get("raw_stream_checkpoint_path_parquet_files") # Clave de checkpoint para este stream
            raw_partitions = dataset_cfg.get("raw_stream_partition_by")

            if not all([kafka_opts, avro_schema_str, raw_parquet_path, raw_checkpoint]):
                print(f"ERROR: Configuración incompleta para stream Kafka '{nombre_dataset_key}'. Omitiendo.")
                print(f"  KafkaOpts: {bool(kafka_opts)}, AvroSchema: {bool(avro_schema_str)}, RawPath: {bool(raw_parquet_path)}, Checkpoint: {bool(raw_checkpoint)}")
                continue
            
            # os.makedirs para el checkpoint ya se hace en utils.construct_full_paths_for_dataset para local

            # Llamar a la función renombrada del procesador
            query = str_processors.process_kafka_avro_payload_to_raw_parquet_files(
                spark_session, kafka_opts, avro_schema_str, raw_parquet_path, raw_checkpoint, raw_partitions
            )
            if query:
                active_kafka_streams.append(query)
        
        if active_kafka_streams:
            print(f"\nINFO: {len(active_kafka_streams)} stream(s) Kafka(Avro Payload)->Raw(Parquet Files) iniciados.")
            print("      Los streams se ejecutarán hasta que se detenga el script (Ctrl+C) o la sesión de Spark.")
            for q_main in active_kafka_streams: 
                try:
                    q_main.awaitTermination() 
                except Exception as e_await_main: 
                    print(f"ERROR: Stream '{q_main.name if q_main.name else q_main.id}' terminado con error: {e_await_main}")
                    traceback.print_exc()
        else:
            print("INFO: No se iniciaron streams activos para Kafka(Avro Payload)->Raw(Parquet Files).")

    except Exception as e_main_stream:
        print(f"ERROR CRÍTICO en ejecución principal del stream Kafka(Avro Payload)->Raw(Parquet Files): {e_main_stream}")
        traceback.print_exc()
    finally:
        spark_to_stop = utils._spark_session_global 
        if entorno_actual == "local" and spark_to_stop is not None:
            print("Deteniendo SparkSession local (Kafka Stream Orchestrator).")
            for q_stop_main in active_kafka_streams:
                if q_stop_main.isActive:
                    try: q_stop_main.stop()
                    except: pass 
            spark_to_stop.stop()
            utils._spark_session_global = None
