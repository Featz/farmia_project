# farmia_engine/utils.py
import json
import os
import shutil 
import platform
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col, date_format, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, DateType
)

_spark_session_global = None
_dbutils_global = None

LOCAL_CONFIG_FILE_PATH = "config.json"
DATABRICKS_DBFS_CONFIG_PATH = "/dbfs/FileStore/configs/farmia_ingest_config.json"
DELTA_LAKE_VERSION = "3.1.0" 
SPARK_VERSION_FOR_PACKAGES = "3.5.0" 

def _detectar_entorno():
    global _dbutils_global
    if hasattr(_detectar_entorno, 'detected_env'):
        return getattr(_detectar_entorno, 'detected_env')
    try:
        from pyspark.dbutils import DBUtils
        active_session_for_dbutils = SparkSession.getActiveSession()
        if not active_session_for_dbutils:
            setattr(_detectar_entorno, 'detected_env', 'local')
            return "local"
        _dbutils_global = DBUtils(active_session_for_dbutils)
        setattr(_detectar_entorno, 'detected_env', 'databricks')
        return "databricks"
    except:
        setattr(_detectar_entorno, 'detected_env', 'local')
        return "local"

def get_spark_session(env_type=None, app_name="FarmIA_Ingestion_Framework"):
    global _spark_session_global
    if _spark_session_global is None:
        detected_env = env_type or _detectar_entorno()
        active_session = SparkSession.getActiveSession()
        if active_session:
            _spark_session_global = active_session
        elif detected_env == "databricks":
            _spark_session_global = SparkSession.builder.appName(app_name).getOrCreate()
        else: 
            print(f"UTILS: Creando SparkSession para local con soporte Delta, Kafka, Avro. AppName: {app_name}")
            kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION_FOR_PACKAGES}"
            delta_pkg = f"io.delta:delta-spark_2.12:{DELTA_LAKE_VERSION}"
            avro_pkg = f"org.apache.spark:spark-avro_2.12:{SPARK_VERSION_FOR_PACKAGES}"
            packages_to_include = f"{delta_pkg},{kafka_pkg},{avro_pkg}"
            builder = SparkSession.builder.appName(app_name).master("local[*]") \
                .config("spark.sql.parquet.compression.codec", "snappy") \
                .config("spark.jars.packages", packages_to_include) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            _spark_session_global = builder.getOrCreate()
            _spark_session_global.sparkContext.setLogLevel("ERROR")
            print(f"UTILS: SparkSession local creada. Nivel de log: ERROR.")
    return _spark_session_global

def _build_adls_uri(adls_base_cfg, env_cfg_base, container_name_key, base_relative_path_key, specific_subpath=""):
    storage_account = adls_base_cfg.get("storage_account_name")
    if not storage_account or storage_account.startswith("<"):
        raise ValueError(f"'storage_account_name' no está definido correctamente en 'adls_config_base': '{storage_account}'")
    container_name = adls_base_cfg.get(container_name_key)
    if not container_name:
        raise ValueError(f"La clave del contenedor '{container_name_key}' no se encuentra en 'adls_config_base'.")
    base_uri_for_container = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net"
    relative_parts = []
    base_relative_from_env = env_cfg_base.get(base_relative_path_key, "")
    if base_relative_from_env and base_relative_from_env.strip('/'):
        relative_parts.append(base_relative_from_env.strip('/'))
    if specific_subpath and specific_subpath.strip('/'):
        relative_parts.append(specific_subpath.strip('/'))
    full_relative_path = "/".join(relative_parts)
    final_path = base_uri_for_container
    if full_relative_path:
        final_path += f"/{full_relative_path}"
    if not final_path.endswith('/'):
        final_path += "/"
    return final_path

def construct_full_paths_for_dataset(env_cfg_base, adls_base_cfg, dataset_cfg, dataset_name, entorno_actual):
    paths = {key: None for key in [
        "source_path", "raw_target_path", "archive_path", "bronze_target_path",
        "autoloader_landing_schema_location", "autoloader_landing_checkpoint_location",
        "autoloader_raw_schema_location", "autoloader_raw_checkpoint_location",
        "raw_target_delta_path", "raw_stream_checkpoint_path", 
        "raw_target_avro_files_path", "raw_stream_checkpoint_path_avro_files", # Mantener por si acaso
        "raw_target_parquet_files_path", "raw_stream_checkpoint_path_parquet_files", # Nuevo para Parquet
        "bronze_target_delta_path", "bronze_stream_checkpoint_path"
    ]}
    clean_dataset_name_for_paths = dataset_name.replace("/", "_").replace("\\", "_")
    dataset_type = dataset_cfg.get("type", "")

    if entorno_actual == "databricks":
        if not adls_base_cfg: raise ValueError("adls_base_cfg es requerida para Databricks.")
        
        paths["source_path"] = _build_adls_uri(adls_base_cfg, env_cfg_base, "container_landing", "landing_base_relative_path", dataset_cfg.get("source_subpath",""))
        paths["raw_target_path"] = _build_adls_uri(adls_base_cfg, env_cfg_base, "container_raw", "raw_base_relative_path", dataset_cfg.get("raw_target_subpath","")) # Para Parquet batch
        paths["archive_path"] = _build_adls_uri(adls_base_cfg, env_cfg_base, "container_archive", "archive_base_relative_path", dataset_cfg.get("archive_subpath", clean_dataset_name_for_paths))
        
        autoloader_meta_container = adls_base_cfg.get("container_autoloader_metadata", adls_base_cfg.get("container_raw"))
        landing_autoloader_base_rel = env_cfg_base.get('autoloader_metadata_base_relative_path','_autoloader_metadata/landing_to_raw')
        paths["autoloader_landing_schema_location"] = _build_adls_uri(adls_base_cfg, env_cfg_base, autoloader_meta_container, landing_autoloader_base_rel, f"{clean_dataset_name_for_paths}/landing_schema")
        paths["autoloader_landing_checkpoint_location"] = _build_adls_uri(adls_base_cfg, env_cfg_base, autoloader_meta_container, landing_autoloader_base_rel, f"{clean_dataset_name_for_paths}/landing_checkpoint")

        bronze_cfg = dataset_cfg.get("bronze_config")
        if bronze_cfg:
            paths["bronze_target_path"] = _build_adls_uri(adls_base_cfg, env_cfg_base, "container_bronze", "bronze_base_relative_path", bronze_cfg.get("bronze_target_subpath_delta",""))
            paths["bronze_target_delta_path"] = paths["bronze_target_path"]
            if bronze_cfg.get("autoloader_for_raw_source", False):
                raw_autoloader_base_rel = env_cfg_base.get('bronze_autoloader_metadata_base_relative_path','_autoloader_metadata/raw_to_bronze')
                paths["autoloader_raw_schema_location"] = _build_adls_uri(adls_base_cfg, env_cfg_base, autoloader_meta_container, raw_autoloader_base_rel, f"{clean_dataset_name_for_paths}/raw_schema")
                paths["autoloader_raw_checkpoint_location"] = _build_adls_uri(adls_base_cfg, env_cfg_base, autoloader_meta_container, raw_autoloader_base_rel, f"{clean_dataset_name_for_paths}/raw_checkpoint")
        
        stream_chkpt_base_rel = env_cfg_base.get('streaming_checkpoints_base_relative_path','_streaming_checkpoints/')
        if dataset_type == "kafka_avro_payload_to_raw_parquet_files": # Tipo actualizado
            paths["raw_target_parquet_files_path"] = _build_adls_uri(adls_base_cfg, env_cfg_base, "container_raw", "raw_base_relative_path", dataset_cfg.get("raw_target_parquet_files_subpath",""))
            paths["raw_stream_checkpoint_path_parquet_files"] = _build_adls_uri(adls_base_cfg, env_cfg_base, autoloader_meta_container, stream_chkpt_base_rel, dataset_cfg.get("raw_stream_checkpoint_subpath", f"{clean_dataset_name_for_paths}/kafka_to_raw_parquet_checkpoint"))
        
        if bronze_cfg and bronze_cfg.get("bronze_stream_checkpoint_subpath"):
            paths["bronze_stream_checkpoint_path"] = _build_adls_uri(adls_base_cfg, env_cfg_base, autoloader_meta_container, stream_chkpt_base_rel, bronze_cfg.get("bronze_stream_checkpoint_subpath"))

    elif entorno_actual == "local":
        paths["source_path"] = os.path.join(env_cfg_base["landing_base_path"], dataset_cfg.get("source_subpath",""))
        paths["raw_target_path"] = os.path.join(env_cfg_base["raw_base_path"], dataset_cfg.get("raw_target_subpath",""))
        paths["archive_path"] = os.path.join(env_cfg_base["archive_base_path"], dataset_cfg.get("archive_subpath", clean_dataset_name_for_paths))
        
        bronze_cfg = dataset_cfg.get("bronze_config")
        if bronze_cfg:
            paths["bronze_target_path"] = os.path.join(env_cfg_base.get("bronze_base_path","lakehouse/bronze/"), bronze_cfg.get("bronze_target_subpath_delta",""))
            paths["bronze_target_delta_path"] = paths["bronze_target_path"]
            if bronze_cfg.get("autoloader_for_raw_source", False):
                local_autoloader_raw_meta_base = os.path.join(env_cfg_base.get("bronze_base_path","lakehouse/bronze/"), "_autoloader_metadata_raw")
                paths["autoloader_raw_schema_location"] = os.path.join(local_autoloader_raw_meta_base, clean_dataset_name_for_paths, "schema/")
                paths["autoloader_raw_checkpoint_location"] = os.path.join(local_autoloader_raw_meta_base, clean_dataset_name_for_paths, "checkpoint/")
                for key_cs in ["autoloader_raw_schema_location", "autoloader_raw_checkpoint_location"]:
                    if paths[key_cs] and not paths[key_cs].endswith(os.sep): paths[key_cs] += os.sep
        
        local_streaming_chkpt_base = env_cfg_base.get("streaming_checkpoint_base_path", "lakehouse/checkpoints/")
        if dataset_type == "kafka_avro_payload_to_raw_parquet_files": # Tipo actualizado
            paths["raw_target_parquet_files_path"] = os.path.join(env_cfg_base.get("raw_base_path","lakehouse/raw/"), dataset_cfg.get("raw_target_parquet_files_subpath",""))
            raw_stream_chkpt_parquet_val = os.path.join(local_streaming_chkpt_base, dataset_cfg.get("raw_stream_checkpoint_subpath", f"{clean_dataset_name_for_paths}/kafka_to_raw_parquet_checkpoint/"))
            os.makedirs(raw_stream_chkpt_parquet_val, exist_ok=True)
            paths["raw_stream_checkpoint_path_parquet_files"] = raw_stream_chkpt_parquet_val

        if bronze_cfg and bronze_cfg.get("bronze_stream_checkpoint_subpath"):
            bronze_stream_chkpt_val = os.path.join(local_streaming_chkpt_base, bronze_cfg.get("bronze_stream_checkpoint_subpath"))
            os.makedirs(bronze_stream_chkpt_val, exist_ok=True)
            paths["bronze_stream_checkpoint_path"] = bronze_stream_chkpt_val
            
    return paths

def load_app_config(env_type, config_path_override=None):
    config_path = config_path_override or (DATABRICKS_DBFS_CONFIG_PATH if env_type == "databricks" else LOCAL_CONFIG_FILE_PATH)
    effective_path = config_path
    if env_type == "databricks" and not config_path.startswith("/dbfs/"):
         if config_path.startswith("dbfs:"):
            effective_path = config_path.replace("dbfs:", "/dbfs", 1)
    print(f"UTILS: Cargando configuración desde: {effective_path}")
    try:
        with open(effective_path, 'r') as f: config_data = json.load(f)
        print(f"UTILS: Configuración cargada exitosamente.")
        return config_data
    except FileNotFoundError: raise FileNotFoundError(f"ARCHIVO DE CONFIGURACIÓN NO ENCONTRADO: {effective_path}")
    except Exception as e: raise Exception(f"Error al cargar/procesar config desde {effective_path}: {e}")

def build_spark_schema_from_config(schema_config_json):
    type_mapping = {"string": StringType(), "integer": IntegerType(), "double": DoubleType(), "timestamp": TimestampType()}
    if not schema_config_json or not isinstance(schema_config_json.get("fields"), list):
        print("ADVERTENCIA (UTILS): Configuración de esquema inválida o sin campos. Se devolverá None.")
        return None
    fields = []
    for fc in schema_config_json.get("fields", []):
        field_name = fc.get("name")
        field_type_str = fc.get("type","string").lower()
        spark_type = type_mapping.get(field_type_str)
        if not field_name: continue 
        if not spark_type: spark_type = StringType()
        fields.append(StructField(field_name, spark_type, True))
    return StructType(fields) if fields else None

def prepare_dataframe_for_partitioning(df, partition_cfg):
    if not partition_cfg or not isinstance(partition_cfg, dict): return df, []
    cols_to_partition_by = partition_cfg.get("columns_to_partition_by", [])
    derive_from_col_name = partition_cfg.get("derive_date_parts_from_column")
    df_with_partitions = df
    final_partition_cols = list(cols_to_partition_by)
    if derive_from_col_name and derive_from_col_name in df.columns:
        source_col_type = df.schema[derive_from_col_name].dataType
        temp_derived_df = df_with_partitions 
        if not isinstance(source_col_type, (TimestampType, DateType)):
            print(f"    INFO (UTILS - Particionamiento): Convirtiendo '{derive_from_col_name}' de {source_col_type} a TimestampType.")
            temp_derived_df = temp_derived_df.withColumn(derive_from_col_name, col(derive_from_col_name).cast(TimestampType()))
        df_with_partitions = temp_derived_df 
        if "year" in cols_to_partition_by:
            df_with_partitions = df_with_partitions.withColumn("year", year(col(derive_from_col_name)))
        if "month" in cols_to_partition_by:
            df_with_partitions = df_with_partitions.withColumn("month", month(col(derive_from_col_name)))
        if "day" in cols_to_partition_by:
            df_with_partitions = df_with_partitions.withColumn("day", dayofmonth(col(derive_from_col_name)))
    elif derive_from_col_name and derive_from_col_name not in df.columns:
        print(f"ADVERTENCIA (UTILS - Particionamiento): Columna '{derive_from_col_name}' no existe en DataFrame.")
    for p_col in final_partition_cols:
        if p_col not in df_with_partitions.columns:
            raise ValueError(f"Columna de partición '{p_col}' no existe. Columnas: {df_with_partitions.columns}")
    return df_with_partitions, final_partition_cols

def get_dbutils():
    global _dbutils_global
    if _dbutils_global is None: _detectar_entorno()
    return _dbutils_global if _dbutils_global else None

def archive_processed_files(spark, processed_files_df_subset, base_archive_path, entorno_actual):
    dbu = get_dbutils()
    files_to_archive_rows = processed_files_df_subset.collect()
    source_file_uris = [row.source_filename for row in files_to_archive_rows if row.source_filename]
    if not source_file_uris: return
    print(f"  ARCHIVADO: Intentando archivar {len(source_file_uris)} fichero(s) a base: {base_archive_path}")
    for source_file_uri in source_file_uris:
        original_uri_for_log = source_file_uri
        try:
            parsed_uri_for_basename = urlparse(source_file_uri)
            file_name = os.path.basename(unquote(parsed_uri_for_basename.path))
            archive_file_path_destination = os.path.join(base_archive_path, file_name).replace("\\", "/")
            path_to_move_from = source_file_uri
            if entorno_actual == "local" and source_file_uri.startswith('file:///'):
                parsed_uri = urlparse(source_file_uri)
                path_component = unquote(parsed_uri.path)
                os_specific_path = url2pathname(path_component)
                if platform.system() == "Windows":
                    if os_specific_path.startswith('/') and len(os_specific_path) > 2 and os_specific_path[1].isalpha() and os_specific_path[2] == ':':
                        os_specific_path = os_specific_path[1:] 
                    elif os_specific_path.startswith('\\') and len(os_specific_path) > 2 and os_specific_path[1].isalpha() and os_specific_path[2] == ':':
                         os_specific_path = os_specific_path[1:]
                path_to_move_from = os_specific_path
            if entorno_actual == "databricks" and dbu:
                archive_dir = os.path.dirname(archive_file_path_destination)
                dbu.fs.mkdirs(archive_dir)
                dbu.fs.mv(source_file_uri, archive_file_path_destination, recurse=False)
            elif entorno_actual == "local":
                os.makedirs(os.path.dirname(archive_file_path_destination), exist_ok=True)
                shutil.move(path_to_move_from, archive_file_path_destination)
        except Exception as e:
            path_info_for_error = f"URI original: {original_uri_for_log}"
            if entorno_actual == "local" and original_uri_for_log != path_to_move_from:
                path_info_for_error += f", Ruta OS intentada: {path_to_move_from}"
            print(f"    ERROR AL ARCHIVAR ({path_info_for_error}): {e}")