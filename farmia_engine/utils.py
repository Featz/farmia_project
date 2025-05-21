# utils.py
import json
import os
import shutil 
import platform
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, DateType
)

# Variables globales a nivel de módulo para SparkSession y DBUtils
# Estas se inicializan una vez por las funciones get_spark_session y _detectar_entorno
_spark_session_global = None
_dbutils_global = None

# Constantes para rutas de configuración
LOCAL_CONFIG_FILE_PATH = "config.json"
DATABRICKS_DBFS_CONFIG_PATH = "/dbfs/FileStore/configs/farmia_ingest_config.json" # Ruta accesible por open() en Databricks

# Versión de Delta Lake a usar con Spark local
# Compatible con Spark 3.5.x. Verificar la última versión compatible si es necesario.
DELTA_LAKE_VERSION = "3.1.0" 

def _detectar_entorno():
    """
    Detecta si el script se ejecuta en un entorno Databricks o localmente.
    Cachea la instancia de DBUtils si se encuentra.
    """
    global _dbutils_global
    # Si ya se detectó y _dbutils_global está (o no) establecido, no volver a intentar.
    if _dbutils_global is not None or hasattr(_detectar_entorno, 'detected_env'):
        return getattr(_detectar_entorno, 'detected_env', 'local')

    try:
        from pyspark.dbutils import DBUtils
        # SparkSession.getActiveSession() puede devolver None si no hay sesión activa.
        active_session_for_dbutils = SparkSession.getActiveSession()
        if not active_session_for_dbutils:
            # No se puede inicializar DBUtils sin una sesión activa. Asumir local.
            setattr(_detectar_entorno, 'detected_env', 'local')
            return "local"
        _dbutils_global = DBUtils(active_session_for_dbutils)
        setattr(_detectar_entorno, 'detected_env', 'databricks')
        return "databricks"
    except (ImportError, AttributeError, TypeError, Exception): # Captura más amplia por si acaso
        setattr(_detectar_entorno, 'detected_env', 'local')
        return "local"

def get_spark_session(env_type=None, app_name="FarmIA_Ingestion_Framework"):
    """
    Obtiene o crea una SparkSession global, adaptada al entorno.
    Si env_type no se proporciona, se detecta automáticamente.
    """
    global _spark_session_global
    if _spark_session_global is None:
        detected_env = env_type or _detectar_entorno() # Usa env_type si se pasa, sino detecta.
        
        # Intenta obtener una sesión activa existente (común en Databricks notebooks)
        active_session = SparkSession.getActiveSession()
        if active_session:
            _spark_session_global = active_session
            print(f"UTILS: Usando SparkSession activa existente. AppName: {_spark_session_global.conf.get('spark.app.name')}")
        elif detected_env == "databricks":
            print(f"UTILS: Creando SparkSession para Databricks. AppName: {app_name}")
            # En un job de Databricks o si no hay sesión global, esto la creará.
            # Para Delta, Databricks Runtimes ya incluyen las extensiones.
            _spark_session_global = SparkSession.builder.appName(app_name).getOrCreate()
        else: # Entorno local
            print(f"UTILS: Creando SparkSession para local con soporte Delta. AppName: {app_name}")
            builder = SparkSession.builder.appName(app_name).master("local[*]") \
                .config("spark.sql.parquet.compression.codec", "snappy") \
                .config("spark.jars.packages", f"io.delta:delta-spark_2.12:{DELTA_LAKE_VERSION}") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            _spark_session_global = builder.getOrCreate()
            # Configurar nivel de log para reducir verbosidad en local
            _spark_session_global.sparkContext.setLogLevel("ERROR")
            print(f"UTILS: SparkSession local creada con Delta Lake {DELTA_LAKE_VERSION}. Nivel de log: ERROR.")
            
    return _spark_session_global

def construct_full_paths_for_dataset(env_cfg_base, adls_base_cfg, dataset_cfg, dataset_name, entorno_actual):
    """
    Construye todas las rutas necesarias (landing, raw, archive, bronze, metadata)
    para un dataset específico, adaptado al entorno.
    env_cfg_base: Corresponde a local_env_config o databricks_env_config.
    adls_base_cfg: Corresponde a adls_config_base (solo para Databricks).
    dataset_cfg: Configuración específica del dataset (de dataset_configs[dataset_name]).
    dataset_name: Nombre clave del dataset (ej. "sales_online_csv").
    """
    source_path, raw_target_path, archive_path, bronze_target_path = None, None, None, None
    autoloader_landing_schema_loc, autoloader_landing_checkpoint_loc = None, None
    autoloader_raw_schema_loc, autoloader_raw_checkpoint_loc = None, None

    clean_dataset_name_for_paths = dataset_name.replace("/", "_").replace("\\", "_")

    if entorno_actual == "databricks":
        if not adls_base_cfg:
            raise ValueError("adls_base_cfg es requerida para el entorno Databricks en la construcción de rutas.")

        storage_account = adls_base_cfg["storage_account_name"]
        container_landing = adls_base_cfg["container_landing"]
        container_raw = adls_base_cfg["container_raw"]
        container_bronze = adls_base_cfg.get("container_bronze", container_raw) 
        container_archive = adls_base_cfg.get("container_archive", container_landing)

        landing_base_adls = f"abfss://{container_landing}@{storage_account}.dfs.core.windows.net/{env_cfg_base['landing_base_relative_path'].strip('/')}"
        raw_base_adls = f"abfss://{container_raw}@{storage_account}.dfs.core.windows.net/{env_cfg_base['raw_base_relative_path'].strip('/')}"
        bronze_base_adls = f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/{env_cfg_base.get('bronze_base_relative_path','bronze/').strip('/')}"
        archive_base_adls = f"abfss://{container_archive}@{storage_account}.dfs.core.windows.net/{env_cfg_base.get('archive_base_relative_path', 'landing_archived/').strip('/')}"
        
        # Rutas de metadatos de Autoloader para landing->raw
        autoloader_landing_meta_base = f"abfss://{container_raw}@{storage_account}.dfs.core.windows.net/{env_cfg_base.get('autoloader_metadata_base_relative_path','_autoloader_metadata/').strip('/')}"
        autoloader_landing_schema_loc = os.path.join(autoloader_landing_meta_base, clean_dataset_name_for_paths, "landing_schema").replace("\\","/")
        autoloader_landing_checkpoint_loc = os.path.join(autoloader_landing_meta_base, clean_dataset_name_for_paths, "landing_checkpoint").replace("\\","/")

        # Rutas de metadatos de Autoloader para raw->bronze (si se usa Autoloader para leer raw)
        autoloader_raw_meta_base = f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/{env_cfg_base.get('bronze_autoloader_metadata_base_relative_path','_bronze_autoloader_metadata/').strip('/')}"
        autoloader_raw_schema_loc = os.path.join(autoloader_raw_meta_base, clean_dataset_name_for_paths, "raw_schema").replace("\\","/")
        autoloader_raw_checkpoint_loc = os.path.join(autoloader_raw_meta_base, clean_dataset_name_for_paths, "raw_checkpoint").replace("\\","/")
        
        source_path = os.path.join(landing_base_adls, dataset_cfg["source_subpath"]).replace("\\","/")
        raw_target_path = os.path.join(raw_base_adls, dataset_cfg["raw_target_subpath"]).replace("\\","/")
        archive_path = os.path.join(archive_base_adls, dataset_cfg.get("archive_subpath", clean_dataset_name_for_paths)).replace("\\","/")
        
        if dataset_cfg.get("bronze_config"):
            bronze_target_path = os.path.join(bronze_base_adls, dataset_cfg["bronze_config"]["bronze_target_subpath_delta"]).replace("\\","/")

    elif entorno_actual == "local":
        source_path = os.path.join(env_cfg_base["landing_base_path"], dataset_cfg["source_subpath"])
        raw_target_path = os.path.join(env_cfg_base["raw_base_path"], dataset_cfg["raw_target_subpath"])
        archive_path = os.path.join(env_cfg_base["archive_base_path"], dataset_cfg.get("archive_subpath", clean_dataset_name_for_paths))
        
        if dataset_cfg.get("bronze_config"):
            bronze_target_path = os.path.join(env_cfg_base.get("bronze_base_path","lakehouse/bronze/"), dataset_cfg["bronze_config"]["bronze_target_subpath_delta"])
            # Rutas para Autoloader leyendo raw local (generalmente no se usa Autoloader localmente así)
            autoloader_raw_schema_loc = os.path.join(env_cfg_base.get("bronze_base_path","lakehouse/bronze/"), "_autoloader_metadata_raw", clean_dataset_name_for_paths, "schema")
            autoloader_raw_checkpoint_loc = os.path.join(env_cfg_base.get("bronze_base_path","lakehouse/bronze/"), "_autoloader_metadata_raw", clean_dataset_name_for_paths, "checkpoint")
    
    return {
        "source_path": source_path,
        "raw_target_path": raw_target_path,
        "archive_path": archive_path,
        "bronze_target_path": bronze_target_path,
        "autoloader_landing_schema_location": autoloader_landing_schema_loc,
        "autoloader_landing_checkpoint_location": autoloader_landing_checkpoint_loc,
        "autoloader_raw_schema_location": autoloader_raw_schema_loc,
        "autoloader_raw_checkpoint_location": autoloader_raw_checkpoint_loc
    }

def load_app_config(env_type, config_path_override=None):
    """Carga el archivo de configuración principal."""
    config_path = config_path_override or (DATABRICKS_DBFS_CONFIG_PATH if env_type == "databricks" else LOCAL_CONFIG_FILE_PATH)
    
    print(f"UTILS: Cargando configuración desde: {config_path}")
    try:
        # En Databricks, /dbfs/ es el prefijo para acceder a DBFS con operaciones de E/S de Python estándar.
        # Si la ruta ya tiene /dbfs/, no se duplica.
        effective_path = config_path
        if env_type == "databricks" and not config_path.startswith("/dbfs/"):
            # Asumir que la ruta es relativa a dbfs:/ si no tiene el prefijo /dbfs/
            # Esto es principalmente para cuando config_path_override se usa desde un Job de Databricks
            # y se pasa como "dbfs:/FileStore/..."
             if config_path.startswith("dbfs:"):
                effective_path = config_path.replace("dbfs:", "/dbfs", 1)

        with open(effective_path, 'r') as f: 
            config_data = json.load(f)
        print(f"UTILS: Configuración cargada exitosamente.")
        return config_data
    except FileNotFoundError: 
        raise FileNotFoundError(f"ARCHIVO DE CONFIGURACIÓN NO ENCONTRADO: {effective_path}")
    except KeyError as e: 
        raise KeyError(f"Clave de configuración esencial faltante en {effective_path}: {e}")
    except Exception as e: 
        raise Exception(f"Error al cargar/procesar config desde {effective_path}: {e}")

def build_spark_schema_from_config(schema_config_json):
    """Construye un StructType de Spark desde la configuración JSON del esquema."""
    type_mapping = {"string": StringType(), "integer": IntegerType(), "double": DoubleType(), "timestamp": TimestampType()}
    if not schema_config_json or not isinstance(schema_config_json.get("fields"), list): # Verificación más robusta
        # Devolver None o un esquema vacío si la configuración no es válida,
        # permitiendo que Autoloader o el lector intenten inferir.
        # O lanzar un error si un esquema siempre es mandatorio.
        print("ADVERTENCIA (UTILS): Configuración de esquema inválida o campos no definidos. Se intentará inferir esquema si es posible.")
        return None # Opcional: raise ValueError("Configuración de esquema inválida o campos no definidos.")
        
    fields = []
    for fc in schema_config_json["fields"]:
        field_name = fc.get("name")
        field_type_str = fc.get("type","string").lower() # Default a string si el tipo no está
        spark_type = type_mapping.get(field_type_str)
        if not field_name:
            print(f"ADVERTENCIA (UTILS): Campo sin nombre en schema_config, omitiendo: {fc}")
            continue
        if not spark_type:
            print(f"ADVERTENCIA (UTILS): Tipo no soportado '{field_type_str}' para campo '{field_name}', usando StringType.")
            spark_type = StringType()
        fields.append(StructField(field_name, spark_type, True))
    return StructType(fields)

def prepare_dataframe_for_partitioning(df, partition_cfg):
    """
    Añade columnas derivadas para particionamiento si está configurado.
    partition_cfg: Un diccionario con "columns_to_partition_by" y opcionalmente "derive_date_parts_from_column".
    """
    if not partition_cfg: 
        return df, []

    cols_to_partition_by = partition_cfg.get("columns_to_partition_by", [])
    derive_from_col_name = partition_cfg.get("derive_date_parts_from_column")
    
    df_with_partitions = df
    final_partition_cols = list(cols_to_partition_by) # Las columnas que se usarán para particionar

    if derive_from_col_name and derive_from_col_name in df.columns:
        # Columnas a derivar vs columnas finales de partición
        # Ej: derive_date_parts_from_column = "order_date"
        #     columns_to_partition_by = ["year", "month", "day"]
        # Esto implica que "year", "month", "day" son los *nombres* de las columnas derivadas.
        
        # Verificar tipo de la columna fuente y castear si es necesario
        source_col_type = df.schema[derive_from_col_name].dataType
        if not isinstance(source_col_type, (TimestampType, DateType)):
            print(f"    INFO (UTILS - Particionamiento): Convirtiendo columna '{derive_from_col_name}' de {source_col_type} a TimestampType.")
            df_with_partitions = df_with_partitions.withColumn(derive_from_col_name, col(derive_from_col_name).cast(TimestampType()))
        
        # Derivar las partes de fecha si están listadas en columns_to_partition_by
        if "year" in cols_to_partition_by:
            print(f"    INFO (UTILS - Particionamiento): Derivando columna 'year' desde '{derive_from_col_name}'.")
            df_with_partitions = df_with_partitions.withColumn("year", year(col(derive_from_col_name)))
        if "month" in cols_to_partition_by:
            print(f"    INFO (UTILS - Particionamiento): Derivando columna 'month' desde '{derive_from_col_name}'.")
            df_with_partitions = df_with_partitions.withColumn("month", month(col(derive_from_col_name)))
        if "day" in cols_to_partition_by:
            print(f"    INFO (UTILS - Particionamiento): Derivando columna 'day' desde '{derive_from_col_name}'.")
            df_with_partitions = df_with_partitions.withColumn("day", dayofmonth(col(derive_from_col_name)))
    elif derive_from_col_name and derive_from_col_name not in df.columns:
        print(f"ADVERTENCIA (UTILS - Particionamiento): Columna para derivar particiones '{derive_from_col_name}' no existe en el DataFrame.")
    
    # Validar que todas las columnas finales de partición existan
    for p_col in final_partition_cols:
        if p_col not in df_with_partitions.columns:
            raise ValueError(f"Columna de partición especificada '{p_col}' no existe en el DataFrame. Columnas disponibles: {df_with_partitions.columns}")
            
    return df_with_partitions, final_partition_cols

def get_dbutils():
    """Devuelve la instancia de DBUtils si está disponible (entorno Databricks)."""
    # _detectar_entorno() se llama en get_spark_session(), que usualmente se llama primero.
    # Si se llama a get_dbutils() antes, _dbutils_global podría ser None.
    # Forzar la detección si es necesario.
    if _dbutils_global is None and _detectar_entorno() == "databricks":
         # _detectar_entorno() ya habrá intentado inicializar _dbutils_global
         pass # No hacer nada más, _dbutils_global ya está seteado o no se pudo
    
    if _dbutils_global: # Solo devuelve si no es None
        return _dbutils_global
    return None

def archive_processed_files(spark, processed_files_df_subset, base_archive_path, entorno_actual):
    """
    Archiva los ficheros procesados desde su ubicación original (en landing) a una carpeta de archivo.
    processed_files_df_subset: DataFrame con una única columna "source_filename".
    base_archive_path: Ruta base (ya incluye subruta del dataset) de ADLS o local donde se archivarán los ficheros.
    """
    dbu = get_dbutils()
    
    files_to_archive_rows = processed_files_df_subset.collect()
    source_file_uris = [row.source_filename for row in files_to_archive_rows if row.source_filename]
    
    if not source_file_uris:
        # print("  ARCHIVADO: No hay archivos para archivar en este lote.") # Comentado para reducir verbosidad
        return

    print(f"  ARCHIVADO: Intentando archivar {len(source_file_uris)} fichero(s) a la base: {base_archive_path}")

    for source_file_uri in source_file_uris:
        original_uri_for_log = source_file_uri
        try:
            parsed_uri_for_basename = urlparse(source_file_uri)
            file_name = os.path.basename(unquote(parsed_uri_for_basename.path))
            archive_file_path_destination = os.path.join(base_archive_path, file_name).replace("\\", "/")
            path_to_move_from = source_file_uri

            if entorno_actual == "local":
                if source_file_uri.startswith('file:///'):
                    parsed_uri = urlparse(source_file_uri)
                    path_component = unquote(parsed_uri.path)
                    os_specific_path = url2pathname(path_component)
                    if platform.system() == "Windows":
                        if os_specific_path.startswith('/') and len(os_specific_path) > 2 and os_specific_path[1].isalpha() and os_specific_path[2] == ':':
                            os_specific_path = os_specific_path[1:] 
                        elif os_specific_path.startswith('\\') and len(os_specific_path) > 2 and os_specific_path[1].isalpha() and os_specific_path[2] == ':':
                             os_specific_path = os_specific_path[1:]
                    path_to_move_from = os_specific_path
            
            # Asegurar que el directorio de destino exista para archivado
            if entorno_actual == "databricks" and dbu:
                archive_dir = os.path.dirname(archive_file_path_destination)
                dbu.fs.mkdirs(archive_dir) # dbutils.fs.mkdirs crea si no existe, no falla si existe.
                dbu.fs.mv(source_file_uri, archive_file_path_destination, recurse=False)
            elif entorno_actual == "local":
                os.makedirs(os.path.dirname(archive_file_path_destination), exist_ok=True)
                shutil.move(path_to_move_from, archive_file_path_destination)
            
            # print(f"    Archivado: {path_to_move_from} -> {archive_file_path_destination}") # Comentado para reducir verbosidad

        except Exception as e:
            path_info_for_error = f"URI original: {original_uri_for_log}"
            if entorno_actual == "local" and original_uri_for_log != path_to_move_from:
                path_info_for_error += f", Ruta OS intentada: {path_to_move_from}"
            print(f"    ERROR AL ARCHIVAR ({path_info_for_error}): {e}")