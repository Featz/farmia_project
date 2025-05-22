# Databricks notebook source
#Configuraciones
# Importar la función orquestadora desde el paquete instalado
from farmia_engine.main_orchestrators import execute_landing_to_raw, execute_raw_to_bronze, utils
import traceback # Importar para un mejor log de errores
from pyspark.sql.functions import col

# Definir el entorno y el dataset que queremos consultar
entorno_actual = "databricks" # Estamos en un notebook de Databricks
dataset_name_key_to_query = "sales_online_csv" # El dataset que vamos a validar

# Obtener la SparkSession (utils.py la configurará o reutilizará la existente)
spark = utils.get_spark_session(env_type=entorno_actual, app_name=f"QueryLayers_{dataset_name_key_to_query}")
# dbutils está disponible globalmente en notebooks Python de Databricks, pero get_dbutils() lo recupera de forma segura.
dbutils_obj = utils.get_dbutils() 

paths_for_listing = {} # Diccionario para guardar las rutas que necesitamos

try:
    print(f"INFO: Cargando configuración para el entorno: {entorno_actual}")
    # utils.load_app_config usará la ruta DATABRICKS_DBFS_CONFIG_PATH por defecto para Databricks
    # (ej. /dbfs/FileStore/configs/farmia_ingest_config.json)
    # Si tu config.json está en otra ruta en DBFS, pásala como config_path_override
    # ej. config_general = utils.load_app_config(entorno_actual, config_path_override="/dbfs/mi/otra/ruta/config.json")
    config_general = utils.load_app_config(entorno_actual)

    dataset_cfg = config_general.get("dataset_configs", {}).get(dataset_name_key_to_query)
    if not dataset_cfg:
        raise ValueError(f"Configuración para el dataset '{dataset_name_key_to_query}' no encontrada en config.json.")

    databricks_env_cfg_base = config_general.get("databricks_env_config")
    adls_base_cfg = config_general.get("adls_config_base")

    if not databricks_env_cfg_base or not adls_base_cfg:
        raise ValueError("Configuraciones 'databricks_env_config' o 'adls_config_base' no encontradas en config.json.")

    # Construir todas las rutas para el dataset especificado
    all_paths = utils.construct_full_paths_for_dataset(
        databricks_env_cfg_base, 
        adls_base_cfg, 
        dataset_cfg, 
        dataset_name_key_to_query, 
        entorno_actual
    )

    # Extraer las rutas específicas que necesitamos para listar
    paths_for_listing["landing"] = all_paths.get("source_path")
    paths_for_listing["archive"] = all_paths.get("archive_path")
    paths_for_listing["raw"] = all_paths.get("raw_target_path")
    paths_for_listing["bronze_delta_path"] = all_paths.get("bronze_target_path")
    
    # Nombre de la tabla bronze para consultas SQL
    bronze_cfg = dataset_cfg.get("bronze_config", {})
    bronze_db_name = bronze_cfg.get("bronze_database_name", "default")
    bronze_table_name_only = bronze_cfg.get("bronze_table_name", dataset_name_key_to_query.replace("/","_") + "_bronze")
    paths_for_listing["bronze_table_fullname"] = f"{bronze_db_name}.{bronze_table_name_only}"


    print("\nINFO: Rutas obtenidas de la configuración:")
    for key, path_val in paths_for_listing.items():
        if path_val: # Solo imprimir si la ruta fue construida
             print(f"  {key.replace('_', ' ').capitalize()}: {path_val}")
        else:
             print(f"  {key.replace('_', ' ').capitalize()}: No configurada o no aplicable.")
    
    # Validar que las rutas esenciales para las siguientes celdas estén definidas
    if not paths_for_listing.get("landing") or \
       not paths_for_listing.get("archive") or \
       not paths_for_listing.get("raw") or \
       not paths_for_listing.get("bronze_delta_path"):
        dbutils.notebook.exit("ERROR: Una o más rutas esenciales no pudieron ser construidas desde la configuración.")

except Exception as e:
    print(f"ERROR FATAL al cargar la configuración o definir rutas: {e}")
    import traceback
    traceback.print_exc()
    dbutils.notebook.exit(f"ERROR FATAL: {e}")



# COMMAND ----------

# CELDA PARA LISTAR LANDING (FUENTE ORIGINAL)

landing_path_to_list = paths_for_listing.get("landing")

if landing_path_to_list:
    print(f"--- Contenido de la Capa LANDING (Fuente Original) ---")
    print(f"Ruta a listar: {landing_path_to_list}")
    print("NOTA: Después del procesamiento y archivado, esta carpeta debería estar vacía o solo con archivos nuevos.")
    try:
        files = dbutils.fs.ls(landing_path_to_list)
        if not files:
            print("El directorio de landing está vacío o no existe (esperado si los archivos fueron archivados).")
        for f in files:
            print(f.path)
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            print(f"El directorio de landing no existe o está vacío: {landing_path_to_list}")
        else:
            print(f"Error listando el directorio de landing: {e}")
else:
    print("ERROR: La ruta de Landing no está definida para listar.")

# COMMAND ----------

# Landing -> Raw

print("INFO: Iniciando pipeline Landing -> Raw en Databricks...")
final_status_message = ""
success = False

try:
    execute_landing_to_raw(config_path_override=config_dbfs_path)
    print("INFO: Pipeline Landing -> Raw completado exitosamente.")
    final_status_message = "SUCCESS - Landing to Raw"
    success = True
except Exception as e:
    print(f"ERROR durante el pipeline Landing -> Raw: {str(e)}")
    # Obtener el traceback completo para un diagnóstico más detallado
    detailed_error = traceback.format_exc()
    print(detailed_error)
    final_status_message = f"FAILURE - Landing to Raw: {str(e)}\n\nTraceback:\n{detailed_error}"
    success = False
finally:
    if not final_status_message: # Fallback por si algo muy extraño ocurre
        final_status_message = "FAILURE - Landing to Raw: Estado final desconocido o ejecución interrumpida."
        success = False
    
    print(f"INFO: Mensaje de salida final del notebook: {final_status_message}")
    if success:
        dbutils.notebook.exit(final_status_message)
    else:
        # Cuando se usa dbutils.notebook.exit con un string que NO comienza con "SUCCESS", 
        # el job se marca como fallido y el string es el mensaje de error.
        dbutils.notebook.exit(final_status_message)

# COMMAND ----------

# CELDA PARA LISTAR ARCHIVE (LANDING PROCESADO)

archive_path_to_list = paths_for_listing.get("archive")

if archive_path_to_list:
    print(f"\n--- Contenido de la Capa ARCHIVE (Landing Procesado) ---")
    print(f"Ruta a listar: {archive_path_to_list}")
    print("NOTA: Aquí deberían estar los archivos CSV originales después de ser procesados por landing-to-raw.")
    try:
        files = dbutils.fs.ls(archive_path_to_list)
        if not files:
            print("El directorio de archivo está vacío o no existe.")
        for f in files:
            print(f.path)
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            print(f"El directorio de archivo no existe: {archive_path_to_list}")
        else:
            print(f"Error listando el directorio de archivo: {e}")
else:
    print("ERROR: La ruta de Archive no está definida para listar.")

# COMMAND ----------

# CELDA PARA LISTAR RAW (PARQUET)

raw_path_to_list = paths_for_listing.get("raw")

def list_files_recursively_generic(path_to_list, path_label="RAW"):
    print(f"Listando recursivamente ({path_label}): {path_to_list}")
    try:
        for i in dbutils.fs.ls(path_to_list):
            print(i.path)
            # Evitar recursar en _delta_log u otras carpetas de metadatos si es necesario
            if i.isDir() and not i.name.startswith("_") and not i.name.startswith("."): 
                list_files_recursively_generic(i.path, path_label) # Llamada recursiva
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
             print(f"  El subdirectorio o archivo no existe: {path_to_list}")
        else:
            print(f"  Error listando {path_to_list}: {e}")


if raw_path_to_list:
    print(f"\n--- Contenido de la Capa RAW (Parquet Particionado) ---")
    print(f"Ruta a listar: {raw_path_to_list}")
    print("NOTA: Esperar estructura de directorios Parquet, posiblemente particionada (ej. year=...).")
    try:
        top_level_files = dbutils.fs.ls(raw_path_to_list)
        if not top_level_files:
            print("El directorio raw está vacío o no existe.")
        else:
            list_files_recursively_generic(raw_path_to_list, "RAW")
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            print(f"El directorio raw no existe: {raw_path_to_list}")
        else:
            print(f"Error listando el directorio raw: {e}")
else:
    print("ERROR: La ruta Raw no está definida para listar.")

# COMMAND ----------

# CELDA DE VALIDACIÓN - CAPA RAW (sales_online_raw_parquet)

print(f"--- Validando Capa RAW para 'sales_online_csv' ---")
print(f"Leyendo datos Parquet desde: {raw_sales_online_path}")

try:
    df_raw_sales = spark.read.parquet(raw_sales_online_path)
    
    print("\nEsquema del DataFrame RAW:")
    df_raw_sales.printSchema()
    
    print(f"\nNúmero total de registros en RAW para esta carga/dataset: {df_raw_sales.count()}")
    
    print("\nMostrando algunas filas de la capa RAW (incluyendo metadatos y columnas de partición si existen aquí):")
    # Selecciona columnas clave, incluyendo las que esperas de la evolución (promo_code)
    # y las columnas de partición de raw (year, month, day) y las de metadatos de ingesta.
    columnas_a_mostrar_raw = []
    if "order_id" in df_raw_sales.columns: columnas_a_mostrar_raw.append("order_id")
    if "order_date" in df_raw_sales.columns: columnas_a_mostrar_raw.append("order_date")
    if "promo_code" in df_raw_sales.columns: columnas_a_mostrar_raw.append("promo_code") # Para verificar evolución
    if "year" in df_raw_sales.columns: columnas_a_mostrar_raw.append("year") # Columna de partición de raw
    if "month" in df_raw_sales.columns: columnas_a_mostrar_raw.append("month")# Columna de partición de raw
    if "day" in df_raw_sales.columns: columnas_a_mostrar_raw.append("day") # Columna de partición de raw
    if "ingestion_date" in df_raw_sales.columns: columnas_a_mostrar_raw.append("ingestion_date")
    if "source_filename" in df_raw_sales.columns: columnas_a_mostrar_raw.append("source_filename")
        
    if columnas_a_mostrar_raw:
        df_raw_sales.select(*columnas_a_mostrar_raw).show(10, truncate=False)
    else:
        print("No se encontraron columnas esperadas para mostrar, mostrando todas:")
        df_raw_sales.show(10, truncate=False)

    # Verificar si la columna 'promo_code' existe y cuántos registros la tienen no nula
    if "promo_code" in df_raw_sales.columns:
        promo_code_not_null_count = df_raw_sales.filter(col("promo_code").isNotNull()).count()
        print(f"\nNúmero de registros en RAW con 'promo_code' no nulo: {promo_code_not_null_count}")
    else:
        print("\nLa columna 'promo_code' no existe en el DataFrame RAW leído.")

except Exception as e:
    print(f"Error al leer o procesar datos de la capa RAW: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------


#Para comenzar a realizar pruebas ideal borrar la posible tabla. Ejecutar solo la primera vez.
%sql
DROP TABLE IF EXISTS farmia_bronze.sales_online_orders;

# COMMAND ----------


# Raw -> Bronze

print("INFO: Iniciando pipeline Raw -> Bronze en Databricks...")
final_status_message = ""
success = False

try:
    execute_raw_to_bronze(config_path_override=config_dbfs_path)
    print("INFO: Pipeline Raw -> Bronze completado exitosamente.")
    final_status_message = "SUCCESS - Landing to Raw"
    success = True
except Exception as e:
    print(f"ERROR durante el pipeline Landing -> Raw: {str(e)}")
    # Obtener el traceback completo para un diagnóstico más detallado
    detailed_error = traceback.format_exc()
    print(detailed_error)
    final_status_message = f"FAILURE - Raw to Bronze: {str(e)}\n\nTraceback:\n{detailed_error}"
    success = False
finally:
    if not final_status_message: # Fallback por si algo muy extraño ocurre
        final_status_message = "FAILURE - Raw to Bronze: Estado final desconocido o ejecución interrumpida."
        success = False
    
    print(f"INFO: Mensaje de salida final del notebook: {final_status_message}")
    if success:
        dbutils.notebook.exit(final_status_message)
    else:
        # Cuando se usa dbutils.notebook.exit con un string que NO comienza con "SUCCESS", 
        # el job se marca como fallido y el string es el mensaje de error.
        dbutils.notebook.exit(final_status_message)

# COMMAND ----------

# CELDA PARA LISTAR Y CONSULTAR BRONZE (DELTA LAKE)

bronze_path_to_list = paths_for_listing.get("bronze_delta_path")
bronze_table_to_query = paths_for_listing.get("bronze_table_fullname")

# Reutilizar la función de listado recursivo de la celda anterior
# def list_files_recursively_generic(path_to_list, path_label="BRONZE"): ... (ya definida)

if bronze_path_to_list:
    print(f"\n--- Contenido de Archivos de la Capa BRONZE (Tabla Delta) ---")
    print(f"Ruta física a listar: {bronze_path_to_list}")
    print("NOTA: Esperar estructura de tabla Delta con _delta_log y archivos de datos Parquet, particionada.")
    try:
        top_level_files_bronze = dbutils.fs.ls(bronze_path_to_list)
        if not top_level_files_bronze:
            print("El directorio de la tabla bronze está vacío o no existe.")
        else:
            list_files_recursively_generic(bronze_path_to_list, "BRONZE")
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            print(f"El directorio de la tabla bronze no existe: {bronze_path_to_list}")
        else:
            print(f"Error listando el directorio de la tabla bronze: {e}")
else:
    print("ERROR: La ruta física de Bronze (Delta Path) no está definida para listar.")


if bronze_table_to_query:
    print(f"\n--- Consultando la Tabla Delta '{bronze_table_to_query}' ---")
    try:
        # Asegurar que la base de datos exista antes de intentar consultar la tabla
        db_name_only = bronze_table_to_query.split('.')[0] if '.' in bronze_table_to_query else "default"
        if db_name_only != "default": # "default" siempre existe
             spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name_only}")
        
        print(f"Verificando si la tabla {bronze_table_to_query} existe...")
        table_exists = spark._jsparkSession.catalog().tableExists(bronze_table_to_query) # Para nombres de tabla completos

        if table_exists:
            print(f"Tabla {bronze_table_to_query} existe. Mostrando esquema y datos de ejemplo:")
            df_bronze = spark.table(bronze_table_to_query)
            
            print("\nEsquema de la tabla BRONZE:")
            df_bronze.printSchema()
            
            print(f"\nNúmero total de registros en BRONZE: {df_bronze.count()}")
            
            print("\nMostrando algunas filas de la capa BRONZE:")
            columnas_a_mostrar = ["order_id", "order_date", "promo_code", "event_year", "event_month"] # Ejemplo
            cols_existentes = [c for c in columnas_a_mostrar if c in df_bronze.columns]
            if cols_existentes:
                df_bronze.select(*cols_existentes).show(10, truncate=False)
            else:
                df_bronze.show(10, truncate=False)
            
            # Historial de la tabla Delta
            print(f"\n--- Historial de la Tabla Delta '{bronze_table_to_query}' ---")
            display(spark.sql(f"DESCRIBE HISTORY {bronze_table_to_query}"))

        else:
            print(f"ERROR: La tabla Delta '{bronze_table_to_query}' no existe en el metastore.")
            print(f"       Verifica si el proceso 'main_promote_raw_to_bronze.py' se ejecutó correctamente y registró la tabla.")

    except Exception as e:
        print(f"Error al consultar la tabla Delta '{bronze_table_to_query}': {e}")
        traceback.print_exc()
else:
    print("ERROR: El nombre completo de la tabla Bronze no está definido para consulta.")