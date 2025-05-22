# Databricks notebook source
import os
import random
import uuid
import datetime # Asegúrate de que datetime esté importado
from datetime import timedelta
from pyspark.sql import SparkSession, DataFrame # DataFrame para type hinting
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


# Celda 1 del Notebook
storage_account_name = spark.conf.get("adls.account.name") # O tu nombre de cuenta fijo
landing_container_name = "landing" 
landing_path = f"abfss://{landing_container_name}@{storage_account_name}.dfs.core.windows.net"
print(f"INFO: Usando landing_path global: {landing_path}")

output_target_folder_in_landing = "sales_online" # Esta será la carpeta {datasource}/{dataset}

print(f"INFO: Los archivos CSV para evolución de esquema se generarán bajo: {landing_path}/{output_target_folder_in_landing}")


# --- 2. Datos de Muestra Base ---
PRODUCT_IDS = ['PROD001', 'PROD002', 'PROD003', 'PROD004', 'PROD005']
CUSTOMER_IDS = ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005']
PROMO_CODES = [None, "SAVE10", "NEW20", None, "SUMMERDEAL", "FREEBIE"]

# --- 3. Funciones Auxiliares ---
def _get_timestamp_filename_suffix_nodate(): # Solo H M S f
    return datetime.datetime.utcnow().strftime("%H%M%S%f")[:-3]

def _generate_base_sales_row_data_for_spark(include_promo_code: bool):
    order_datetime_obj = datetime.datetime.utcnow() - timedelta(seconds=random.randint(0, 3600*2))
    row = {
        "order_id": str(uuid.uuid4()), "product_id": random.choice(PRODUCT_IDS),
        "quantity": random.randint(1, 5), "price": round(random.uniform(10.0, 200.0), 2),
        "customer_id": random.choice(CUSTOMER_IDS), "order_date": order_datetime_obj
    }
    if include_promo_code:
        row["promo_code"] = random.choice(PROMO_CODES)
    return row

# --- 4. Función Principal de Generación de Archivo Único (usando Spark y lógica de rutas similar a tu ejemplo) ---
def generate_single_csv_file_via_spark(
    spark_session: SparkSession,
    dbutils_object, # Pasar dbutils
    base_landing_path: str, # ej. abfss://landing@<cuenta>.dfs.core.windows.net
    datasource_name: str, # ej. "farmia" o "ventas"
    dataset_name: str,    # ej. "sales_online" o "pedidos"
    file_name_prefix: str, # ej. "sales_orig_schema"
    schema_for_df: StructType, 
    num_rows: int, 
    include_promo_code: bool,
    output_format: str = "csv" # Formato de salida
):
    """
    Genera un DataFrame, lo guarda temporalmente y luego lo mueve a una ruta final estructurada
    con fecha (YYYY/MM/DD) y nombre de archivo con timestamp.
    """
    
    # 1. Crear DataFrame
    print(f"  Generando {num_rows} filas de datos para: {datasource_name}/{dataset_name} (promo: {include_promo_code})")
    data_to_write = [_generate_base_sales_row_data_for_spark(include_promo_code) for _ in range(num_rows)]
    try:
        df = spark_session.createDataFrame(data_to_write, schema=schema_for_df)
    except Exception as e_df:
        print(f"  ERROR creando DataFrame: {e_df}")
        return None

    # 2. Escribir a una ruta temporal (global bajo landing_path o específica del dataset)
    # Usaremos una ruta temporal específica del dataset para evitar colisiones y facilitar limpieza.
    dataset_base_path_in_landing = f"{base_landing_path.rstrip('/')}/{dataset_name}"
    tmp_uuid = str(uuid.uuid4().hex[:6])
    tmp_path_adls = f"{dataset_base_path_in_landing}/_tmp_spark_write_{file_name_prefix}_{tmp_uuid}/" 
    csv_writer_timestamp_format = "yyyy-MM-dd HH:mm:ss" 
    print(f"    Escribiendo DataFrame temporalmente a: {tmp_path_adls}")
    try:
        df.coalesce(1).write.format(output_format)\
            .option("timestampFormat", csv_writer_timestamp_format) \
            .option("header", "true").mode("overwrite").save(tmp_path_adls)
    except Exception as e_write:
        print(f"    ERROR al escribir temporalmente a {tmp_path_adls}: {e_write}")
        try: dbutils_object.fs.rm(tmp_path_adls, True)
        except: pass
        return None

    # 3. Preparar ruta final y nombre de archivo (SIN subcarpetas de fecha)
    now = datetime.datetime.utcnow()
    timestamp_for_filename = now.strftime("%Y%m%d%H%M%S%f")[:-3] # Sigue siendo útil para nombres de archivo únicos
    
    # Directorio de destino final para el archivo: landing_path/datasource_name/dataset_name/
    final_target_directory = dataset_base_path_in_landing # Ya no se añade /YYYY/MM/DD
    
    final_file_name_part = f"{file_name_prefix}_{timestamp_for_filename}.{output_format}"
    final_file_destination_path = f"{final_target_directory.rstrip('/')}/{final_file_name_part}"

    # Crear el directorio de destino final si no existe
    dbutils_object.fs.mkdirs(final_target_directory)

    # 4. Mover el archivo desde la carpeta temporal a la final
    moved_successfully = False
    try:
        files_in_tmp = dbutils_object.fs.ls(tmp_path_adls)
        part_file_to_move = None
        for file_info in files_in_tmp:
            if file_info.name.startswith("part-") and (output_format == "csv" or file_info.name.endswith(f".{output_format}")):
                part_file_to_move = file_info.path
                break 
        
        if part_file_to_move:
            print(f"    Moviendo archivo: {part_file_to_move} -> {final_file_destination_path}")
            dbutils_object.fs.mv(part_file_to_move, final_file_destination_path)
            print(f"  Archivo CSV guardado exitosamente en: {final_file_destination_path}")
            moved_successfully = True
        elif files_in_tmp: # Hubo archivos en tmp pero no el part-file esperado
            print(f"  ADVERTENCIA: No se encontró el archivo 'part-*.{output_format}' esperado en {tmp_path_adls} para mover. Contenido: {[f.name for f in files_in_tmp]}")
        else: # tmp_path_adls estaba vacío
             print(f"  ADVERTENCIA: El directorio temporal {tmp_path_adls} estaba vacío después de la escritura de Spark.")

    except Exception as e_mv:
        print(f"  ERROR al mover el archivo desde '{tmp_path_adls}'. Error: {e_mv}")
    finally:
        # 5. Limpiar la carpeta temporal
        print(f"    Limpiando directorio temporal: {tmp_path_adls}")
        try:
            dbutils_object.fs.rm(tmp_path_adls, True)
        except Exception as e_rm:
            print(f"    ADVERTENCIA: No se pudo eliminar el directorio temporal '{tmp_path_adls}'. Error: {e_rm}")

    return final_file_destination_path if moved_successfully else None

# COMMAND ----------

 # Esquema original
schema_original = StructType([
    StructField("order_id", StringType(), True), StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True), StructField("price", DoubleType(), True),
    StructField("customer_id", StringType(), True), StructField("order_date", TimestampType(), True)
])

# Esquema evolucionado
schema_evolved = StructType(schema_original.fields + [StructField("promo_code", StringType(), True)])
    

# COMMAND ----------

#Para generar archivos de sales_online con el esquema original
generate_single_csv_file_via_spark(
    spark_session=spark,
    dbutils_object=dbutils,
    base_landing_path=landing_path, # La raíz donde se creará la carpeta "sales_online"
    file_name_prefix="sales_online",
    schema_for_df=schema_original,
    output_format="csv",
    datasource_name="sales_online",
    dataset_name="sales_online",
    num_rows=random.randint(5, 10),
    include_promo_code=False
)
dbutils.notebook.exit("Generación de CSVs para evolución de esquema con Spark completada.")

# COMMAND ----------

#Para generar archivos de sales_online con el esquema modificar con columna extra cupon
generate_single_csv_file_via_spark(
    spark_session=spark,
    dbutils_object=dbutils,
    base_landing_path=landing_path, # La raíz donde se creará la carpeta "sales_online"
    file_name_prefix="sales_online",
    schema_for_df=schema_evolved,
    output_format="csv",
    datasource_name="sales_online",
    dataset_name="sales_online",
    num_rows=random.randint(5, 10),
    include_promo_code=True
)
dbutils.notebook.exit("Generación de CSVs para evolución de esquema con Spark completada.")