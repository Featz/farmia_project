# raw_to_bronze_processors.py
from pyspark.sql.functions import col, year, month, current_timestamp, input_file_name 
from pyspark.sql.types import TimestampType, DateType 
import utils 

class RawDataReader:
    def __init__(self, spark, raw_source_path, dataset_cfg, entorno_actual, autoloader_raw_paths=None):
        self.spark = spark
        self.raw_source_path = raw_source_path
        self.dataset_cfg = dataset_cfg
        self.entorno_actual = entorno_actual
        self.autoloader_raw_paths = autoloader_raw_paths
        self.is_streaming = False

    def read(self):
        bronze_cfg = self.dataset_cfg["bronze_config"] # Asumimos que bronze_config existe
        raw_format = bronze_cfg.get("raw_source_format", "parquet")
        reader_options = bronze_cfg.get("raw_reader_options", {})
        use_autoloader_for_raw = bronze_cfg.get("autoloader_for_raw_source", False)

        df = None
        if self.entorno_actual == "databricks" and use_autoloader_for_raw:
            print(f"  BRONZE READER: Usando Autoloader para leer '{raw_format}' de capa RAW: {self.raw_source_path}")
            self.is_streaming = True
            if not self.autoloader_raw_paths or \
               not self.autoloader_raw_paths.get("autoloader_raw_schema_location") or \
               not self.autoloader_raw_paths.get("autoloader_raw_checkpoint_location"):
                raise ValueError("Rutas de metadatos de Autoloader (schema y checkpoint) para fuente RAW no proporcionadas o incompletas.")

            autoloader_opts = {
                "cloudFiles.format": raw_format,
                "cloudFiles.schemaLocation": self.autoloader_raw_paths["autoloader_raw_schema_location"],
                # Para Parquet en RAW, el esquema debería ser estable. "none" es estricto.
                # "addNewColumns" fue solicitado, pero "none" o "rescue" son más comunes para Parquet.
                "cloudFiles.schemaEvolutionMode": bronze_cfg.get("autoloader_raw_schema_evolution_mode", "addNewColumns"),
            }
            autoloader_opts.update(reader_options)
            df = self.spark.readStream.format("cloudFiles").options(**autoloader_opts).load(self.raw_source_path)
        
        else: # Lectura Batch
            print(f"  BRONZE READER: Usando lector Spark batch para '{raw_format}' de capa RAW: {self.raw_source_path}")
            self.is_streaming = False
            if raw_format.lower() == "parquet":
                df = self.spark.read.format("parquet").options(**reader_options).load(self.raw_source_path)
            elif raw_format.lower() == "delta": # En caso de que la capa raw ya sea Delta
                 df = self.spark.read.format("delta").options(**reader_options).load(self.raw_source_path)
            else:
                raise ValueError(f"Formato '{raw_format}' no soportado para lectura batch de capa RAW en este procesador.")
        
        if df is None: # Chequeo por si la carga falla o devuelve None (aunque Spark suele devolver DF vacío)
             raise ValueError(f"No se pudieron leer datos de {self.raw_source_path} con formato {raw_format}")
        return df, self.is_streaming


class BronzeDeltaWriter:
    def __init__(self, spark, bronze_target_path, bronze_table_full_name, dataset_cfg, entorno_actual, 
                 autoloader_checkpoint_location=None):
        self.spark = spark
        self.bronze_target_path = bronze_target_path
        self.bronze_table_full_name = bronze_table_full_name
        self.dataset_cfg = dataset_cfg 
        self.entorno_actual = entorno_actual
        self.bronze_stream_checkpoint_location = autoloader_checkpoint_location 

    def _derive_bronze_partition_columns(self, df_input, bronze_partition_derivation_cfg):
        df_with_derived_cols = df_input 

        if not bronze_partition_derivation_cfg or not isinstance(bronze_partition_derivation_cfg, dict):
            print("    INFO (DERIVACIÓN BRONZE): Config 'bronze_partition_columns_derivation' no válida o no proporcionada. No se derivarán columnas.")
            return df_with_derived_cols

        source_col_name = bronze_partition_derivation_cfg.get("source_column")
        year_col_name_target = bronze_partition_derivation_cfg.get("year_column_name")
        month_col_name_target = bronze_partition_derivation_cfg.get("month_column_name")
        
        if isinstance(source_col_name, str) and source_col_name and source_col_name in df_input.columns:
            current_df_state = df_with_derived_cols 
            
            source_col_type = df_input.schema[source_col_name].dataType
            if not isinstance(source_col_type, (TimestampType, DateType)):
                print(f"    INFO (DERIVACIÓN BRONZE): Convirtiendo columna '{source_col_name}' de {source_col_type} a TimestampType.")
                current_df_state = current_df_state.withColumn(source_col_name, col(source_col_name).cast(TimestampType()))
            
            if isinstance(year_col_name_target, str) and year_col_name_target:
                print(f"    INFO (DERIVACIÓN BRONZE): Creando columna '{year_col_name_target}' desde '{source_col_name}'.")
                current_df_state = current_df_state.withColumn(year_col_name_target, year(col(source_col_name)))
            
            if isinstance(month_col_name_target, str) and month_col_name_target:
                print(f"    INFO (DERIVACIÓN BRONZE): Creando columna '{month_col_name_target}' desde '{source_col_name}'.")
                current_df_state = current_df_state.withColumn(month_col_name_target, month(col(source_col_name)))
            
            df_with_derived_cols = current_df_state 
        elif source_col_name:
             print(f"ADVERTENCIA (DERIVACIÓN BRONZE): Columna fuente '{source_col_name}' no es string válido o no fue encontrada en DataFrame. No se derivarán columnas. Columnas DF: {df_input.columns}")
        else:
            print(f"    INFO (DERIVACIÓN BRONZE): 'source_column' no especificada en 'bronze_partition_columns_derivation'. No se derivarán columnas.")
        
        return df_with_derived_cols

    def _write_micro_batch_to_delta(self, batch_df, epoch_id):
        bronze_cfg = self.dataset_cfg["bronze_config"]
        log_dataset_identifier = self.dataset_cfg.get('source_subpath', f'epoch_{epoch_id}')
        print(f"  BRONZE WRITER (foreachBatch - {log_dataset_identifier}): Procesando epoch_id {epoch_id} para Delta.")

        df_to_write = self._derive_bronze_partition_columns(batch_df, bronze_cfg.get("bronze_partition_columns_derivation"))
        partition_cols_bronze = bronze_cfg.get("partition_by_bronze") # Lista de nombres de columnas finales

        writer = df_to_write.write.format("delta").mode("append")
        if bronze_cfg.get("merge_schema_on_write", True): writer = writer.option("mergeSchema", "true")
        if bronze_cfg.get("optimize_write", False) and self.entorno_actual == "databricks": writer = writer.option("autoOptimize.optimizeWrite", "true")
        
        if partition_cols_bronze:
            for p_col in partition_cols_bronze:
                if p_col not in df_to_write.columns:
                    raise ValueError(f"Columna de partición Bronze '{p_col}' no encontrada en DataFrame (foreachBatch) TRAS DERIVACIÓN. Columnas: {df_to_write.columns}")
            writer = writer.partitionBy(*partition_cols_bronze)
        
        if self.entorno_actual == "databricks" and self.bronze_table_full_name:
            # print(f"    Escribiendo micro-lote Delta en: {self.bronze_target_path} y tabla {self.bronze_table_full_name}")
            writer.option("path", self.bronze_target_path).saveAsTable(self.bronze_table_full_name)
        else:
            # print(f"    Escribiendo micro-lote Delta en: {self.bronze_target_path}")
            writer.save(self.bronze_target_path)

    def write(self, df_input, is_streaming_source):
        bronze_cfg = self.dataset_cfg["bronze_config"]
        db_name = bronze_cfg.get("bronze_database_name", "default")
        log_table_identifier = self.bronze_table_full_name or self.bronze_target_path
        print(f"  BRONZE WRITER: Iniciando escritura Delta para '{log_table_identifier}' a '{self.bronze_target_path}'.")

        if self.entorno_actual == "databricks": self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        if is_streaming_source:
            if not self.bronze_stream_checkpoint_location:
                raise ValueError("Checkpoint location para stream raw->bronze no proporcionado.")
            
            # La derivación de columnas de partición se aplica dentro de _write_micro_batch_to_delta
            # usando el batch_df, por lo que df_input (el stream) se pasa directamente.
            df_stream_to_write = df_input 

            query_name = f"promote_{self.dataset_cfg.get('source_subpath','unnamed_dataset').replace('/','_')}_to_bronze"
            query = df_stream_to_write.writeStream \
                .foreachBatch(self._write_micro_batch_to_delta) \
                .outputMode("update") \
                .option("checkpointLocation", self.bronze_stream_checkpoint_location) \
                .trigger(availableNow=True) \
                .queryName(query_name) \
                .start()
            print(f"    Stream raw->bronze iniciado ({query_name}). Esperando finalización...")
            query.awaitTermination()
        
        else: # Escritura Batch a Delta
            df_final_derived = self._derive_bronze_partition_columns(df_input, bronze_cfg.get("bronze_partition_columns_derivation"))
            partition_cols_bronze = bronze_cfg.get("partition_by_bronze")
            
            writer = df_final_derived.write.format("delta").mode("append") # Mantener append como default para batch local (según tu última decisión)
            if bronze_cfg.get("merge_schema_on_write", True): writer = writer.option("mergeSchema", "true")
            if bronze_cfg.get("optimize_write", False) and self.entorno_actual == "databricks": writer = writer.option("autoOptimize.optimizeWrite", "true")
            
            if partition_cols_bronze:
                for p_col in partition_cols_bronze:
                    if p_col not in df_final_derived.columns:
                        raise ValueError(f"Columna de partición Bronze '{p_col}' no encontrada en DataFrame (batch) TRAS DERIVACIÓN. Columnas: {df_final_derived.columns}")
                # print(f"    Particionando batch Delta por: {partition_cols_bronze}") # Reducir verbosidad
                writer = writer.partitionBy(*partition_cols_bronze)
            
            if self.entorno_actual == "databricks" and self.bronze_table_full_name:
                # print(f"    Escribiendo batch Delta en: {self.bronze_target_path} y tabla {self.bronze_table_full_name}")
                writer.option("path", self.bronze_target_path).saveAsTable(self.bronze_table_full_name)
            else:
                # print(f"    Escribiendo batch Delta en: {self.bronze_target_path}")
                writer.save(self.bronze_target_path)

        print(f"BRONZE WRITER: Escritura Delta completada para '{log_table_identifier}'.")

def process_dataset_raw_to_bronze(spark, raw_source_path, bronze_target_path, bronze_table_full_name, dataset_cfg, entorno_actual, autoloader_raw_paths=None):
    """Función orquestadora para promover un dataset de Raw (Parquet) a Bronze (Delta)."""
    
    reader = RawDataReader(spark, raw_source_path, dataset_cfg, entorno_actual, autoloader_raw_paths)
    df_from_raw, is_streaming = reader.read()

    if df_from_raw is None and not is_streaming:
        print(f"  RAW TO BRONZE: No se leyeron datos de {raw_source_path} para {dataset_cfg.get('source_subpath')}. No se escribe a Bronze.")
        return

    autoloader_checkpoint = autoloader_raw_paths.get("autoloader_raw_checkpoint_location") if autoloader_raw_paths else None
    
    bronze_writer = BronzeDeltaWriter(spark, bronze_target_path, bronze_table_full_name, dataset_cfg, entorno_actual, 
                                      autoloader_checkpoint_location=autoloader_checkpoint)
    bronze_writer.write(df_from_raw, is_streaming)