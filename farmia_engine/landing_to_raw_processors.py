# landing_to_raw_processors.py
from pyspark.sql.functions import current_timestamp, input_file_name
import utils 

class DatasetReader:
    def __init__(self, spark, source_path, dataset_cfg, entorno_actual, autoloader_specific_paths=None):
        self.spark = spark
        self.source_path = source_path
        self.dataset_cfg = dataset_cfg
        self.entorno_actual = entorno_actual
        self.autoloader_specific_paths = autoloader_specific_paths
        self.is_streaming = False

    def _add_metadata_columns(self, df):
        return df.withColumn("ingestion_date", current_timestamp()) \
                 .withColumn("source_filename", input_file_name())

    def read(self):
        file_type = self.dataset_cfg["type"].lower()
        schema_config = self.dataset_cfg.get("schema_config") # Usar get por si no todos los tipos lo tienen
        reader_options = self.dataset_cfg.get("reader_options", {})

        spark_schema = None # Default a None, para que Autoloader/lector infiera si no hay schema_config
        if schema_config:
            spark_schema = utils.build_spark_schema_from_config(schema_config)
        
        # timestampFormat para CSV
        if file_type == "csv":
            # Usar source_subpath para un log más general si partition_config no está.
            log_id_for_ts = self.dataset_cfg.get("source_subpath", "csv_dataset")
            # partition_config puede no existir
            partition_cfg = self.dataset_cfg.get("partition_config", {}) 
            timestamp_field_name = partition_cfg.get("derive_date_parts_from_column", "order_date")
            
            # schema_config puede ser None si build_spark_schema_from_config devolvió None
            fields_in_schema = schema_config.get("fields", []) if schema_config else []
            csv_ts_format = next((f.get("format") for f in fields_in_schema if f.get("name") == timestamp_field_name and f.get("type")=="timestamp"), "yyyy-MM-dd HH:mm:ss")
            
            if "timestampFormat" not in reader_options: 
                # print(f"  READER ({log_id_for_ts}): Usando timestampFormat '{csv_ts_format}' para CSV.")
                reader_options["timestampFormat"] = csv_ts_format

        df = None
        if self.entorno_actual == "databricks" and file_type in ["csv", "json", "parquet", "avro", "text", "binaryFile", "image"]: # Tipos soportados por Autoloader
            print(f"  READER: Usando Databricks Autoloader para formato '{file_type}'.")
            self.is_streaming = True
            if not self.autoloader_specific_paths or \
               not self.autoloader_specific_paths.get("autoloader_schema_location") or \
               not self.autoloader_specific_paths.get("autoloader_checkpoint_location"): # Validar ambas claves
                raise ValueError("Rutas de Autoloader (schema y checkpoint) no proporcionadas o incompletas para Databricks.")

            autoloader_opts = {
                "cloudFiles.format": file_type,
                "cloudFiles.schemaLocation": self.autoloader_specific_paths["autoloader_schema_location"],
                "cloudFiles.schemaEvolutionMode": self.dataset_cfg.get("autoloader_schema_evolution_mode", "addNewColumns"),
            }
            if self.dataset_cfg.get("autoloader_infer_column_types"): # Nueva opción en config.json si se desea
                 autoloader_opts["cloudFiles.inferColumnTypes"] = "true"

            autoloader_opts.update(reader_options)
            
            if file_type == "csv" and "delimiter" in autoloader_opts and "sep" not in autoloader_opts:
                autoloader_opts["sep"] = autoloader_opts.pop("delimiter")
            
            df_reader = self.spark.readStream.format("cloudFiles").options(**autoloader_opts)
            if spark_schema: # Solo aplicar si spark_schema no es None
                df_reader = df_reader.schema(spark_schema)
            df = df_reader.load(self.source_path)
        
        else: # Lectura Batch local (o para formatos no Autoloader en Databricks, o si Autoloader no está habilitado)
            print(f"  READER: Usando lector Spark batch para formato '{file_type}' en entorno '{self.entorno_actual}'.")
            self.is_streaming = False
            
            local_reader_options = reader_options.copy()
            if file_type == "csv" and "delimiter" in local_reader_options:
                local_reader_options["sep"] = local_reader_options.pop("delimiter")
            
            # Usar .format(file_type).options(...).load(...) para consistencia
            df_reader_batch = self.spark.read.format(file_type)
            if spark_schema: # Solo aplicar si spark_schema no es None
                df_reader_batch = df_reader_batch.schema(spark_schema)
            
            # Pasar opciones directamente a .load() o .options() antes de .load()
            # spark.read.csv tiene kwargs específicos, otros formatos usan .option()
            if file_type == "csv":
                # header es un kwarg de .csv(), otras opciones de reader_options deben ser compatibles
                # Separar header de otras opciones si es necesario.
                header_opt = local_reader_options.pop("header", "true") # Default a true, sacar de local_reader_options
                sep_opt = local_reader_options.pop("sep", ",")
                ts_format_opt = local_reader_options.pop("timestampFormat", None)

                df = self.spark.read.csv(self.source_path, 
                                         schema=spark_schema, 
                                         header=header_opt, 
                                         sep=sep_opt, 
                                         timestampFormat=ts_format_opt, 
                                         **local_reader_options) # Resto de opciones
            elif file_type == "json":
                 df = df_reader_batch.options(**local_reader_options).load(self.source_path)
            elif file_type == "parquet":
                 df = df_reader_batch.options(**local_reader_options).load(self.source_path) # schema no se pasa a Parquet usualmente
            elif file_type == "avro": # Requiere paquete spark-avro
                 df = df_reader_batch.options(**local_reader_options).load(self.source_path)
            elif file_type == "image" or file_type == "binaryFile": # Spark native formats
                 df = df_reader_batch.options(**local_reader_options).load(self.source_path)
            else:
                raise ValueError(f"Tipo de archivo '{file_type}' no soportado para lectura batch simple en este procesador.")

        if df is None:
            raise Exception(f"No se pudo leer el DataFrame para {self.source_path} con formato {file_type}")

        return self._add_metadata_columns(df), self.is_streaming


class RawLayerWriter:
    def __init__(self, spark, target_path, archive_path, dataset_cfg, entorno_actual, autoloader_checkpoint_location=None):
        self.spark = spark
        self.target_path = target_path
        self.archive_path = archive_path
        self.dataset_cfg = dataset_cfg
        self.entorno_actual = entorno_actual
        self.autoloader_checkpoint_location = autoloader_checkpoint_location # Para el stream writer

    def _write_micro_batch_and_archive_parquet(self, batch_df, epoch_id):
        dataset_name_log = self.dataset_cfg.get('source_subpath', f'epoch_{epoch_id}')
        # print(f"  WRITER (foreachBatch L->R - {dataset_name_log}): Procesando epoch_id {epoch_id}.") # Reducir verbosidad
        
        df_to_write, partition_cols = utils.prepare_dataframe_for_partitioning(batch_df, self.dataset_cfg.get("partition_config"))

        writer = df_to_write.write.mode("append").format("parquet")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(self.target_path)
        # print(f"    Micro-lote Parquet escrito en: {self.target_path}") # Reducir verbosidad

        if "source_filename" in batch_df.columns:
            utils.archive_processed_files(self.spark, batch_df.select("source_filename").distinct(), self.archive_path, self.entorno_actual)
        # else: # Reducir verbosidad
            # print("    ARCHIVADO (foreachBatch L->R): Columna 'source_filename' no encontrada.")


    def write_and_archive(self, df_input, is_streaming):
        print(f"  WRITER (L->R): Iniciando escritura para '{self.dataset_cfg.get('source_subpath', 'N/A')}' a '{self.target_path}'.")

        if is_streaming: 
            if not self.autoloader_checkpoint_location:
                raise ValueError("Checkpoint location de Autoloader no proporcionado para escritura de stream (L->R).")
            
            # Aplicar preparación para particionamiento a la definición del stream
            # para que batch_df dentro de foreachBatch ya tenga las columnas derivadas.
            df_stream_prepared, _ = utils.prepare_dataframe_for_partitioning(df_input, self.dataset_cfg.get("partition_config"))
            
            query_name = f"ingest_landing_to_raw_{self.dataset_cfg.get('source_subpath','unnamed_dataset').replace('/','_')}"
            query = df_stream_prepared.writeStream \
                .foreachBatch(self._write_micro_batch_and_archive_parquet) \
                .outputMode("update") \
                .option("checkpointLocation", self.autoloader_checkpoint_location) \
                .trigger(availableNow=True) \
                .queryName(query_name) \
                .start()
            
            print(f"    Stream Landing->Raw iniciado ({query_name}). Esperando finalización...")
            query.awaitTermination()
        
        else: # Escritura Batch local
            df_final, partition_cols = utils.prepare_dataframe_for_partitioning(df_input, self.dataset_cfg.get("partition_config"))
            
            writer = df_final.write.mode("overwrite").format("parquet")
            if partition_cols:
                # print(f"    Particionando batch (L->R) por: {partition_cols}") # Reducir verbosidad
                writer = writer.partitionBy(*partition_cols)
            writer.save(self.target_path)
            print(f"    Datos batch (L->R) escritos en: {self.target_path}")

            if "source_filename" in df_final.columns:
                utils.archive_processed_files(self.spark, df_final.select("source_filename").distinct(), self.archive_path, self.entorno_actual)
            # else: # Reducir verbosidad
                # print("    ARCHIVADO (batch L->R): Columna 'source_filename' no encontrada.")
        
        print(f"WRITER (L->R): Escritura y archivado completados para '{self.dataset_cfg.get('source_subpath', 'N/A')}'.")

# --- Funciones Orquestadoras por Tipo de Dataset (para Landing -> Raw) ---

def process_csv_dataset(spark, source_path, target_path, archive_path, dataset_cfg, entorno_actual, autoloader_specific_paths=None):
    """Función orquestadora para procesar un dataset CSV usando las clases Reader y Writer."""
    
    reader = DatasetReader(spark, source_path, dataset_cfg, entorno_actual, autoloader_specific_paths)
    df_with_metadata, is_streaming = reader.read()

    # Asegurarse de que autoloader_checkpoint_location se pasa correctamente si es un stream
    checkpoint_loc_for_writer = None
    if is_streaming and autoloader_specific_paths:
        checkpoint_loc_for_writer = autoloader_specific_paths.get("autoloader_checkpoint_location")

    writer = RawLayerWriter(spark, target_path, archive_path, dataset_cfg, entorno_actual, 
                            autoloader_checkpoint_location=checkpoint_loc_for_writer)
    
    writer.write_and_archive(df_with_metadata, is_streaming)

# Agregar siguientes process_json_dataset, process_avro_dataset, etc.
# Cada una instanciará DatasetReader y RawLayerWriter, configurando DatasetReader
# con el 'type' y 'reader_options' correctos para ese formato.