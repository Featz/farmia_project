# farmia_engine/streaming_to_raw_processors.py
from pyspark.sql.functions import current_timestamp, date_format, col
try:
    from pyspark.sql.avro.functions import from_avro
except ImportError:
    from pyspark.sql.functions import from_avro # Fallback para versiones/setups diferentes
import json
# Para la estructura de paquete, la importación relativa es preferida si este archivo
# está en el mismo nivel que utils.py o en una subcarpeta.
# from . import utils
# Si utils.py está un nivel arriba y farmia_engine es el paquete raíz:
# from .. import utils 
# Por ahora, asumimos que utils es accesible directamente o a través de PYTHONPATH
import utils 

def process_kafka_avro_payload_to_raw_parquet_files(
    spark, 
    kafka_source_options: dict, 
    value_avro_schema_string: str,
    raw_target_parquet_files_path: str, 
    raw_stream_checkpoint_path: str, 
    raw_stream_partition_by: list = None
):
    """
    Lee datos de Kafka (payload Avro), deserializa, y escribe como archivos Parquet en Raw.
    """
    print(f"INFO (KafkaAvro->RawParquet): Iniciando stream desde Kafka.")
    print(f"  Config Kafka: {kafka_source_options}")
    print(f"  Destino Raw Parquet Files: {raw_target_parquet_files_path}")
    print(f"  Checkpoint: {raw_stream_checkpoint_path}")
    if raw_stream_partition_by:
        print(f"  Particionando Raw por: {raw_stream_partition_by}")

    if not value_avro_schema_string:
        raise ValueError("El 'value_avro_schema_string' es requerido para deserializar los mensajes de Kafka.")

    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .options(**kafka_source_options) \
            .load()

        avro_options = {"mode": "FAILFAST"} 
        
        deserialized_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            from_avro(col("value"), value_avro_schema_string, avro_options).alias("avro_payload_struct"), # Renombrado
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"), 
            col("timestampType").alias("kafka_timestampType")
        ).withColumn("ingestion_timestamp_raw", current_timestamp())

        try:
            avro_schema_parsed = json.loads(value_avro_schema_string)
            payload_field_names_from_avro = [field["name"] for field in avro_schema_parsed.get("fields", [])]
        except json.JSONDecodeError:
            raise ValueError("value_avro_schema_string no es un JSON válido.")

        # Lista inicial de NOMBRES de columnas de metadatos de Kafka (strings)
        kafka_metadata_column_names = [
            "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", 
            "kafka_timestamp", "kafka_timestampType", "ingestion_timestamp_raw"
        ]
        
        # Lista final de OBJETOS Column de Spark para la selección
        final_select_column_objects = [col(name) for name in kafka_metadata_column_names]
        
        for field_name in payload_field_names_from_avro:
            alias_name = field_name
            # Comprobar colisión con los NOMBRES de las columnas de metadatos (string in list of strings)
            if field_name in kafka_metadata_column_names:
                alias_name = f"payload_{field_name}" 
                print(f"ADVERTENCIA (KafkaAvro->RawParquet): Campo Avro '{field_name}' renombrado a '{alias_name}' para evitar colisión.")
            # Añadir como objeto Column de Spark
            final_select_column_objects.append(col(f"avro_payload_struct.{field_name}").alias(alias_name))
        
        final_df = deserialized_df.select(*final_select_column_objects) # Usar select con la lista de objetos Column
        
        if raw_stream_partition_by:
            if "ingestion_date" in raw_stream_partition_by:
                final_df = final_df.withColumn(
                    "ingestion_date", date_format(col("ingestion_timestamp_raw"), "yyyy-MM-dd")
                )
            
            for p_col in raw_stream_partition_by:
                if p_col not in final_df.columns:
                    raise ValueError(f"Columna de partición para Raw Parquet '{p_col}' no encontrada. Columnas: {final_df.columns}")
        
        query_writer = final_df.writeStream \
            .format("parquet")  \
            .outputMode("append") \
            .option("checkpointLocation", raw_stream_checkpoint_path)
        
        if raw_stream_partition_by:
            query_writer = query_writer.partitionBy(*raw_stream_partition_by)
        
        streaming_query = query_writer.start(raw_target_parquet_files_path)
        
        print(f"INFO (KafkaAvro->RawParquet): Stream iniciado. Escribiendo archivos Parquet a {raw_target_parquet_files_path}")
        return streaming_query

    except Exception as e:
        print(f"ERROR (KafkaAvro->RawParquet): Falló la creación o inicio del stream. Error: {e}")
        import traceback
        traceback.print_exc()
        return None

# Aquí podrías añadir en el futuro la función para Raw(Parquet) -> Bronze(Delta) si también es streaming.
# def process_raw_parquet_to_bronze_delta_stream(...):
#     ...