# streaming_to_raw_processors.py
from pyspark.sql.functions import current_timestamp, date_format, col
try:
    from pyspark.sql.avro.functions import from_avro
except ImportError:
    from pyspark.sql.functions import from_avro # Fallback
import json
import utils # O from . import utils si está en un paquete

def process_kafka_avro_payload_to_raw_avro_files(
    spark, 
    kafka_source_options: dict, 
    value_avro_schema_string: str,
    raw_target_avro_files_path: str, 
    raw_stream_checkpoint_path: str, 
    raw_stream_partition_by: list = None
):
    print(f"INFO (KafkaAvro->RawAvroFiles): Iniciando stream desde Kafka.")
    print(f"  Configuración Kafka: {kafka_source_options}")
    print(f"  Destino Raw Avro Files: {raw_target_avro_files_path}")
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
            from_avro(col("value"), value_avro_schema_string, avro_options).alias("avro_payload_struct"), # Renombrado para claridad
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("timestampType").alias("kafka_timestampType")
        ).withColumn("ingestion_timestamp_raw", current_timestamp())

        # --- Lógica de Selección Corregida ---
        try:
            avro_schema_parsed = json.loads(value_avro_schema_string)
            payload_field_names_from_avro = [field["name"] for field in avro_schema_parsed.get("fields", [])]
        except json.JSONDecodeError:
            raise ValueError("value_avro_schema_string no es un JSON válido.")

        # Lista inicial de columnas de metadatos de Kafka (como strings para la comprobación)
        kafka_metadata_column_names = [
            "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", 
            "kafka_timestamp", "kafka_timestampType", "ingestion_timestamp_raw"
        ]
        
        # Lista final de objetos Column para la selección de Spark
        final_select_column_objects = [col(name) for name in kafka_metadata_column_names]
        
        # Añadir campos del payload Avro deserializado
        for field_name in payload_field_names_from_avro:
            alias_name = field_name
            # Comprobar colisión con los nombres de las columnas de metadatos
            if field_name in kafka_metadata_column_names:
                alias_name = f"payload_{field_name}" 
                print(f"ADVERTENCIA (KafkaAvro->RawAvroFiles): Campo Avro '{field_name}' renombrado a '{alias_name}' para evitar colisión.")
            final_select_column_objects.append(col(f"avro_payload_struct.{field_name}").alias(alias_name))
        
        final_df = deserialized_df.select(*final_select_column_objects)
        # --- Fin de la Lógica de Selección Corregida ---
        
        if raw_stream_partition_by:
            if "ingestion_date" in raw_stream_partition_by:
                final_df = final_df.withColumn(
                    "ingestion_date", date_format(col("ingestion_timestamp_raw"), "yyyy-MM-dd")
                )
            # Aquí podrías añadir lógica para otras columnas de partición si necesitan derivarse

        query_writer = final_df.writeStream \
            .format("avro") \
            .outputMode("append") \
            .option("checkpointLocation", raw_stream_checkpoint_path)
        
        if raw_stream_partition_by:
            for p_col in raw_stream_partition_by:
                if p_col not in final_df.columns: # Esta validación ahora se hace sobre el DF final
                    raise ValueError(f"Columna de partición para Raw Avro Files '{p_col}' no encontrada en el DataFrame. Columnas disponibles: {final_df.columns}")
            query_writer = query_writer.partitionBy(*raw_stream_partition_by)
        
        streaming_query = query_writer.start(raw_target_avro_files_path)
        
        print(f"INFO (KafkaAvro->RawAvroFiles): Stream iniciado. Escribiendo archivos Avro a {raw_target_avro_files_path}")
        return streaming_query

    except Exception as e:
        print(f"ERROR (KafkaAvro->RawAvroFiles): Falló la creación o inicio del stream. Error: {e}")
        import traceback
        traceback.print_exc()
        return None