# farmia_engine/streaming_to_raw_processors.py
from pyspark.sql.functions import current_timestamp, date_format, col
# from_avro puede estar en dos lugares según la versión de Spark/Delta y cómo se instala el paquete Avro
# Intentar la importación más común primero
try:
    from pyspark.sql.avro.functions import from_avro
except ImportError:
    # Fallback para versiones más antiguas o configuraciones diferentes donde está en sql.functions
    from pyspark.sql.functions import from_avro 

# Asumimos que utils.py está en el mismo paquete.
# Cuando se ejecute como parte del paquete, la importación relativa es preferible:
# from . import utils
# Pero para que funcione si se prueba este script standalone (con utils.py en PYTHONPATH),
# o si la estructura del paquete no está completamente configurada para imports relativos aún:
import utils # Asegúrate de que farmia_engine esté en tu PYTHONPATH si lo ejecutas standalone.


def process_kafka_avro_payload_to_raw_avro_files(
    spark, 
    kafka_source_options: dict, 
    value_avro_schema_string: str,
    raw_target_avro_files_path: str, 
    raw_stream_checkpoint_path: str, 
    raw_stream_partition_by: list = None
):
    """
    Lee datos de un topic de Kafka (con payload Avro), deserializa el Avro,
    y escribe los datos estructurados + metadatos como archivos Avro en la capa Raw.
    Diseñado para ejecución local.
    """
    print(f"INFO (KafkaAvro->RawAvroFiles): Iniciando stream desde Kafka.")
    print(f"  Configuración Kafka: {kafka_source_options}")
    print(f"  Destino Raw Avro Files: {raw_target_avro_files_path}")
    print(f"  Checkpoint: {raw_stream_checkpoint_path}")
    if raw_stream_partition_by:
        print(f"  Particionando Raw por: {raw_stream_partition_by}")

    if not value_avro_schema_string:
        raise ValueError("El 'value_avro_schema_string' (esquema Avro del valor) es requerido para deserializar los mensajes de Kafka.")

    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .options(**kafka_source_options) \
            .load()

        # Opciones para from_avro:
        # "mode": "FAILFAST" (default), "PERMISSIVE" (campos que fallan se vuelven null), "DROPMALFORMED"
        avro_options = {"mode": "FAILFAST"} 
        
        # Seleccionar columnas de Kafka y deserializar el valor Avro
        deserialized_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            from_avro(col("value"), value_avro_schema_string, avro_options).alias("avro_payload"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"), # Timestamp del evento en Kafka
            col("timestampType").alias("kafka_timestampType")
        ).withColumn("ingestion_timestamp_raw", current_timestamp()) # Timestamp de cuando Spark procesa el evento

        # Expandir la estructura del payload Avro.
        # Esto asume que quieres todos los campos del Avro al mismo nivel que los metadatos de Kafka.
        # Si hay colisión de nombres, deberás renombrar o seleccionar explícitamente.
        # Ejemplo: .select("kafka_key", "avro_payload.sensor_id", "avro_payload.timestamp", ...)
        
        # Para hacerlo dinámicamente basado en el schema Avro:
        import json
        try:
            avro_schema_parsed = json.loads(value_avro_schema_string)
            payload_fields = [field["name"] for field in avro_schema_parsed.get("fields", [])]
        except json.JSONDecodeError:
            raise ValueError("value_avro_schema_string no es un JSON válido.")

        select_expressions = [
            "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", 
            "kafka_timestamp", "kafka_timestampType", "ingestion_timestamp_raw"
        ]
        for field_name in payload_fields:
            # Prevenir colisión si un campo Avro se llama igual que una columna de metadatos de Kafka
            alias_name = field_name
            if field_name in select_expressions:
                alias_name = f"payload_{field_name}" 
                print(f"ADVERTENCIA: Campo Avro '{field_name}' renombrado a '{alias_name}' para evitar colisión con metadatos de Kafka.")
            select_expressions.append(col(f"avro_payload.{field_name}").alias(alias_name))
        
        final_df = deserialized_df.selectExpr(*select_expressions)
        
        # Añadir columnas de partición basadas en ingestion_timestamp_raw si se especifica
        # El formato "yyyy-MM-dd" es compatible con nombres de directorio de partición de Hive.
        if raw_stream_partition_by:
            if "ingestion_date" in raw_stream_partition_by:
                final_df = final_df.withColumn(
                    "ingestion_date", date_format(col("ingestion_timestamp_raw"), "yyyy-MM-dd")
                )
            # Aquí podrías añadir lógica para otras columnas de partición si necesitan derivarse
            # (ej. extraer año/mes de un campo de fecha del payload Avro).

        # Escribir el stream como archivos Avro en la capa Raw
        query_writer = final_df.writeStream \
            .format("avro") \
            .outputMode("append") \
            .option("checkpointLocation", raw_stream_checkpoint_path)
        
        if raw_stream_partition_by:
            # Verificar que las columnas de partición existan en final_df
            for p_col in raw_stream_partition_by:
                if p_col not in final_df.columns:
                    raise ValueError(f"Columna de partición para Raw Avro Files '{p_col}' no encontrada en el DataFrame. Columnas disponibles: {final_df.columns}")
            query_writer = query_writer.partitionBy(*raw_stream_partition_by)
        
        # Iniciar el stream
        streaming_query = query_writer.start(raw_target_avro_files_path)
        
        print(f"INFO (KafkaAvro->RawAvroFiles): Stream iniciado. Escribiendo archivos Avro a {raw_target_avro_files_path}")
        return streaming_query

    except Exception as e:
        print(f"ERROR (KafkaAvro->RawAvroFiles): Falló la creación o inicio del stream. Error: {e}")
        import traceback
        traceback.print_exc()
        return None