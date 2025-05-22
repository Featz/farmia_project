# scripts/run_kafka_stream_to_raw.py
import sys
import os

# --- Añadir el directorio raíz del proyecto a sys.path ---
# Esto permite que Python encuentre el paquete 'farmia_engine' cuando
# este script se ejecuta desde dentro del directorio 'scripts/' o desde
# el directorio raíz del proyecto, y 'farmia_engine' no está instalado formalmente.

# Ruta al archivo actual (scripts/run_local_landing_to_raw.py)
current_file_path = os.path.abspath(__file__)
# Ruta al directorio 'scripts/'
scripts_dir = os.path.dirname(current_file_path)
# Ruta al directorio raíz del proyecto (un nivel arriba de 'scripts/')
project_root = os.path.dirname(scripts_dir)

# Añadir el directorio raíz del proyecto al inicio de sys.path
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- Fin de la configuración de sys.path ---

try:
    from farmia_engine.main_orchestrators import execute_kafka_avro_payload_to_raw_files_stream
    # utils y processors son usados internamente por main_orchestrators
except ImportError as e:
    print(f"Error: No se pudo importar desde 'farmia_engine'. Asegúrate de que:")
    print(f"1. Estás ejecutando este script desde el directorio raíz del proyecto ('{project_root}').")
    print(f"2. O que el paquete 'farmia_engine' está instalado en tu entorno Python.")
    print(f"Detalle del error de importación: {e}")
    sys.exit(1)
except Exception as e_general:
    print(f"Ocurrió un error inesperado durante la importación: {e_general}")
    import traceback
    traceback.print_exc()
    sys.exit(1)


def start_local_kafka_to_raw_stream():
    """
    Punto de entrada para iniciar el stream local de Kafka (Avro Payload) a Raw (Avro Files).
    """
    print("======================================================================")
    print("INICIANDO SCRIPT LOCAL: STREAM KAFKA (AVRO) -> RAW (AVRO FILES)")
    print("======================================================================")
    
    # La ruta a config.json se resolverá por defecto a "config.json" en el directorio
    # desde donde se ejecuta este script (idealmente, el raíz del proyecto).
    # utils.load_app_config dentro de execute_kafka_to_raw_avro_files lo manejará.
    
    # Es importante que tu productor de Kafka (scripts/kafka_iot_producer.py)
    # esté enviando mensajes en formato Avro al topic configurado.
    # El productor actual envía JSON, así que necesitarás ajustarlo o usar uno que produzca Avro.
    
    active_stream_queries = []
    try:
        # execute_kafka_to_raw_avro_files ahora está en main_orchestrators
        # y devuelve una lista de queries activas.
        # Asumimos que execute_kafka_to_raw_avro_files maneja la obtención de SparkSession y config.
        print("Llamando a execute_kafka_to_raw_avro_files...")
        # La función execute_kafka_to_raw_avro_files ya no devuelve las queries directamente en la última refactorización,
        # sino que maneja el awaitTermination o la lógica de streams activos internamente.
        # Si quisiéramos controlarlo aquí, necesitaríamos que devuelva las queries.
        # Por ahora, confiamos en la lógica de main_orchestrators.
        
        execute_kafka_avro_payload_to_raw_files_stream(config_path_override=None)
        
        # Para un script de ejecución local que debe mantenerse vivo para el streaming:
        print("\nINFO: El stream (o streams) de Kafka a Raw se ha iniciado.")
        
   


    except KeyboardInterrupt:
        print("\nINFO: Script detenido por el usuario (Ctrl+C).")
    except Exception as e:
        print(f"ERROR FATAL en el script de streaming Kafka -> Raw: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # La detención de SparkSession ahora se maneja dentro de la función execute_...
        # si es un entorno local y no hay streams activos que se espera que sigan corriendo.
        # Si los streams deben correr indefinidamente, no se debe llamar a spark.stop() aquí
        # hasta que se decida terminar la aplicación de streaming.
        print("INFO: Script de streaming Kafka -> Raw finalizando (la sesión de Spark podría seguir activa si los streams son continuos y no se detuvieron).")

    print("======================================================================")
    print("SCRIPT LOCAL: STREAM KAFKA (AVRO) -> RAW (AVRO FILES) FINALIZADO (o streams siguen en segundo plano)")
    print("======================================================================")

if __name__ == "__main__":
    start_local_kafka_to_raw_stream()