from confluent_kafka import Consumer, KafkaException, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import happybase  # Cliente para interactuar con HBase
import time

KAFKA_PRODUCER_HOST = "35.173.137.173"
EMR_HOST = "ec2-44-220-168-251.compute-1.amazonaws.com"
HDFS_NAMENODE_HOST = EMR_HOST + ":8020"

# Parámetros configurables
TABLE_NAME = "movie_votes_temp"
BATCH_SIZE = 100

# Configuración del consumer de Kafka
conf = {
    'bootstrap.servers': KAFKA_PRODUCER_HOST + ":9092",
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest',  # Leer mensajes desde el principio si no hay commits
}

# Crear una instancia del consumer
consumer = Consumer(conf)

# Suscribirse al topic
consumer.subscribe(['movie_vote'])

# Crear una sesión de Spark
spark = SparkSession.builder.appName("KafkaHBaseIntegration").enableHiveSupport().getOrCreate()

# Ruta de la tabla Parquet en HDFS
title_ratings_parquet = f"hdfs://{HDFS_NAMENODE_HOST}/user/hive/warehouse/title_ratings_parquet/000000_0"
print("Usando: ", title_ratings_parquet)

# Schema del archivo Parquet
schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("averagerating", StringType(), True),
    StructField("numvotes", IntegerType(), True)
])

def get_hbase_connection():
    try:
        connection = happybase.Connection(EMR_HOST, autoconnect=False)
        connection.open()
        print(f"Conexión exitosa a HBase en {EMR_HOST}.")
        return connection
    except Exception as e:
        print(f"Error al conectar con HBase: {e}")
        raise

# Crear la tabla en HBase si no existe
def create_table():
    try:
        connection = get_hbase_connection()
        tables = connection.tables()
        if TABLE_NAME.encode('utf-8') not in tables:
            connection.create_table(
                TABLE_NAME,
                {'cf': dict()}  # Familia de columnas "cf"
            )
            print(f"Tabla {TABLE_NAME} creada en HBase.")
        else:
            print(f"Tabla {TABLE_NAME} ya existe en HBase.")
    except Exception as e:
        print(f"Error al crear la tabla {TABLE_NAME}: {e}")
        raise
    finally:
        connection.close()

# Crear la tabla si no existe
create_table()

# Función para almacenar datos en HBase
def store_in_hbase(tconst, vote):
    print(f"Almacenando tconst: {tconst} vote : {vote}")
    try:
        # Crear un timestamp único para cada mensaje
        timestamp = str(int(time.time() * 1000))  # Milisegundos
        row_key = f"{tconst}_{timestamp}"  # Concatenar tconst y timestamp para crear una clave única
        
        connection = get_hbase_connection()
        table = connection.table(TABLE_NAME)
        table.put(
            row_key.encode('utf-8'),  # Clave de fila (row key) con timestamp
            {
                b'cf:vote': str(vote).encode('utf-8')  # Almacenar el voto en la columna 'vote'
            }
        )
        print(f"Registro almacenado en HBase: {row_key}, voto: {vote}")
    except Exception as e:
        print(f"Error al almacenar datos en HBase: {e}")
        raise
    finally:
        connection.close()

# Contador de mensajes procesados en HBase
#def get_hbase_row_count():
#    return sum(1 for _ in table.scan())

# Unión de datos de HBase y Parquet
#def merge_hbase_and_parquet():
#    # Leer HBase como RDD
#    hbase_data = table.scan()
#    hbase_rows = [(row[0].decode('utf-8'), int(row[1][b'cf:vote'].decode('utf-8'))) for row in hbase_data]
#
#    # Convertir HBase a DataFrame
#    hbase_df = spark.createDataFrame(hbase_rows, schema=["tconst", "vote"])
#
#    # Leer Parquet
#    parquet_df = spark.read.schema(schema).parquet(title_ratings_parquet)
#
#    # Unión de DataFrames
#    combined_df = parquet_df.join(hbase_df, parquet_df["tconst"] == hbase_df["tconst"], "outer") \
#        .withColumn(
#            "numvotes",
#            when(parquet_df["tconst"].isNotNull(), col("numvotes").cast("int") + col("vote").cast("int")).otherwise(col("vote"))
#        ) \
#        .withColumn(
#            "averagerating",
#            when(parquet_df["tconst"].isNotNull(),
#                 (col("averagerating").cast("double") * col("numvotes").cast("int") + col("vote")) /
#                 (col("numvotes").cast("int") + col("vote"))
#            ).otherwise(None)
#        )
#
#    # Sobrescribir Parquet
#    combined_df.write.mode("overwrite").parquet(title_ratings_parquet)
#    print("Parquet actualizado con los datos de HBase.")

# Procesar mensajes de Kafka
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Fin de la partición {msg.partition()} alcanzado en {msg.topic()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Procesar el mensaje
            message = msg.value().decode('utf-8')
            try:
                message = message[1:-1]  # Eliminar los caracteres de inicio y fin
                print("Procesando el mensaje: ", message)
                # Suponiendo que el mensaje tiene el formato: "tconst_vote"
                tconst, vote = message.split('_')
                tconst = tconst.strip()  # Eliminar espacios
                vote = int(vote.strip())  # Convertir el voto a entero

                # Almacenar en HBase
                store_in_hbase(tconst, vote)

                # Verificar si se alcanza el tamaño del batch
                #row_count = get_hbase_row_count()
                #if row_count % BATCH_SIZE == 0:
                #    print(f"Procesados {row_count} registros, iniciando combinación con Parquet.")
                #    merge_hbase_and_parquet()

            except Exception as e:
                print(f"Error procesando el mensaje: {e}")

except KeyboardInterrupt:
    print("Saliendo...")

finally:
    consumer.close()
