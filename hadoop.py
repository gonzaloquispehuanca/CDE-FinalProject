import os
import shutil
import requests
from datetime import datetime
from subprocess import call
import subprocess
import time

# Constante que define el intervalo en minutos
# Configuración
HDFS_PATH = "/user/latest"
LOCAL_DIR = "latest"
URLS = [
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    "https://datasets.imdbws.com/title.akas.tsv.gz",
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.crew.tsv.gz",
    "https://datasets.imdbws.com/title.episode.tsv.gz",
    "https://datasets.imdbws.com/title.principals.tsv.gz",
    "https://datasets.imdbws.com/title.ratings.tsv.gz"
]

# Plantillas específicas para cada archivo
HIVE_TABLE_TEMPLATES = {
    "name.basics": """
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        nconst STRING,
        primaryName STRING,
        birthYear STRING,
        deathYear STRING,
        primaryProfession STRING,
        knownForTitles STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\t'
    STORED AS TEXTFILE
    LOCATION '{hdfs_path}'
    TBLPROPERTIES (
        "skip.header.line.count"="1",
        "hive.lazysimple.extended_boolean_literal"="true");    
    """,
    "title.akas": """
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        titleId STRING,
        ordering INT,
        title STRING,
        region STRING,
        language STRING,
        types STRING,
        attributes STRING,
        isOriginalTitle STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\t'
    STORED AS TEXTFILE
    LOCATION '{hdfs_path}'
    TBLPROPERTIES (
        "skip.header.line.count"="1",
        "hive.lazysimple.extended_boolean_literal"="true");
    """,
    "title.basics": """
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        tconst STRING,
        titleType STRING,
        primaryTitle STRING,
        originalTitle STRING,
        isAdult STRING,
        startYear STRING,
        endYear STRING,
        runtimeMinutes STRING,
        genres STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\t'
    STORED AS TEXTFILE
    LOCATION '{hdfs_path}'
    TBLPROPERTIES (
        "skip.header.line.count"="1",
        "hive.lazysimple.extended_boolean_literal"="true");
    """,
    "title.crew": """
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        tconst STRING,
        directors STRING,
        writers STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\t'
    STORED AS TEXTFILE
    LOCATION '{hdfs_path}'
    TBLPROPERTIES (
        "skip.header.line.count"="1",
        "hive.lazysimple.extended_boolean_literal"="true");    
    """,
    "title.episode": """
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        tconst STRING,
        parentTconst STRING,
        seasonNumber INT,
        episodeNumber INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\t'
    STORED AS TEXTFILE
    LOCATION '{hdfs_path}'
    TBLPROPERTIES (
        "skip.header.line.count"="1",
        "hive.lazysimple.extended_boolean_literal"="true");   
    """,
    "title.principals": """
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        tconst STRING,
        ordering INT,
        nconst STRING,
        category STRING,
        job STRING,
        characters STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\t'
    STORED AS TEXTFILE
    LOCATION '{hdfs_path}'
    TBLPROPERTIES (
        "skip.header.line.count"="1",
        "hive.lazysimple.extended_boolean_literal"="true");    
    """,
    "title.ratings": """
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        tconst STRING,
        averageRating STRING,
        numVotes INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\t'
    STORED AS TEXTFILE
    LOCATION '{hdfs_path}'
    TBLPROPERTIES (
        "skip.header.line.count"="1",
        "hive.lazysimple.extended_boolean_literal"="true");   
    """
}

# Función para verificar espacio en disco
def check_disk_space():
    """Verifica el espacio disponible en disco."""
    total, used, free = shutil.disk_usage("/")
    print(f"Total: {total // (2**30)} GB; Usado: {used // (2**30)} GB; Libre: {free // (2**30)} GB")
    return free

# Función para descargar y subir archivos a HDFS
def download_and_upload_to_hdfs():
    """Descarga archivos directamente a HDFS, sin descomprimir localmente."""
    for url in URLS:
        filename = url.split("/")[-1]
        hdfs_path = f"{HDFS_PATH}/{filename}"
        print(f"Descargando y subiendo a HDFS: {url}...")

        # Verificar el espacio disponible antes de descargar
        if check_disk_space() < 1 * 1024**3:  # Verifica si hay menos de 1GB libre
            print("Espacio en disco insuficiente. Abortando descarga.")
            return
        
        # Descargar el archivo a un archivo temporal
        response = requests.get(url, stream=True)
        temp_file_path = f"/tmp/{filename}"

        with open(temp_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        print(f"Archivo {filename} descargado temporalmente en {temp_file_path}.")

        # Verificar si el archivo se descargó correctamente
        if not os.path.exists(temp_file_path):
            print(f"Error: El archivo {filename} no se descargó correctamente.")
            continue

        # Subir el archivo comprimido a HDFS
        upload_result = call(["hdfs", "dfs", "-put", temp_file_path, hdfs_path])
        if upload_result != 0:
            print(f"Error al subir el archivo {filename} a HDFS.")
            continue

        os.remove(temp_file_path)  # Eliminar archivo temporal local
        print(f"Archivo {filename} subido a HDFS.")

        # Descomprimir el archivo en HDFS
        hdfs_text_file_path = f"{hdfs_path[:-3]}"
        with open(os.devnull, 'w') as devnull:
            decompress_result = call(["hdfs", "dfs", "-text", hdfs_path], stdout=devnull)

        if decompress_result != 0:
            print(f"Error al descomprimir el archivo {filename} en HDFS.")
        else:
            print(f"Archivo {filename} descomprimido en HDFS a {hdfs_text_file_path}.")


# Función para crear tablas en Hive
def create_hive_tables():
    """Crea tablas en Hive basadas en plantillas específicas."""
    print("Creando tablas en Hive...")
    for url in URLS:
        print("Leyendo: ", url)
        file_name = url.split("/")[-1].replace(".tsv.gz", "")
        table_name = f"{file_name.replace('.', '_')}_raw"
        hdfs_file_path = f"{HDFS_PATH}/{file_name}.tsv"
        
        # Seleccionar plantilla específica para el archivo
        if file_name in HIVE_TABLE_TEMPLATES:
            hive_template = HIVE_TABLE_TEMPLATES[file_name]
            hive_commands = hive_template.format(table_name=table_name, hdfs_path=hdfs_file_path)

            # Ejecutar comandos Hive
            call(["hive", "-e", hive_commands])
            print(f"Tabla {table_name} creada.")
        else:
            print(f"Advertencia: No se encontró una plantilla Hive para {file_name}. Saltando.")


# Función para cargar los datos en Hive
def load_data_into_hive():
    """Carga los datos desde los archivos descomprimidos en HDFS a las tablas de Hive."""
    print("Cargando datos a Hive...")
    for url in URLS:
        file_name = url.split("/")[-1].replace(".tsv.gz", "")
        hdfs_file_path = f"{HDFS_PATH}/{file_name}.tsv.gz"
        table_name = f"{file_name.replace('.', '_')}_raw"

        # Cargar los datos en la tabla Hive
        load_command = f"LOAD DATA INPATH '{hdfs_file_path}' INTO TABLE {table_name};"
        call(["hive", "-e", load_command])
        print(f"Datos cargados en la tabla {table_name}.")


def partition_and_load_movies():
    """Particiona y carga los datos en una tabla Hive sin necesidad de especificar el puerto."""
    # Nombre de las tablas
    raw_table = "title_basics_raw"
    partitioned_table = "movies_partitioned_by_startyear"

    # Comando para eliminar la tabla particionada si ya existe
    drop_table_command = f"DROP TABLE IF EXISTS {partitioned_table};"
    
    # Ejecutar el comando para eliminar la tabla si existe
    subprocess.run(f"hive -e \"{drop_table_command}\"", shell=True, check=True)
    print(f"Tabla {partitioned_table} eliminada si existía.")

    # Comando para crear la tabla particionada
    create_table_command = f"""
    CREATE TABLE IF NOT EXISTS {partitioned_table} (
        tconst STRING,
        titleType STRING,
        primaryTitle STRING,
        originalTitle STRING,
        isAdult INT,
        endYear STRING,
        runtimeMinutes STRING,
        genres STRING
    )
    PARTITIONED BY (startYear INT)
    STORED AS PARQUET;
    """
    
    # Ejecutar el comando para crear la tabla
    subprocess.run(f"hive -e \"{create_table_command}\"", shell=True, check=True)
    print(f"Tabla {partitioned_table} creada exitosamente.")

    # Comando para insertar datos en la tabla particionada
    insert_data_command = f"""
    SET hive.exec.dynamic.partition.mode=nonstrict;
    
    INSERT OVERWRITE TABLE {partitioned_table} PARTITION (startYear)
    SELECT 
        tconst, 
        titleType, 
        primaryTitle, 
        originalTitle, 
        isAdult, 
        endYear, 
        runtimeMinutes, 
        genres, 
        startYear 
    FROM {raw_table};
    """
    
    # Ejecutar el comando para insertar los datos
    subprocess.run(f"hive -e \"{insert_data_command}\"", shell=True, check=True)
    print(f"Datos insertados correctamente en {partitioned_table}.")


def convert_tables_to_parquet():
    """Convierte las tablas existentes a formato PARQUET."""
    tables = [
        "name_basics_raw",
        "title_akas_raw",
        "title_crew_raw",
        "title_episode_raw",
        "title_principals_raw",
        "title_ratings_raw"
    ]
    
    # Iteramos sobre cada tabla para crearla en formato PARQUET
    for table in tables:
        # Nombre de la nueva tabla en formato PARQUET
        parquet_table = f"{table.replace('_raw', '')}_parquet"
        
        # Comando para eliminar la tabla particionada si ya existe
        drop_table_command = f"DROP TABLE IF EXISTS {parquet_table};"
        
        # Ejecutar el comando para eliminar la tabla si existe
        subprocess.run(f"hive -e \"{drop_table_command}\"", shell=True, check=True)
        print(f"Tabla {parquet_table} eliminada si existía.")

        # Comando para crear la tabla en formato PARQUET
        create_table_command = f"""
        CREATE TABLE IF NOT EXISTS {parquet_table} 
        STORED AS PARQUET AS
        SELECT * FROM {table};
        """
        
        # Ejecutar el comando para crear la tabla en formato PARQUET
        subprocess.run(f"hive -e \"{create_table_command}\"", shell=True, check=True)
        print(f"Tabla {parquet_table} creada exitosamente en formato PARQUET.")
    
    print("Conversión de tablas a formato PARQUET completada.")


def run_process():
    try:
        download_and_upload_to_hdfs()
        create_hive_tables()
        load_data_into_hive()
        partition_and_load_movies()
        convert_tables_to_parquet()
        print("Ejecución completada exitosamente.")
    except Exception as e:
        print(f"Error durante la ejecución: {e}")


if __name__ == "__main__":
    INTERVAL_MINUTES = 600
    # Cálculo del intervalo en segundos
    interval_seconds = INTERVAL_MINUTES * 60
    while True:
        print("Iniciando ciclo de ejecución...")
        run_process()
        print(f"Esperando {INTERVAL_MINUTES} minutos para la próxima ejecución.")
        time.sleep(interval_seconds)