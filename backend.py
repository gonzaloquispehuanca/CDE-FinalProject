from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
import requests
import json
import happybase
from flask_cors import CORS
from pyspark.sql.functions import col, floor, explode, split
from pyspark.sql import functions as F

################################## INITIALICE ###########################################

spark = SparkSession.builder.appName("BackendService").getOrCreate()
EMR_HOST = "ec2-44-220-168-251.compute-1.amazonaws.com"
HDFS_NAMENODE_HOST = EMR_HOST + ":8020"

name_basics_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/name_basics_parquet/000000_0"
title_akas_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_akas_parquet/000000_0"
title_crew_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_crew_parquet/000000_0"
title_episode_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_episode_parquet/000000_0"
title_principals_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_principals_parquet/000000_0"
title_ratings_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_ratings_parquet/000000_0"
title_basics_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/movies_partitioned_by_startyear/"

API_KEY = '1d5244cba600b25da53d407a784687f7'
BASE_URL = 'https://api.themoviedb.org/3'

def get_hbase_connection():
    try:
        connection = happybase.Connection(EMR_HOST, autoconnect=False)
        connection.open()  # Abrir la conexión
        print(f"Conexión exitosa a HBase en {EMR_HOST}.")
        return connection
    except Exception as e:
        print(f"Error al conectar con HBase: {e}")
        raise

app = Flask(__name__)
CORS(app)



################################## FUNCTIONS ###########################################
def load_name_basics():
    return spark.read.format("parquet").load(name_basics_parquet)

def load_title_akas():
    return spark.read.format("parquet").load(title_akas_parquet)

def load_title_crew():
    return spark.read.format("parquet").load(title_crew_parquet)

def load_title_episode():
    return spark.read.format("parquet").load(title_episode_parquet)

def load_title_principals():
    return spark.read.format("parquet").load(title_principals_parquet)

def load_title_ratings():
    return spark.read.format("parquet").load(title_ratings_parquet)

def load_title_basics():
    # Cargar el archivo title_basics desde HDFS, sin especificar partición
    return spark.read.format("parquet").load(title_basics_parquet)

def load_title_ratings_by_id(tconst):
    return spark.read.format("parquet").load(title_ratings_parquet).filter(f"tconst = '{tconst}'")

def search_movie_tmdb_local(nombre_pelicula):
    print("Searching: ", nombre_pelicula)
    url = f"{BASE_URL}/search/movie?api_key={API_KEY}&query={nombre_pelicula}&language=en-US"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return None
    
def search_id_tmdb_local(id):
    print("Searching: ", id)
    url = f"{BASE_URL}/movie/{id}?api_key={API_KEY}&query={id}&language=en-US"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return None

def mx_get_last_votes():
    try:
        connection = get_hbase_connection()
        table = connection.table("movie_votes_temp")
        rows = table.scan()
        result = []
        for key, data in rows:
            decoded_key = key.decode('utf-8')
            decoded_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
            result.append({'row_key': decoded_key, 'data': decoded_data})
        return result  # Devuelve la lista directamente
    except Exception as e:
        print("Error: ", e)
        return None
    finally:
        connection.close()
        
    
# Consultar HBase y actualizar el valor
def mx_get_rating_by_movie_id(movie_id):
    print(f"mx_get_rating_by_movie_id: {movie_id}")

    # Cargar el rating del archivo Parquet
    df_title_ratings = load_title_ratings_by_id(movie_id)

    if df_title_ratings.count() == 0:
        print(f"No se encontró el movie_id: {movie_id} en el Parquet.")
        return None  # Si no se encuentra el id en Parquet, retornamos None

    # Obtener valores del Parquet
    row = df_title_ratings.collect()[0]  # Asumimos que solo hay un registro para cada tconst
    tconst = row['tconst']
    averagerating = row['averagerating']
    numvotes = row['numvotes']

    try:
        connection = get_hbase_connection()
        table = connection.table("movie_votes_temp")

        # Buscar registros en HBase donde la clave contenga el `tconst`
        hbase_rows = table.scan(row_start=tconst.encode('utf-8'))  # Realiza una búsqueda de todas las claves que empiecen con `tconst`
        
        total_votes = 0
        total_rating = 0.0
        
        for row_key, data in hbase_rows:
            # Comprobar si el tconst está contenido en la clave de HBase
            if tconst.encode('utf-8') in row_key:
                # Leer el voto almacenado en HBase
                hbase_vote = int(data[b'cf:vote'].decode('utf-8'))
                
                # Acumular el total de votos y la calificación
                total_votes += 1
                total_rating += hbase_vote
        
        # Actualizar el averagerating y numvotes
        if total_votes > 0:
            new_numvotes = numvotes + total_votes
            new_averagerating = (float(averagerating) * numvotes + total_rating) / new_numvotes
            print(f"Nuevo promedio: {new_averagerating}, Nuevo número de votos: {new_numvotes}")
            return {"averagerating": new_averagerating, "numvotes": new_numvotes}

        else:
            print(f"No se encontraron votos en HBase para el tconst: {tconst}. Usando valores originales.")
            return {"averagerating": averagerating, "numvotes": numvotes}
    except Exception as e:
        print(f"Error al interactuar con HBase: {e}")
        return None
    finally:
        connection.close()

################################## END POINTS ###########################################
@app.route('/select_name_basics', methods=['GET'])
def select_name_basics():
    df_name_basics_parquet = load_name_basics()
    """Consulta los datos de name_basics."""
    result = df_name_basics_parquet.select("nconst", "primaryName").limit(10).collect()
    
    result_list = [{"nconst": row['nconst'], "primaryName": row['primaryName']} for row in result]
    
    return jsonify(result_list)

@app.route('/select_title_akas', methods=['GET'])
def select_title_akas():
    df_title_akas_parquet = load_title_akas()
    """Consulta los datos de title_akas."""
    result = df_title_akas_parquet.select("title", "region").limit(10).collect()
    result_list = [{"title": row['title'], "region": row['region']} for row in result]
    
    return jsonify(result_list)

@app.route('/search_tmdb_local', methods=['GET'])
def search_movie_tmdb_local_endpoint():
    movie_name = request.args.get('movie_name') 
    if movie_name:
        result = search_movie_tmdb_local(movie_name) 
        if result:
            return jsonify(result)  
        else:
            return jsonify({"error": "No results found"}), 404  
    else:
        return jsonify({"error": "Missing movie_name parameter"}), 400  
    
@app.route('/search_id_tmdb_local', methods=['GET'])
def search_id_tmdb_local_endpoint():
    movie_id = request.args.get('movie_id')  
    if movie_id:
        result = search_id_tmdb_local(movie_id)  
        if result:
            return jsonify(result)  
        else:
            return jsonify({"error": "No results found"}), 404  
    else:
        return jsonify({"error": "Missing movie_name parameter"}), 400  
    
@app.route('/get_rating_by_movie_id', methods=['GET'])
def get_rating():
    movie_id = request.args.get('movie_id') 
    if movie_id:
        result = mx_get_rating_by_movie_id(movie_id)
        if result:
            return result
        else:
            return {"error": "No results found"}, 404  
    else:
        return jsonify({"error": "Missing movie_name parameter"}), 400  

@app.route('/get_last_votes', methods=['GET'])
def get_last_votes():
    result = mx_get_last_votes()
    if result:
        return jsonify(result)  
    else:
        return jsonify({"error": "No results found"}), 404
    
@app.route('/distribution_genres_by_decade', methods=['GET'])
def distribution_genres_by_decade():
    # Cargar el archivo title.basics.tsv.gz
    df_title_basics = load_title_basics()
    
    # Filtrar las columnas 'genres' y 'startYear'
    df_filtered = df_title_basics.select("genres", "startYear").filter(df_title_basics["startYear"].isNotNull())
    
    # Convertir 'startYear' a década (ejemplo: 1980, 1990, etc.)
    df_filtered = df_filtered.withColumn("decade", floor(col("startYear") / 10) * 10)
    
    # Explode de la columna 'genres' para poder contar cada género por separado
    df_exploded = df_filtered.withColumn("genre", explode(split(df_filtered["genres"], ",")))
    
    # Agrupar por década y género, y contar el número de títulos en cada grupo
    df_grouped = df_exploded.groupBy("decade", "genre").count()
    
    # Ordenar los resultados por década y género
    df_grouped = df_grouped.orderBy("decade", "genre")
    
    # Recoger los resultados y convertirlos en formato JSON
    result = df_grouped.collect()
    result_list = [{"decade": row['decade'], "genre": row['genre'], "count": row['count']} for row in result]
    
    return jsonify(result_list)


@app.route('/heatmap_movie_releases', methods=['GET'])
def heatmap_movie_releases():
    # Cargar los datos desde el archivo Parquet
    df_title_basics = load_title_basics()

    # Filtrar por las columnas de interés (startYear y startMonth)
    df_filtered = df_title_basics.select("startYear", "startMonth").filter(df_title_basics.startYear.isNotNull() & df_title_basics.startMonth.isNotNull())

    # Agrupar por año y mes y contar el número de lanzamientos
    df_count = df_filtered.groupBy("startYear", "startMonth").count()

    # Recoger los resultados
    results = df_count.collect()

    # Organizar los datos en una estructura de matriz para el mapa de calor
    heatmap_data = {}
    for row in results:
        year = row['startYear']
        month = row['startMonth']
        count = row['count']
        
        if year not in heatmap_data:
            heatmap_data[year] = [0] * 12  # Inicializar los 12 meses
        heatmap_data[year][month - 1] = count  # Los meses empiezan en 1, así que restamos 1

    # Convertir la estructura en un formato adecuado para el frontend
    heatmap_matrix = [{"year": year, "months": months} for year, months in heatmap_data.items()]

    return jsonify(heatmap_matrix)

@app.route('/collaboration_heatmap', methods=['GET'])
def collaboration_heatmap():
    # Cargar los datos desde los archivos Parquet
    df_crew = load_title_crew()
    df_principals = load_title_principals()
    df_ratings = load_title_ratings()

    # Filtrar y seleccionar las columnas necesarias
    df_crew_filtered = df_crew.select("tconst", "directors", "writers")
    df_principals_filtered = df_principals.select("tconst", "nconst")  # nconst: Actor/actriz principal
    df_ratings_filtered = df_ratings.select("tconst", "averageRating")

    # Unir los DataFrames para obtener los directores, escritores, actores y las calificaciones promedio
    df = df_crew_filtered.join(df_ratings_filtered, on="tconst", how="inner") \
                         .join(df_principals_filtered, on="tconst", how="inner")

    # Crear un grafo de colaboraciones (unimos directores, escritores y actores)
    # Para ello, separamos los directores y escritores en listas
    df = df.withColumn("directors", F.split(col("directors"), ","))
    df = df.withColumn("writers", F.split(col("writers"), ","))

    # Crear las combinaciones entre actores, directores y escritores
    df_actor_director = df.withColumn("actor_director", F.explode("directors")) \
                          .withColumn("collaborator", col("nconst")) \
                          .select("actor_director", "collaborator", "averageRating")
    
    df_actor_writer = df.withColumn("actor_writer", F.explode("writers")) \
                        .withColumn("collaborator", col("nconst")) \
                        .select("actor_writer", "collaborator", "averageRating")

    df_director_writer = df.withColumn("director_writer", F.explode("writers")) \
                           .withColumn("collaborator", col("directors")) \
                           .select("director_writer", "collaborator", "averageRating")

    # Unir todas las combinaciones
    df_combined = df_actor_director.union(df_actor_writer).union(df_director_writer)

    # Agrupar por las combinaciones y calcular la calificación promedio
    df_grouped = df_combined.groupBy("actor_director", "collaborator") \
                            .agg(F.avg("averageRating").alias("averageRating"))

    # Recoger los resultados
    results = df_grouped.collect()

    # Organizar los datos en un formato adecuado para el frontend
    collaboration_matrix = []
    for row in results:
        collaboration_matrix.append({
            "actor_director": row['actor_director'],
            "collaborator": row['collaborator'],
            "averageRating": row['averageRating']
        })

    return jsonify(collaboration_matrix)
#https://image.tmdb.org/t/p/original/oYuLEt3zVCKq57qu2F8dT7NIa6f.jpg

#https://image.tmdb.org/t/p/original/8ZTVqvKDQ8emSGUEMjsS4yHAwrp.jpg

# Iniciar el servidor
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
