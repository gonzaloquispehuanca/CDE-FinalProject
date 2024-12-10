from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
import requests
import json
import happybase

################################## INITIALICE ###########################################

spark = SparkSession.builder.appName("BackendService").getOrCreate()
EMR_HOST = "ec2-44-197-184-85.compute-1.amazonaws.com"
HDFS_NAMENODE_HOST = EMR_HOST + ":8020"

name_basics_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/name_basics_parquet/000000_0"
title_akas_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_akas_parquet/000000_0"
title_crew_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_crew_parquet/000000_0"
title_episode_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_episode_parquet/000000_0"
title_principals_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_principals_parquet/000000_0"
title_ratings_parquet = "hdfs://"+HDFS_NAMENODE_HOST+"/user/hive/warehouse/title_ratings_parquet/000000_0"

API_KEY = '1d5244cba600b25da53d407a784687f7'
BASE_URL = 'https://api.themoviedb.org/3'


def get_hbase_connection():
    try:
        connection = happybase.Connection(EMR_HOST)
        connection.open()  # Abrir la conexión
        print(f"Conexión exitosa a HBase en {EMR_HOST}.")
        return connection
    except Exception as e:
        print(f"Error al conectar con HBase: {e}")
        raise
connection = get_hbase_connection()

app = Flask(__name__)



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
            return json.dumps({"averagerating": new_averagerating, "numvotes": new_numvotes})

        else:
            print(f"No se encontraron votos en HBase para el tconst: {tconst}. Usando valores originales.")
            return json.dumps({"averagerating": averagerating, "numvotes": numvotes})


    except Exception as e:
        print(f"Error al interactuar con HBase: {e}")
        return None

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
            return jsonify(result)  
        else:
            return jsonify({"error": "No results found"}), 404  
    else:
        return jsonify({"error": "Missing movie_name parameter"}), 400  


#https://image.tmdb.org/t/p/original/oYuLEt3zVCKq57qu2F8dT7NIa6f.jpg

#https://image.tmdb.org/t/p/original/8ZTVqvKDQ8emSGUEMjsS4yHAwrp.jpg

# Iniciar el servidor
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
