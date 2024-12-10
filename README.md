# Proyecto Final: Ciencia de Datos Escalable

Este proyecto corresponde al curso **Ciencia de Datos Escalable**, parte de la Maestría en Ciencia de Datos de la **Universidad Católica San Pablo**.

Autor: **Gonzalo Emiliano Quispe Huanca**

Este documento detalla los pasos necesarios para implementar el proyecto final, incluyendo la configuración de un **broker de Kafka** en una instancia EC2 de Amazon Web Services (AWS).  

### Variables Importantes:
Debe actualizar estas variables en los scripts de hadoop.py, backend.py, consumer.py e index.html antes de continuar con el proceso.
- `{KAFKA_PRODUCER_HOST}`: Corresponde al DNS público del broker de Kafka.
- `{EMR_HOST}`: Corresponde al DNS público del EMR Amazon con Hadoop.

## Ejecución del Broker de Kafka

### 1. Crear una instancia EC2 en AWS:  
   - Sistema operativo: **Ubuntu**  
   - Tipo de instancia: **m5.large**  
   - Usuario por defecto: `ubuntu`

### 2. Configurar el puerto de red:  
   Asegúrate de abrir el puerto **9092 (TCP)** en las reglas de seguridad de la instancia para permitir conexiones al broker de Kafka.

### 3. Instalar Apache Kafka:  
    ```bash
    sudo apt-get update
    wget https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
    tar -xzf kafka_2.13-3.9.0.tgz
    cd kafka_2.13-3.9.0/
    ```
### 4. Ejecutar nano config/server.properties y actualizar:
    - listeners=PLAINTEXT://0.0.0.0:9092
    - advertised.listeners=PLAINTEXT://{KAFKA_PRODUCER_HOST}:9092
### 5. Ejecutar los caomandos para instalar java y levantar kafka :
    ```bash
    sudo apt-get -y install openjdk-8-jdk
    bin/zookeeper-server-start.sh config/zookeeper.properties &
    bin/kafka-server-start.sh config/server.properties &
    ```
### 6. Crear el topico 'movie_vote' en kafka:
    ```bash
    bin/kafka-topics.sh --create --topic movie_vote --bootstrap-server localhost:9092
    ```
## Ejecución de Kafka REST
Este proyecto incluye una aplicación web que producirá mensajes para el broker de Kafka. Para permitir el envío directo de mensajes desde JavaScript, se utilizará Kafka REST.

### 1. Instalación de Kafka REST
    ```bash
    curl -O https://packages.confluent.io/archive/7.3/confluent-community-7.3.0.tar.gz
    tar -xvzf confluent-community-7.3.0.tar.gz
    cd confluent-7.3.0
    sudo apt-get install confluent-kafka-rest
    ```

### 2. Configuración de kafka-rest.properties
    ```bash
    cd /home/ubuntu/kafka_2.13-3.9.0/confluent-7.3.0/etc/kafka-rest/
    nano /home/ubuntu/kafka_2.13-3.9.0/confluent-7.3.0/etc/kafka-rest/kafka-rest.properties
    ```
### 3. Actualiza las siguientes líneas:
   - bootstrap.servers=PLAINTEXT://{KAFKA_PRODUCER_HOST}:9092
   - access.control.allow.origin=*
   - access.control.allow.methods=GET,POST,OPTIONS

### 4. Ejecutar Kafka REST
    ```bash
    cd /home/ubuntu/kafka_2.13-3.9.0/confluent-7.3.0/bin
    ./kafka-rest-start /home/ubuntu/kafka_2.13-3.9.0/confluent-7.3.0/etc/kafka-rest/kafka-rest.properties
    ```

## Creación y Ejecución de Hadoop
Esta sección detalla los pasos necesarios para configurar un clúster **Hadoop** en Amazon EMR y crear un servicio para procesar datos utilizando un script de Python.

### 1. Crear un clúster EMR en Amazon
Configura un clúster de **Elastic MapReduce (EMR)** en Amazon para manejar Hadoop, HBase, Ozzie, Hue, Phoenix, Spark, Ozzie, Livy.

### 2. Probar el uso de HBase usando los siguientes comandos:
    ```bash
    hbase shell
    create 'movie_vote', 'data'
    list
    ```

### 3. Otorga los permisos necesarios a las carpetas dentro del sistema de archivos HDFS:
    ```bash
    sudo -u hdfs bash
    hdfs dfs -mkdir /user
    hdfs dfs -mkdir /user/latest
    hdfs dfs -chown -R ec2-user:ec2-user /user
    hdfs dfs -chown ec2-user:ec2-user /user/latest
    hdfs dfs -chmod -R 770 /user
    hdfs dfs -chown -R hbase:hbase /user/hbase
    hdfs dfs -ls -R /user/hbase
    hdfs dfs -chmod -R 755 /user/hbase
    exit 
    sudo -u hdfs hdfs dfs -chmod 755 /user
    sudo -u hdfs hdfs dfs -chmod 775 /user
    sudo -u hdfs hdfs dfs -chown -R ec2-user:ec2-user /user
    sudo -u hdfs hdfs dfs -chmod -R 775 /user
    hdfs dfs -ls /user
    ```

### 4. Crea el script de procesamiento para Hadoop y edítalo con el siguiente comando. Luego copia el contenido de hadoop.py:
    ```bash
    nano hadoop.py
    ```
### 5. Crear el servicio de Linux para hadoop.py
    ```bash
    sudo nano /etc/systemd/system/hadoop_service.service
    ```

### 6.Pega el siguiente contenido en el archivo:

[Unit]
Description=Servicio Python para procesamiento Hadoop en EMR
After=network.target

[Service]
ExecStart=/usr/bin/python3 /home/ec2-user/hadoop.py
Restart=always
User=ec2-user

[Install]
WantedBy=multi-user.target

### 7. Inicializar el servicio
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable hadoop_service
    sudo systemctl start hadoop_service
    sudo systemctl status hadoop_service
    ```

## Creación y Ejecución del Servicio BACKend
Este servicio permite realizar consultas HTTP a los archivos almacenados en **HDFS** utilizando un servidor **Flask** y **PySpark**.

### 1. Configurar puertos
- **8020 (TCP)**: Para acceso a HDFS.  
- **5000 (TCP)**: Para el servidor Flask.

### 2. Instalar las dependencias
    ```bash
    pip install flask pyspark
    ```
### 3. Crear el archivo backend.py. Luego copia el contenido de backend.py
    ```bash
    sudo nano backend.py
    ```
### 4. Configurar el servicio de Linux para backend.py
    ```bash
    sudo nano /etc/systemd/system/backend_service.service
    ```
### 5. Copiar el contenido
    [Unit]
    Description=Servicio Flask para consultas spark
    After=network.target

    [Service]
    ExecStart=/usr/bin/python3 /home/ec2-user/backend.py
    Restart=always
    User=ec2-user

    [Install]
    WantedBy=multi-user.target

### 6. Inicializar el servicio
    ```bash
    sudo systemctl daemon-reload                                                    
    sudo systemctl enable backend_service
    sudo systemctl start backend_service
    sudo systemctl status backend_service
    ```

## Creación y Ejecución del Servicio Consumer
Este servicio se encargará de leer los mensajes del **broker de Kafka** y actualizar la información de las películas en una tabla de **HBase** con los ratings correspondientes.

### 1. Instalación de dependencias
Ejecuta los siguientes comandos para instalar las dependencias necesarias:  
    ```bash
    sudo yum install -y python3-devel
    sudo yum groupinstall -y "Development Tools"
    pip3 install thriftpy2 --no-cache-dir
    pip install confluent_kafka
    pip install hbase-python
    pip install happybase
    ```
### 2. Crear el archivo consumer.py. Luego copie el contenido de consumer.py
    ```bash
    nano consumer.py
    ```
### 4. Configurar el servicio de Linux para consumer.py
    ```bash
    sudo nano /etc/systemd/system/consumer_service.service
    ```
### 5. Copiar el contenido
    [Unit]
    Description=Servicio Python para consumir evetos kafka
    After=network.target

    [Service]
    ExecStart=/usr/bin/python3 /home/ec2-user/consumer.py
    Restart=always
    User=ec2-user

    [Install]
    WantedBy=multi-user.target

### 6. Inicializar el servicio
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable consumer_service
    sudo systemctl start consumer_service
    sudo systemctl status consumer_service
    ```