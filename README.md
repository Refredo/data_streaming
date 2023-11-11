# data_streaming
Getting random names from the API, then sends this data to Kafka topic every period of time  by using Airflow. Then each message is fetched by a Kafka consumer with using Spark and then writing data to a Cassandra table.

----------
First step
----------
create necessary directories by using this command
- mkdir logs config plugins

-----------
Second step
-----------
build extended Airflow image by command
- docker build . --tag extending_airflow:latest

----------
Third step
----------
Then you should to customize cassandra database
Firsty, you need run this command - docker-compose up
Then, run all this commands below sequentially in another terminal
- docker exec -it cassandra /bin/bash
- cqlsh -u cassandra -p cassandra
- CREATE KEYSPACE spark_streaming WITH replication = {'class':'SimpleStrategy','replication_factor':1};
- CREATE TABLE spark_streaming.random_names(full_name text primary key, gender text, location text, city text, country text, postcode int, latitude float, longitude float, email text);
- DESCRIBE spark_streaming.random_names;

-----------
Fourth step
-----------
For starting Spark Streaming, run commands below
- docker cp dags/scripts/stream_to_spark.py spark2:/opt/bitnami/spark/
- docker exec -it spark2 /bin/bash
- spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 stream_to_spark.py
