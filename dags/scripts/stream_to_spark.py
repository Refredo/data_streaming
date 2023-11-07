import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,StringType
from pyspark.sql.functions import from_json,col

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

logger = logging.getLogger("spark_streaming")

def create_spark_session():
    try:
        spark = SparkSession \
                .builder \
                .appName("SparkStreaming") \
                .config("spark.cassandra.connection.host", "cassandra") \
                .config("spark.cassandra.connection.port","9042")\
                .config("spark.cassandra.auth.username", "cassandra") \
                .config("spark.cassandra.auth.password", "cassandra") \
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully')
    except Exception as e:
        logging.info(f"Couldn't create Spark session: {e}")

    return spark

# def create_initial_dataframe(spark_session):
#     try:
#         df = spark_session \
#             .readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'kafka:9092') \
#             .option('subscribe', 'random_names') \
#             .option('delimiter', ',') \
#             .option('startingOffsets', 'earliest') \
#             .load()
#         logging.info('Initial dataframe created successfully')
#     except Exception as e:
#         logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

#     return df

def create_initial_dataframe(spark_session):

    try:
        # Gets the streaming data from topic random_names
        df = spark_session \
              .readStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", "kafka:9092") \
              .option("subscribe", "random_namess") \
              .option("delimeter",",") \
              .option("startingOffsets", "earliest") \
              .load()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    df.printSchema()
    
    return df


def create_final_dataframe(df, spark_session):

    schema = StructType([
        StructField('full_name', StringType(), False),
        StructField("gender",StringType(),False),
        StructField("location",StringType(),False),
        StructField("city",StringType(),False),
        StructField("country",StringType(),False),
        StructField("postcode",IntegerType(),False),
        StructField("latitude",FloatType(),False),
        StructField("longitude",FloatType(),False),
        StructField("email",StringType(),False)
    ])

    df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col('value'), schema).alias('data')).select('data.*')
    print(f'this is my df: {df1}')
    return df1

def start_streaming(df):
    logging.info('Streaming is being started...')

    query = (df.writeStream
                .format('org.apache.spark.sql.cassandra')
                .outputMode('append')
                .option("checkpointLocation", "/tmp/checkpoints")
                .options(table='random_names', keyspace='spark_streaming') \
                .start())

    return query.awaitTermination() 


def streaming_to_cassandra():
    spark = create_spark_session()    
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)


if __name__ == "__main__":
    streaming_to_cassandra()