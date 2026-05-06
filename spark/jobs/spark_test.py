from pyspark.sql.functions import window, col
import os
import sys

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, BooleanType
import json
import socket

"""
IF ERROR RUN:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1 \
 spark/jobs/spark_test.py
"""

def get_kafka_broker():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect(('localhost', 9094))
        s.close()
        return "localhost:9094"
    except:
        return "music_kafka:29092"

KAFKA_BOOTSTRAP_SERVERS = get_kafka_broker()
TOPIC = "listen_events"

# Lấy đường dẫn schema
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMA_PATH = os.path.join(BASE_DIR, "schemas", "listen_events.avsc")

def avro_to_spark_schema(avro_schema_path):
    with open(avro_schema_path, 'r') as f:
        schema_dict = json.load(f)
    type_map = {"string": StringType(), "long": LongType(), "int": IntegerType(), "double": DoubleType(), "boolean": BooleanType()}
    fields = []
    for field in schema_dict['fields']:
        f_type = field['type']
        actual_type = [t for t in f_type if t != "null"][0] if isinstance(f_type, list) else f_type
        fields.append(StructField(field['name'], type_map.get(actual_type, StringType()), True))
    fields.append(StructField("_corrupt_record", StringType(), True))
    return StructType(fields)

def main():
    spark = SparkSession.builder \
        .appName("MusicStreamStreamingProcessor") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"\n--- Application Info ---")
    print(f"Loading schema from: {SCHEMA_PATH}")
    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"------------------------\n")

    spark_schema = avro_to_spark_schema(SCHEMA_PATH)

    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .withColumn("data", from_json(col("json_payload"), spark_schema, {"mode": "PERMISSIVE"}))

    df_flat = df_parsed.select("data.*")
    df_flat = df_flat.withColumn("event_time", to_timestamp(from_unixtime(col("ts") / 1000)))
    df_agg=(df_flat.withWatermark("event_time","10 minutes").groupBy(
        window(col("event_time"), "5 minutes"),
        col("artist"),
        col("song")
    ).agg({"song":"count", "duration":"sum"}))

    query = df_agg.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()