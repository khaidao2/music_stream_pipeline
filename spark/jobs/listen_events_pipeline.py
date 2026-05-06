import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, BooleanType
import json

KAFKA_BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "music_kafka:29092")
GCS_BUCKET = "music-stream-data-lake-realestate-492305"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def load_schema(topic_name):
    path = os.path.join(BASE_DIR, "schemas", f"{topic_name}.avsc")
    if not os.path.exists(path):
        path = f"/opt/spark/schemas/{topic_name}.avsc"
        
    with open(path, 'r') as f:
        schema_dict = json.load(f)
    type_map = {
        "string": StringType(), 
        "long": LongType(), 
        "int": IntegerType(), 
        "double": DoubleType(), 
        "boolean": BooleanType()
    }
    fields = []
    for f in schema_dict['fields']:
        # Extract the actual type from Avro schema array (e.g., ["null", "double"] -> "double")
        field_type = f['type']
        if isinstance(field_type, list):
            actual_type = next((t for t in field_type if t != "null"), "string")
        else:
            actual_type = field_type
            
        spark_type = type_map.get(actual_type, StringType())
        fields.append(StructField(f['name'], spark_type, True))
        
    return StructType(fields)

def process_topic(spark, topic_name):
    schema = load_schema(topic_name)
    
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(from_unixtime(col("ts") / 1000))) \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", dayofmonth(col("event_timestamp"))) \
        .withColumn("hour", hour(col("event_timestamp")))

    output_path = f"gs://{GCS_BUCKET}/raw/{topic_name}"
    checkpoint_path = f"gs://{GCS_BUCKET}/checkpoints/{topic_name}"

    print(f"Starting stream for {topic_name} -> {output_path}")

    return df_parsed.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day") \
        .start()

def main():
    # Sử dụng file JAR Shaded cục bộ để tránh lỗi Guava
    spark = SparkSession.builder \
        .appName("MusicStream_to_GCS_Pipeline") \
        .config("spark.jars", "/opt/spark/libs/gcs-connector.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.abstract_gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    topics = ["listen_events", "auth_events", "page_view_events"]
    queries = [process_topic(spark, t) for t in topics]

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
