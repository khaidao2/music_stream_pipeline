import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, year, month, dayofmonth, hour, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, BooleanType
import json

KAFKA_BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "music_kafka:29092")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "music-stream-data-lake-realestate-492305")
BASE_DIR = "/opt/spark"

LOG_PATH = f"gs://{GCS_BUCKET}/logs/pipeline"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class GCSLogHandler(logging.Handler):
    def __init__(self, spark, log_path):
        super().__init__()
        self.spark = spark
        self.log_path = log_path
        self.records = []

    def emit(self, record):
        self.records.append({
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "message": self.format(record)
        })

    def flush_to_gcs(self, filename):
        if not self.records:
            return
        df = self.spark.createDataFrame(self.records)
        output_path = f"{self.log_path}/{filename}"
        df.coalesce(1).write.mode("append").parquet(output_path)
        self.records = []

gcs_handler = None

def setup_logging(spark):
    global gcs_handler
    gcs_handler = GCSLogHandler(spark, LOG_PATH)
    gcs_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    root_logger = logging.getLogger()
    root_logger.addHandler(gcs_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    root_logger.addHandler(console_handler)

    root_logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    root_logger.info(f"GCS Bucket: {GCS_BUCKET}")
    root_logger.info(f"Log Path: {LOG_PATH}")

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

    logging.info(f"Starting stream for {topic_name} -> {output_path}")

    query = df_parsed.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="1 minute") \
        .start()

    return query


def write_logs_periodically(spark, batch_id):
    if gcs_handler and gcs_handler.records:
        log_df = spark.createDataFrame(gcs_handler.records)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_df.coalesce(1).write.mode("append").parquet(f"{LOG_PATH}/batch_{batch_id}_{timestamp}.parquet")
        logging.info(f"Flushed {len(gcs_handler.records)} log records to GCS")
        gcs_handler.records = []


def main():
    spark = SparkSession.builder \
        .appName("MusicStream_to_GCS_Pipeline") \
        .config("spark.jars", "/opt/spark/libs/gcs-connector.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.abstract_gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    setup_logging(spark)
    logging.info("Spark session created successfully")

    topics = ["listen_events", "auth_events", "page_view_events"]
    queries = [process_topic(spark, t) for t in topics]

    batch_counter = 0
    while any(q.isActive for q in queries):
        import time
        time.sleep(60)
        batch_counter += 1
        write_logs_periodically(spark, batch_counter)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
