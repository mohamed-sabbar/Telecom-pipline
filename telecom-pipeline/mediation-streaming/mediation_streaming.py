from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("MediationStreaming")

# Schéma d'entrée aligné avec le Data Generator
input_schema = StructType([
    StructField("record_type", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("caller_id", StringType(), True),
    StructField("callee_id", StringType(), True),
    StructField("sender_id", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("session_duration_sec", IntegerType(), True),
    StructField("data_volume_mb", DoubleType(), True),
    StructField("cell_id", StringType(), False),
    StructField("technology", StringType(), False)
])

VALID_TECHNOLOGIES = {
    'voice': ['2G', '3G', '4G', '5G'],
    'sms': ['2G', '3G', '4G'],
    'data': ['3G', '4G', '5G', 'LTE']
}

def configure_spark():
    return SparkSession.builder \
        .appName("TelecomMediation") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

def parse_timestamp(df):
    """Gestion robuste des formats de date"""
    return df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd['T'HH:mm:ss[.SSS]['Z']]")
    )


def validate_records(df):
    """Validation renforcée avec vérification des champs obligatoires"""
    validation_condition = (
        col("record_type").isin(["voice", "sms", "data"]) &
        col("timestamp").isNotNull() &
        col("cell_id").isNotNull() &  # Contrôle renforcé
        col("technology").isNotNull() &
        (
            ((col("record_type") == "voice") &
            col("caller_id").isNotNull() &
            col("callee_id").isNotNull() &
            (col("duration_sec") > 0) &
            col("duration_sec").isNotNull()  # Vérification existence
        ) |
        ((col("record_type") == "sms") &
         col("sender_id").isNotNull() &
         col("receiver_id").isNotNull()) |
        ((col("record_type") == "data") &
         col("user_id").isNotNull() &
         (col("data_volume_mb") > 0) &
         (col("session_duration_sec") > 0) &
         col("data_volume_mb").isNotNull() &  # Vérification existence
         col("session_duration_sec").isNotNull())
        ) &
        col("technology").isin(
            [tech for sublist in VALID_TECHNOLOGIES.values() for tech in sublist]
        )
    )
    return df.withColumn("is_valid", validation_condition)


def process_stream(spark):
    try:
        # Lecture depuis Kafka
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "cdr-topic") \
            .option("startingOffsets", "earliest") \
            .load()

        # Parsing des données
        parsed_df = kafka_stream.select(
            from_json(col("value").cast("string"), input_schema).alias("data")
        ).select("data.*")

        # Traitement des timestamps
        parsed_df = parse_timestamp(parsed_df)

        # Validation des enregistrements
        validated_df = validate_records(parsed_df)

        # Séparation des flux
        valid_df = validated_df.filter(col("is_valid") == True)
        invalid_df = validated_df.filter(col("is_valid") == False)

        # Dédoublonnage avec fenêtre glissante
        deduplicated_df = valid_df \
            .withWatermark("timestamp", "5 minutes") \
            .dropDuplicates(["record_type", "caller_id", "timestamp"])

        # Écriture des résultats
        query_clean = deduplicated_df.drop("is_valid").selectExpr("to_json(struct(*)) AS value")\
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "clean-cdr-topic") \
            .option("checkpointLocation", "/tmp/checkpoint-clean") \
            .start()

        query_errors = invalid_df.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "dead-letter-topic") \
            .option("checkpointLocation", "/tmp/checkpoint-errors") \
            .start()

        logger.info("Pipeline opérationnel - Traitement en cours...")
        query_clean.awaitTermination()
        query_errors.awaitTermination()

    except Exception as e:
        logger.error(f"ERREUR CRITIQUE: {str(e)}")
        raise

if __name__ == "__main__":
    spark = configure_spark()
    process_stream(spark)
    spark.stop()