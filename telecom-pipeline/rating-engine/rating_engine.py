from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, expr, from_json, create_map
from pyspark.sql.types import *
import logging
import time
from pyspark.sql.streaming import StreamingQuery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("RatingEngine")

# Configuration
MAX_RETRIES = 10
RETRY_INTERVAL = 30
KAFKA_TOPIC = "clean-cdr-topic"
POSTGRES_TABLE = "telecom_data.rated_events"

input_schema = StructType([
    StructField("record_type", StringType(), False),
    StructField("timestamp", TimestampType(), False),
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

PRICING = {
    'voice': {'base': 0.01, 'peak_hours': 0.015},
    'sms': {'flat': 0.05},
    'data': {
        'rates': {'3G': 0.08, '4G': 0.10, '5G': 0.12, 'LTE': 0.09},
        'volume_discount': {'threshold': 1000, 'discount': 0.15}
    }
}

def check_kafka_topic(spark):
    """Vérifie l'existence du topic Kafka avec l'API AdminClient"""
    try:
        props = spark._jvm.java.util.Properties()
        props.put("bootstrap.servers", "kafka:9092")
        props.put("client.id", "spark-admin-client")

        admin_client = spark._jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
        return admin_client.listTopics().names().get().contains(KAFKA_TOPIC)
    except Exception as e:
        logger.error(f"Erreur vérification topic: {str(e)}")
        return False

def connect_to_kafka(spark):
    """Connexion résiliente à Kafka avec vérification du topic"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if not check_kafka_topic(spark):
                raise Exception(f"Topic {KAFKA_TOPIC} non trouvé")

            logger.info(f"Connexion à Kafka (tentative {attempt}/{MAX_RETRIES})")

            return spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load()

        except Exception as e:
            logger.error(f"Échec connexion Kafka: {str(e)}")
            if attempt == MAX_RETRIES:
                raise
            sleep_time = RETRY_INTERVAL * attempt
            logger.info(f"Nouvelle tentative dans {sleep_time}s...")
            time.sleep(sleep_time)



def calculate_cost(df):
    """Calcul des coûts avec gestion des valeurs manquantes"""
    data_rates = create_map(
        [lit(e) for k, v in PRICING['data']['rates'].items() for e in (k, v)]
    )

    return df.withColumn("cost",
        when(col("record_type") == "voice",
            when(
                (col("duration_sec").isNotNull()) & (col("duration_sec") > 0),
                expr("""
                    CASE
                        WHEN hour(timestamp) BETWEEN 8 AND 11 THEN duration_sec * 0.015
                        WHEN hour(timestamp) BETWEEN 17 AND 19 THEN duration_sec * 0.015
                        ELSE duration_sec * 0.01
                    END
                """).cast(DecimalType(10,4))
            ).otherwise(lit(0.0))
        ).when(col("record_type") == "sms",
            lit(PRICING['sms']['flat']).cast(DecimalType(10,4))
        ).when(col("record_type") == "data",
            when(
                (col("data_volume_mb").isNotNull()) & (col("data_volume_mb") > 0),
                when(
                    col("data_volume_mb") > PRICING['data']['volume_discount']['threshold'],
                    col("data_volume_mb") * data_rates[col("technology")] * (1 - PRICING['data']['volume_discount']['discount'])
                ).otherwise(
                    col("data_volume_mb") * data_rates[col("technology")]
                )
            ).otherwise(lit(0.0))
        ).otherwise(lit(0.0))
    )




def write_to_postgres(batch_df, batch_id):
    """Écriture batch sécurisée avec valeurs par défaut"""
    if not batch_df.isEmpty():
        # Remplacement des valeurs manquantes et négatives
        safe_batch = batch_df.fillna({
            'duration_sec': 0,
            'data_volume_mb': 0.0
        }).withColumn("duration_sec",
            when(col("duration_sec") < 0, 0).otherwise(col("duration_sec"))
        ).withColumn("data_volume_mb",
            when(col("data_volume_mb") < 0, 0.0).otherwise(col("data_volume_mb"))
        ).filter(
            (col("cell_id").isNotNull()) &  # Filtre crucial
            (col("user_identifier").isNotNull()) &
            (col("cost") >= 0)
        )
        if safe_batch.isEmpty():
            logger.info(f"Batch #{batch_id} filtré (valeurs invalides)")
            return

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                safe_batch.write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://postgres:5432/telecom?currentSchema=telecom_data") \
                    .option("dbtable", "telecom_data.rated_events") \
                    .option("user", "admin") \
                    .option("password", "admin") \
                    .mode("append") \
                    .save()
                break
            except Exception as e:
                logger.error(f"Échec écriture batch #{batch_id} (tentative {attempt}): {str(e)}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_INTERVAL)



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TelecomRatingEngine") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.5.4") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint-rating") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    try:
        logger.info("Initialisation du moteur de tarification...")

        # Connexion sécurisée à Kafka
        kafka_stream = connect_to_kafka(spark)

        # Pipeline de traitement
        processed_stream = (
            kafka_stream
            .select(from_json(col("value").cast("string"), input_schema).alias("data"))
            .select("data.*")
            .transform(calculate_cost)
            .withColumn("user_identifier",
             coalesce(col("caller_id"), col("sender_id"), col("user_id"))
                )
            .withColumn("rating_status",
             when(col("cost") > 0, lit("rated")).otherwise(lit("unrated"))
                )
            .select(
              col("record_type"),
              col("timestamp").alias("event_time"),
              "user_identifier",
              "cell_id",
              "technology",
              "cost",
              "duration_sec",
              "data_volume_mb",
              "rating_status"
            )
        )

        deduplicated_stream = processed_stream \
           .withWatermark("event_time", "10 minutes") \
           .dropDuplicates(["record_type", "user_identifier", "event_time", "cell_id"])

        # Écriture continue avec gestion d'erreur
        query = deduplicated_stream.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("update") \
            .start()

        logger.info("Démarrage du traitement streaming...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"ERREUR CRITIQUE: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Arrêt du moteur de tarification")


