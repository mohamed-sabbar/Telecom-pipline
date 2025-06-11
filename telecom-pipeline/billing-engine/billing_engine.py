from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, current_date, when
from pyspark.sql.types import DecimalType
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BillingEngine")

MAX_RETRIES = 5
RETRY_INTERVAL = 30

def connect_to_postgres(spark):
    """Connexion avec réessai à PostgreSQL"""
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Connexion à PostgreSQL (essai {attempt + 1})")
            return spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/telecom?currentSchema=telecom_data") \
                .option("dbtable", "telecom_data.rated_events") \
                .option("user", "admin") \
                .option("password", "admin") \
                .load()
        except Exception as e:
            logger.error(f"Échec connexion PostgreSQL: {str(e)}")
            time.sleep(RETRY_INTERVAL)
    raise ConnectionError("Connexion PostgreSQL impossible")

def generate_invoices(df):
    """Génération des factures avec gestion des erreurs"""
    try:
        logger.info("Génération des factures...")
        invoices = df.groupBy("caller_id").agg(
            sum(when(col("record_type") == "voice", col("cost"))).alias("voice_cost"),
            sum(when(col("record_type") == "sms", col("cost"))).alias("sms_cost"),
            sum(when(col("record_type") == "data", col("cost"))).alias("data_cost"),
            sum(col("cost")).alias("total_cost")
        ).withColumn("invoice_date", current_date())
        
        logger.info("Aperçu des factures générées :")
        invoices.show(5, truncate=False)
        return invoices
    except Exception as e:
        logger.error(f"Erreur lors de la génération : {str(e)}")
        raise

def write_invoices(df):
    """Écriture avec validation"""
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Écriture dans PostgreSQL (essai {attempt + 1})")
            df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/telecom") \
                .option("dbtable", "invoices") \
                .option("user", "admin") \
                .option("password", "admin") \
                .option("createTableColumnTypes", "total_cost DECIMAL(10,2)") \
                .mode("append") \
                .save()
            logger.info("Écriture réussie !")
            return
        except Exception as e:
            logger.error(f"Échec écriture : {str(e)}")
            time.sleep(RETRY_INTERVAL)
    raise Exception("Échec écriture après plusieurs tentatives")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("BillingEngine") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.hadoop.ipc.client.fallback-to-simple-auth-allowed", "true") \
        .getOrCreate()

    try:
        logger.info("=== Démarrage du Billing Engine ===")
        rated_events = connect_to_postgres(spark)
        invoices = generate_invoices(rated_events)
        write_invoices(invoices)
        logger.info("=== Traitement terminé avec succès ===")
    except Exception as e:
        logger.error(f"ERREUR CRITIQUE : {str(e)}")
        raise
    finally:
        spark.stop()
