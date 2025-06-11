from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, lit, when, month, year
from pyspark.sql.types import DecimalType
import json
import datetime

# Configuration des règles de facturation
FREE_MINUTES = 100
VOICE_UNIT_COST = 0.01
VAT_RATE = 0.2
LOYALTY_DISCOUNT = 0.1
FIXED_PROMO = 20

def load_rated_events(spark):
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/telecom") \
        .option("dbtable", "telecom_data.rated_events") \
        .option("user", "admin") \
        .option("password", "admin") \
        .load() \
        .filter(col("rating_status") == "rated")

def compute_billing(df):
    df = df.withColumn("billing_month", month("event_time")) \
           .withColumn("billing_year", year("event_time"))

    grouped = df.groupBy("user_identifier", "billing_year", "billing_month", "record_type") \
                .agg(
                    _sum("duration_sec").alias("total_duration_sec"),
                    _sum("data_volume_mb").alias("total_data_mb"),
                    _sum("cost").alias("total_cost"),
                    count("*").alias("event_count")
                )

    billed = grouped.withColumn("adjusted_cost",
        when((col("record_type") == "voice") & (col("total_duration_sec") <= FREE_MINUTES * 60),
             lit(0.0)) \
        .when((col("record_type") == "voice") & (col("total_duration_sec") > FREE_MINUTES * 60),
             (col("total_duration_sec") - (FREE_MINUTES * 60)) * VOICE_UNIT_COST / 60) \
        .otherwise(col("total_cost"))
    )

    billed = billed.withColumn("discounted_cost",
        billed.adjusted_cost * (1 - LOYALTY_DISCOUNT) - FIXED_PROMO
    )

    billed = billed.withColumn("final_cost",
        (billed.discounted_cost * (1 + VAT_RATE)).cast(DecimalType(10, 2))
    )

    return billed

def generate_invoices(df):
    invoices = []
    for row in df.collect():
        invoice = {
            "customer_id": row["user_identifier"],
            "billing_period": f"{row['billing_year']}-{row['billing_month']:02d}",
            "service": row["record_type"],
            "total_events": row["event_count"],
            "final_cost_MAD": float(row["final_cost"]),
            "original_cost": float(row["total_cost"]),
            "adjusted_cost": float(row["adjusted_cost"]),
            "discounted_cost": float(row["discounted_cost"]),
            "generated_at": datetime.datetime.now().isoformat()
        }
        invoices.append(invoice)
    return invoices

def save_invoices_as_json(invoices, path="invoices"):
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"{path}/invoice_batch_{ts}.json", "w") as f:
        json.dump(invoices, f, indent=2)

def save_to_postgres(df):
    df_to_save = df.withColumnRenamed("user_identifier", "customer_id") \
                   .withColumnRenamed("record_type", "service") \
                   .withColumn("generated_at", lit(datetime.datetime.now()))

    df_to_save.select(
        "customer_id", "billing_year", "billing_month", "service", "event_count",
        "total_cost", "adjusted_cost", "discounted_cost", "final_cost", "generated_at"
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/telecom?currentSchema=telecom_data") \
        .option("dbtable", "telecom_data.billing_summary") \
        .option("user", "admin") \
        .option("password", "admin") \
        .mode("append") \
        .save()

# === Main ===
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("BillingEngineBatch") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .getOrCreate()

    rated_df = load_rated_events(spark)
    billed_df = compute_billing(rated_df)

    save_to_postgres(billed_df)

    invoices = generate_invoices(billed_df)
    save_invoices_as_json(invoices)

    spark.stop()
