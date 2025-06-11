FROM bitnami/spark:3.4.1

USER root


# 1. Installer les dépendances système
RUN apt-get update && \
    apt-get install -y wget libnss3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Télécharger les dépendances Kafka avec les versions compatibles
RUN mkdir -p /opt/bitnami/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar -P /opt/bitnami/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar -P /opt/bitnami/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar -P /opt/bitnami/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P /opt/bitnami/spark/jars/ && \
    chmod 644 /opt/bitnami/spark/jars/*.jar

# 3. Configuration des permissions
RUN chown -R 1001:root /opt/bitnami/spark/jars && \
    chmod -R 775 /opt/bitnami/spark/jars

USER 1001

WORKDIR /app
COPY mediation_streaming.py .

CMD ["/opt/bitnami/spark/bin/spark-submit", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1", \
    "--conf", "spark.jars.ivy=/tmp", \
    "mediation_streaming.py"]
