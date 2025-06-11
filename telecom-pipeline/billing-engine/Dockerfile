FROM bitnami/spark:3.4.1

USER root

# Dependencies et configuration de sécurité
RUN apt-get update && \
    apt-get install -y wget libnss3 && \
    useradd -u 1001 -m spark-user

# Configuration Hadoop/Kerberos
RUN echo "spark.hadoop.fs.defaultFS=file:///" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.hadoop.security.authentication=simple" >> /opt/bitnami/spark/conf/spark-defaults.conf

# Téléchargement des jars
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P /opt/bitnami/spark/jars/ && \
    chmod 644 /opt/bitnami/spark/jars/*.jar

USER 1001

WORKDIR /app
COPY billing_engine.py .

CMD ["/opt/bitnami/spark/bin/spark-submit", "--conf", "spark.hadoop.ipc.client.fallback-to-simple-auth-allowed=true", "billing_engine.py"]
