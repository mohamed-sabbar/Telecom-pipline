FROM bitnami/spark:3.4.1

USER root

# Installer les dépendances système
RUN apt-get update && \
    apt-get install -y wget libnss3 && \
    useradd -u 1001 -m spark-user

# Créer le dossier de configuration et fixer les permissions
RUN mkdir -p /opt/bitnami/spark/conf && \
    chown -R 1001:root /opt/bitnami/spark/conf && \
    chmod -R 775 /opt/bitnami/spark/conf

# Configuration sécurité Hadoop
RUN echo "export HADOOP_OPTS=\"-Djava.security.krb5.realm= -Djava.security.krb5.kdc=\"" >> /opt/bitnami/spark/conf/spark-env.sh && \
    echo "spark.hadoop.fs.defaultFS=file:///" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    chown 1001:root /opt/bitnami/spark/conf/* && \
    chmod 664 /opt/bitnami/spark/conf/*

# Télécharger les dépendances Kafka et PostgreSQL
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar -P /opt/bitnami/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar -P /opt/bitnami/spark/jars/ && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P /opt/bitnami/spark/jars/ && \
    chown -R 1001:root /opt/bitnami/spark/jars && \
    chmod -R 775 /opt/bitnami/spark/jars
# Télécharger toutes les dépendances Kafka
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar -P /opt/bitnami/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar -P /opt/bitnami/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar -P /opt/bitnami/spark/jars/
RUN mkdir -p /opt/bitnami/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P /opt/bitnami/spark/jars/ && \
    chmod 644 /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar
USER 1001

WORKDIR /app
COPY rating_engine.py .

CMD ["/opt/bitnami/spark/bin/spark-submit", \
    "--conf", "spark.hadoop.ipc.client.fallback-to-simple-auth-allowed=true", \
    "--conf", "spark.jars.ivy=/tmp", \
    "rating_engine.py"]
