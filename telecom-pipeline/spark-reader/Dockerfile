FROM bitnami/spark:latest

USER root

# Installer cron
RUN apt-get update && apt-get install -y cron

# Copier le script PySpark
COPY billing.py /app/billing.py

# Copier le driver JDBC
ADD https://jdbc.postgresql.org/download/postgresql-42.5.4.jar /opt/bitnami/spark/jars/postgresql-42.5.4.jar

# Copier le cronjob et script de démarrage
COPY cronjob /app/cronjob
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Définir le dossier de travail
WORKDIR /app

# Commande par défaut : lancer le cron
CMD ["/app/start.sh"]
