FROM python:3.9-slim

WORKDIR /app

# Installation des dépendances avec nettoyage automatique
RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_generator.py .

CMD ["python", "data_generator.py"]
