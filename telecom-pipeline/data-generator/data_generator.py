import json
import random
import logging
from time import sleep
from datetime import datetime, timedelta
from collections import defaultdict
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuration de base
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DataGenerator")

# Paramètres métier
SERVICES_DIST = {'voice': 0.6, 'data': 0.3, 'sms': 0.1}
ERROR_RATE = 0.05
CITIES = ['ALHOCEIMA', 'IMZOUREN', 'NADOR', 'TANGER', 'RABAT']
TECH_MAPPING = {
    'voice': ['2G', '3G', '4G', '5G'],
    'sms': ['2G', '3G', '4G'],
    'data': ['3G', '4G', '5G', 'LTE']
}
TECH_PARAMS = {
    '2G': {'duration': (10, 3600), 'data_vol': (0, 0.1)},
    '3G': {'duration': (5, 1800), 'data_vol': (0.1, 10)},
    '4G': {'duration': (1, 1200), 'data_vol': (10, 500)},
    '5G': {'duration': (1, 600), 'data_vol': (100, 2000)},
    'LTE': {'duration': (1, 900), 'data_vol': (50, 1500)}
}

DUPLICATE_CACHE = defaultdict(list)
faker = Faker()

def retry_connection(func, max_retries=30, retry_interval=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            logger.warning(f"Tentative {i+1}/{max_retries} échouée: {str(e)}")
            sleep(retry_interval)
    raise ConnectionError(f"Échec après {max_retries} tentatives")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=10,
        retry_backoff_ms=5000,
        request_timeout_ms=60000,
        api_version=(2, 8, 1),
        compression_type='gzip'
    )

def generate_timestamp(out_of_order=False):
    ts = datetime.utcnow() - timedelta(seconds=random.randint(0, 86400))
    if out_of_order and random.random() < 0.02:
        ts -= timedelta(days=random.randint(1, 30))
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def generate_voice_record():
    tech = random.choice(TECH_MAPPING['voice'])
    return {
        "record_type": "voice",
        "timestamp": generate_timestamp(),
        "caller_id": f"2126{faker.numerify('########')}",
        "callee_id": f"2126{faker.numerify('########')}",
        "duration_sec": random.randint(TECH_PARAMS[tech]['duration'][0], TECH_PARAMS[tech]['duration'][1]),
        "cell_id": f"{random.choice(CITIES)}_{random.randint(1, 50)}",
        "technology": tech
    }

def generate_sms_record():
    tech = random.choice(TECH_MAPPING['sms'])
    return {
        "record_type": "sms",
        "timestamp": generate_timestamp(),
        "sender_id": f"2126{faker.numerify('########')}",
        "receiver_id": f"2126{faker.numerify('########')}",
        "cell_id": f"{random.choice(CITIES)}_{random.randint(1, 50)}",
        "technology": tech
    }

def generate_data_record():
    tech = random.choice(TECH_MAPPING['data'])
    return {
        "record_type": "data",
        "timestamp": generate_timestamp(),
        "user_id": f"2126{faker.numerify('########')}",
        "data_volume_mb": round(random.uniform(*TECH_PARAMS[tech]['data_vol']), 2),
        "session_duration_sec": random.randint(TECH_PARAMS[tech]['duration'][0], TECH_PARAMS[tech]['duration'][1]),
        "cell_id": f"{random.choice(CITIES)}_{random.randint(1, 50)}",
        "technology": tech
    }

def inject_anomalies(record):
    global DUPLICATE_CACHE

    if random.random() > ERROR_RATE:
        return [record]

    anomaly_type = random.choices(
        ['missing_field', 'corrupt_value', 'duplicate', 'out_of_order', 'invalid_service'],
        weights=[0.3, 0.3, 0.2, 0.1, 0.1]
    )[0]

    # Application des anomalies
    if anomaly_type == 'missing_field':
        fields = list(record.keys())
        fields.remove('record_type')
        if fields:
            record.pop(random.choice(fields))

    elif anomaly_type == 'corrupt_value':
        field = random.choice(list(record.keys()))
        if field in ['duration_sec', 'data_volume_mb']:
            record[field] = -abs(record[field])
        else:
            record[field] = "INVALID"

    elif anomaly_type == 'duplicate':
        DUPLICATE_CACHE[json.dumps(record, sort_keys=True)].append(record)
        return [record, record.copy()]

    elif anomaly_type == 'out_of_order':
        record['timestamp'] = generate_timestamp(out_of_order=True)

    elif anomaly_type == 'invalid_service':
        record['record_type'] = random.choice(['mms', 'fax', 'undefined'])
        record['technology'] = 'INVALID_TECH'

    return [record]

def generate_record():
    service = random.choices(
        list(SERVICES_DIST.keys()),
        weights=list(SERVICES_DIST.values())
    )[0]

    if service == 'voice':
        return generate_voice_record()
    elif service == 'sms':
        return generate_sms_record()
    return generate_data_record()

if __name__ == "__main__":
    producer = retry_connection(create_producer)
    logger.info("Connecté à Kafka avec succès")

    try:
        while True:
            record = generate_record()
            records = inject_anomalies(record)

            for r in records:
                producer.send('cdr-topic', value=r).add_errback(
                    lambda e: logger.error(f"Erreur d'envoi: {str(e)}")
                )

            if DUPLICATE_CACHE and random.random() < 0.05:
                duplicate = random.choice(list(DUPLICATE_CACHE.values()))[0]
                producer.send('cdr-topic', value=duplicate)

            sleep(0.2)

    except KeyboardInterrupt:
        logger.info("Arrêt du générateur")
    except Exception as e:
        logger.error(f"Erreur critique: {str(e)}")
    finally:
        producer.flush()
        producer.close()
