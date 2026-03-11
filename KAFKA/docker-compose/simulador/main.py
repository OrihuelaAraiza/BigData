import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

MAX_RETRIES = 20
RETRY_INTERVAL = 5  


NOMBRE_ALUMNO = "JUAN PABLO ORIHUELA ARAIZA"
MATRICULA = 256860 


random.seed(MATRICULA) 


for attempt in range(MAX_RETRIES):
    try:
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        print("Conectado a Kafka")
        break
    except NoBrokersAvailable:
        print(f"Kafka no disponible (intento {attempt + 1}/{MAX_RETRIES}). Esperando {RETRY_INTERVAL}s...")
        time.sleep(RETRY_INTERVAL)
else:
    raise RuntimeError("No se pudo conectar a Kafka después de varios intentos.")


KAFKA_SERVER = 'kafka:9092'
TOPIC = 'camiones-eventos'

iniciales = "".join([n[0] for n in NOMBRE_ALUMNO.split()])
camiones = [f"{iniciales}-{i}" for i in range(1, 11)]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



def generar_evento():
    camion_id = random.choice(camiones)
    evento = {
        'camion_id': camion_id,
        'timestamp': time.time(),
        'velocidad_kmh': round(random.uniform(30, 120), 2),
        'temperatura_motor': round(random.uniform(70, 110), 1),
        'ubicacion': {
            'lat': round(random.uniform(19.0, 19.6), 6),
            'lon': round(random.uniform(-99.3, -99.1), 6)
        }
    }
    return evento

print("Iniciando simulador de camiones...")
while True:
    evento = generar_evento()
    key = evento['camion_id'].encode('utf-8')
    producer.send(TOPIC, key=key, value=evento)
    print(f" Evento enviado: {evento}")
    time.sleep(2)
