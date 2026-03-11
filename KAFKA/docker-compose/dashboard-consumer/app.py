from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import json
import threading
import time

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Almacenamiento en memoria de las últimas posiciones de los camiones
truck_positions = {}

def kafka_consumer_thread():
    """Thread que consume mensajes de Kafka y actualiza las posiciones"""
    max_retries = 20
    retry_interval = 5
    
    # Esperar a que Kafka esté disponible
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                'camiones-eventos',
                bootstrap_servers='kafka:9092',
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='dashboard-consumer-group'
            )
            print(" Dashboard Consumer conectado a Kafka")
            break
        except Exception as e:
            print(f" Kafka no disponible (intento {attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_interval)
    else:
        raise RuntimeError("No se pudo conectar a Kafka después de varios intentos.")
    
    # Consumir mensajes continuamente
    for message in consumer:
        data = message.value
        camion_id = data['camion_id']
        
        # Actualizar posición del camión
        truck_positions[camion_id] = {
            'camion_id': camion_id,
            'lat': data['ubicacion']['lat'],
            'lon': data['ubicacion']['lon'],
            'velocidad': data['velocidad_kmh'],
            'temperatura': data['temperatura_motor'],
            'timestamp': data['timestamp']
        }
        
        # Emitir actualización a todos los clientes conectados
        socketio.emit('truck_update', truck_positions[camion_id])
        print(f"📍 Actualización: {camion_id} - Lat: {data['ubicacion']['lat']}, Lon: {data['ubicacion']['lon']}")

@app.route('/')
def index():
    """Página principal con el mapa"""
    return render_template('index.html')

@app.route('/api/trucks')
def get_trucks():
    """API para obtener todas las posiciones actuales"""
    return jsonify(list(truck_positions.values()))

if __name__ == '__main__':
    # Iniciar el thread del consumer de Kafka
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    print(" Dashboard iniciado en http://localhost:5000")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
