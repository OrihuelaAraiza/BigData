from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import json
import threading
import time
from datetime import datetime
from collections import deque

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Configuración de alertas
TEMP_THRESHOLD = 95.0  # Temperatura de alerta en °C
TEMP_CRITICAL = 105.0  # Temperatura crítica en °C

# Almacenar alertas recientes (máximo 100)
alerts = deque(maxlen=100)
alert_counter = 0

# Estadísticas por camión
truck_stats = {}

def kafka_consumer_thread():
    """Thread que consume mensajes de Kafka y genera alertas"""
    global alert_counter
    
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
                group_id='alert-consumer-group'
            )
            print("Alert Consumer conectado a Kafka")
            break
        except Exception as e:
            print(f"Kafka no disponible (intento {attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_interval)
    else:
        raise RuntimeError("No se pudo conectar a Kafka después de varios intentos.")
    
    # Consumir mensajes continuamente
    for message in consumer:
        data = message.value
        camion_id = data['camion_id']
        temperatura = data['temperatura_motor']
        
        # Actualizar estadísticas del camión
        if camion_id not in truck_stats:
            truck_stats[camion_id] = {
                'total_events': 0,
                'alerts': 0,
                'critical_alerts': 0,
                'max_temp': 0,
                'last_temp': 0
            }
        
        stats = truck_stats[camion_id]
        stats['total_events'] += 1
        stats['last_temp'] = temperatura
        stats['max_temp'] = max(stats['max_temp'], temperatura)
        
        # Verificar si hay que generar una alerta
        alert_type = None
        if temperatura >= TEMP_CRITICAL:
            alert_type = 'CRÍTICA'
            stats['critical_alerts'] += 1
            stats['alerts'] += 1
        elif temperatura >= TEMP_THRESHOLD:
            alert_type = 'ADVERTENCIA'
            stats['alerts'] += 1
        
        if alert_type:
            alert_counter += 1
            alert = {
                'id': alert_counter,
                'timestamp': datetime.now().isoformat(),
                'camion_id': camion_id,
                'tipo': alert_type,
                'temperatura': temperatura,
                'velocidad': data['velocidad_kmh'],
                'ubicacion': data['ubicacion']
            }
            alerts.appendleft(alert)  # Agregar al inicio
            
            # Emitir alerta a todos los clientes conectados
            socketio.emit('new_alert', alert)
            
            # Emitir estadísticas actualizadas
            socketio.emit('stats_update', get_statistics())
            
            icon = '🔥' if alert_type == 'CRÍTICA' else '⚠️' 
            print(f"{icon} ALERTA {alert_type}: {camion_id} - Temperatura: {temperatura}°C")

def get_statistics():
    """Obtener estadísticas agregadas"""
    total_alerts = sum(s['alerts'] for s in truck_stats.values())
    total_critical = sum(s['critical_alerts'] for s in truck_stats.values())
    total_warnings = total_alerts - total_critical
    
    return {
        'total_alerts': total_alerts,
        'critical_alerts': total_critical,
        'warning_alerts': total_warnings,
        'monitored_trucks': len(truck_stats),
        'truck_stats': truck_stats
    }

@app.route('/')
def index():
    """Página principal con el sistema de alertas"""
    return render_template('index.html')

@app.route('/api/alerts')
def get_alerts():
    """API para obtener todas las alertas"""
    return jsonify(list(alerts))

@app.route('/api/stats')
def get_stats():
    """API para obtener estadísticas"""
    return jsonify(get_statistics())

if __name__ == '__main__':
    # Iniciar el thread del consumer de Kafka
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    print("🚨 Sistema de Alertas iniciado en http://localhost:5001")
    socketio.run(app, host='0.0.0.0', port=5001, debug=False, allow_unsafe_werkzeug=True)
