# Guia de Inicio Rapido - Sistema de Monitoreo con Kafka

Nota: Los comandos usan Docker Compose v2 (`docker compose`). Si tu instalacion usa la version antigua, reemplaza por `docker-compose`.

## 1. Navegar al directorio del proyecto

Desde la raiz de este repositorio:

```bash
cd "KAFKA/docker-compose"
```

Alternativa (sin cambiar de carpeta):

```bash
docker compose -f "KAFKA/docker-compose/docker-compose.yml" up --build
```

## 2. Iniciar todos los servicios

```bash
docker compose up --build
```

El sistema tarda ~30-60 segundos en inicializar completamente (Kafka necesita tiempo para arrancar).

## 3. Acceder a las interfaces

| Servicio | URL | Descripcion |
|---|---|---|
| Kafka UI | http://localhost:8080 | Administracion de Kafka |
| Dashboard | http://localhost:5002 | Mapa de camiones en tiempo real |
| Alertas | http://localhost:5001 | Sistema de alertas de temperatura |

## Detener el sistema

```bash
docker compose down
```

Este comando:

- Detiene todos los contenedores
- Elimina los contenedores
- Libera los puertos
- Limpia la red virtual

## Comandos utiles

Ver logs de todos los servicios:

```bash
docker compose logs -f
```

Ver logs de un servicio especifico:

```bash
docker compose logs -f simulador
docker compose logs -f dashboard-consumer
docker compose logs -f alert-consumer
docker compose logs -f kafka
```

Presiona `Ctrl+C` para salir de los logs.

Reiniciar un servicio especifico:

```bash
docker compose restart simulador
docker compose restart dashboard-consumer
docker compose restart alert-consumer
```

Ver el estado de los servicios:

```bash
docker compose ps
```

Reconstruir un servicio especifico:

```bash
docker compose up -d --build dashboard-consumer
```

Ver mensajes de Kafka desde la terminal:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic camiones-eventos \
  --from-beginning
```

Presiona `Ctrl+C` para salir.

## Solucion de problemas

### Problema: "Port already in use" / "Puerto ya en uso"

Causa: otro programa esta usando los puertos 5001, 5002, 8080 o 9092.

Solucion:

En Windows (PowerShell como administrador):

```powershell
# Ver que esta usando el puerto
netstat -ano | findstr :5002

# Matar el proceso (reemplaza PID con el numero que aparece)
taskkill /PID <PID> /F
```

En macOS/Linux:

```bash
# Ver que esta usando el puerto
lsof -i :5002

# Matar el proceso (reemplaza PID con el numero que aparece)
kill -9 <PID>
```

### Problema: Los consumidores no se conectan

Causa: Kafka aun no esta listo.

Solucion:

- Espera 30-60 segundos mas
- Los consumidores tienen reintentos automaticos
- Verifica los logs: `docker compose logs kafka`

### Problema: El mapa no carga

Causa: sin conexion a internet o firewall bloqueando OpenStreetMap.

Solucion:

- Verifica tu conexion a internet
- Abre la consola del navegador (F12) para ver errores
- Prueba desactivar temporalmente el firewall/antivirus

### Problema: "No aparecen alertas"

Las temperaturas son aleatorias (70-110C) y puede tardar en superar el umbral de 95C.

Solucion:

- Espera 1-2 minutos
- Verifica los logs: `docker compose logs -f alert-consumer`

## Reiniciar todo desde cero

```bash
# Detener todo
docker compose down

# Limpiar volumenes (opcional)
docker volume prune -f

# Iniciar de nuevo
docker compose up --build
```

## Modo desarrollo

Los volumenes montados hacen que tus cambios de codigo esten disponibles dentro del contenedor, pero el proceso de Python no se recarga solo.

Despues de modificar codigo:

```bash
docker compose restart dashboard-consumer
```

Si cambiaste dependencias (`requirements.txt`) o el Dockerfile:

```bash
docker compose up -d --build dashboard-consumer
```

## Puedo cambiar los puertos

Si. Edita `KAFKA/docker-compose/docker-compose.yml` y ajusta las secciones `ports:`.

## Requisitos aproximados

- Disco: 1-2 GB (imagenes de Docker)
- RAM: minimo 4 GB total (Docker usara ~1-1.5 GB para todos los servicios)
