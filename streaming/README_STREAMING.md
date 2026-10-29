# Procesamiento en tiempo real: Kafka + Spark Streaming (Anexo 3)

Adaptación del Anexo 3 al dataset de COVID-19 en Colombia. Cambia el `sensor_data` del anexo por
casos reales del dataset enviados al topic `casos_covid`.

| Anexo 3 | Este proyecto |
|---|---|
| `kafka_producer.py` (sensores aleatorios) | `kafka_producer_covid.py` (casos reales del CSV, limpiados) |
| `spark_streaming_consumer.py` (promedio por sensor, consola) | `spark_streaming_covid.py` (casos, fallecidos, edad promedio por ventana/departamento/sexo/edad) |
| Topic `sensor_data` | Topic `casos_covid` |
| Salida solo en consola | Consola + dashboard web en tiempo real (`dashboard_streaming.py`) |

## Arquitectura
```
CSV datos.gov.co -> kafka_producer_covid.py -> Kafka (topic casos_covid)
   -> Spark Structured Streaming (ventanas de 1 min) -> estado.json -> dashboard (navegador)
```

## Paso a paso (en la VM, usuario `vboxuser`; conexión por PuTTY como en las Figuras 1 y 2)

**0. Copiar los archivos a la VM y el dataset** (WinSCP, `scp` o `wget`):
`streaming/*` y el CSV `casos_covid.csv` (el mismo del batch) en `/home/vboxuser/`.

**1. Instalar y arrancar Kafka, y crear el topic** (equivale a Figuras 3 a 10):
```bash
bash setup_kafka_vm.sh
```
Si prefiere hacerlo a mano, son los comandos del anexo, cambiando el nombre del topic:
```bash
pip install kafka-python
wget https://archive.apache.org/dist/kafka/3.9.2/kafka_2.12-3.9.2.tgz
tar -xzf kafka_2.12-3.9.2.tgz && sudo mv kafka_2.12-3.9.2 /opt/Kafka
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
   --replication-factor 1 --partitions 1 --topic casos_covid
```

**2. Terminal A: productor (generador de datos)** (Figura 15). Se ejecuta primero:
```bash
python3 kafka_producer_covid.py --input casos_covid.csv --tasa 20
```

**3. Terminal B: consumidor Spark Streaming** (Figura 17):
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_covid.py
```
Cada 5 segundos imprime los casos acumulados, los fallecidos, la letalidad y el top de departamentos.

**4. Terminal C: visualización en tiempo real**:
```bash
python3 dashboard_streaming.py --archivo salida_streaming/estado.json --puerto 8050
```
Abra en el navegador de su computador `http://<IP-de-la-VM>:8050` (por ejemplo `http://192.168.1.7:8050`).

**5. Consola de Spark** (Figuras 19 a 22): `http://<IP-de-la-VM>:4040` (Jobs, Stages, Structured Streaming).

**6. Finalizar:** `Ctrl+C` en cada terminal.

## Qué calcula el consumidor
Agrupa por ventana de tiempo de 1 minuto, departamento, sexo y grupo de edad y calcula:
número de casos, fallecidos, suma y conteo de edades (para la edad promedio exacta).
Con eso arma: KPIs globales, top 15 de departamentos, casos por sexo, por grupo de edad y la
serie de las últimas 15 ventanas.

## Problemas frecuentes
- **`NoBrokersAvailable`** en el productor: Kafka no está corriendo. Revise `~/kafka_logs/kafka.log`.
- **El consumidor no muestra datos:** inicie primero el productor, o use `--offsets earliest`.
- **`Failed to find data source: kafka`:** falta el parámetro `--packages` en `spark-submit`.
  Debe coincidir con la versión de Spark (3.5.3) y de Scala (2.12.18, ver la pestaña Environment).
- **El dashboard no abre desde el anfitrión:** verifique la IP con `ip a`, que la VM tenga red en modo
  puente (como en el anexo) y que el puerto 8050 no esté bloqueado.
- **El dashboard no dibuja gráficos:** el navegador necesita internet para cargar Chart.js.
- **Descarga de Kafka con error 404:** el anexo usa `downloads.apache.org`, que solo guarda las versiones
  vigentes; use `archive.apache.org/dist/kafka/` como en este script.
