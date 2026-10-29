#!/usr/bin/env bash
# Tarea 3 - Big Data (UNAD) | Instalación de Kafka en la VM (sigue el Anexo 3)
# Uso: bash setup_kafka_vm.sh
# Requiere Java 11 y Spark 3.5.3 ya instalados (actividades anteriores).
set -e

KAFKA_VERSION="3.9.2"
SCALA_VERSION="2.12"
ARCHIVO="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
# Se usa archive.apache.org porque downloads.apache.org solo conserva las versiones vigentes
URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${ARCHIVO}.tgz"
TOPIC="casos_covid"

echo ">> 1/5 Instalando la librería de Python para Kafka"
pip install kafka-python

if [ ! -d /opt/Kafka ]; then
  echo ">> 2/5 Descargando y descomprimiendo Kafka ${KAFKA_VERSION}"
  wget -nc "${URL}"
  tar -xzf "${ARCHIVO}.tgz"
  sudo mv "${ARCHIVO}" /opt/Kafka
else
  echo ">> 2/5 /opt/Kafka ya existe, se omite la descarga"
fi

mkdir -p "$HOME/kafka_logs"

echo ">> 3/5 Iniciando ZooKeeper"
sudo nohup /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties \
  > "$HOME/kafka_logs/zookeeper.log" 2>&1 &
sleep 10

echo ">> 4/5 Iniciando el broker de Kafka"
sudo nohup /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties \
  > "$HOME/kafka_logs/kafka.log" 2>&1 &
sleep 15

echo ">> 5/5 Creando el topic ${TOPIC}"
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic "${TOPIC}" || true

echo
echo "Topics existentes:"
/opt/Kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
echo "Listo. Logs en $HOME/kafka_logs/"
