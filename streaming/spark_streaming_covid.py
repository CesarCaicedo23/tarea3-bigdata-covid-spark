"""
Tarea 3 - Big Data (UNAD) | Consumidor Spark Structured Streaming + Kafka

Lee los casos de COVID-19 del topic de Kafka, los agrega por ventanas de tiempo
(departamento, sexo y grupo de edad), imprime los resultados en consola y escribe
un archivo JSON que el dashboard (dashboard_streaming.py) muestra en tiempo real.

Ejecución en la VM del curso (Spark 3.5.3, Scala 2.12):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        spark_streaming_covid.py
"""
import argparse
import json
import os
from collections import defaultdict
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

# Esquema de los mensajes JSON enviados por kafka_producer_covid.py
ESQUEMA = StructType([
    StructField("departamento", StringType()),
    StructField("edad_anios", DoubleType()),
    StructField("grupo_edad", StringType()),
    StructField("sexo", StringType()),
    StructField("estado", StringType()),
    StructField("fallecido", IntegerType()),
    StructField("fecha_caso", StringType()),
    StructField("ts_evento", LongType()),
])


def leer_kafka(spark, bootstrap, topic, offsets):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", offsets)
            .load())


def parsear(df):
    """value (bytes) -> columnas del esquema + columna de tiempo del evento."""
    datos = df.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("d")).select("d.*")
    return datos.withColumn("evento_ts", F.col("ts_evento").cast("timestamp"))


def agregar(parsed, ventana):
    """Conteos y sumas por ventana de tiempo, departamento, sexo y grupo de edad."""
    return (parsed
            .groupBy(F.window("evento_ts", ventana), "departamento", "sexo", "grupo_edad")
            .agg(F.count("*").alias("casos"),
                 F.sum("fallecido").alias("fallecidos"),
                 F.sum("edad_anios").alias("suma_edad"),
                 F.count("edad_anios").alias("n_edad")))


def resumir(filas, lote):
    """Convierte las filas agregadas del micro-batch en el resumen que consume el dashboard."""
    por_dep = defaultdict(lambda: [0, 0, 0.0, 0])  # casos, fallecidos, suma_edad, n_edad
    por_sexo, por_grupo, por_ventana = defaultdict(int), defaultdict(int), defaultdict(lambda: [0, 0])
    for r in filas:
        d = por_dep[r["departamento"]]
        d[0] += r["casos"]
        d[1] += r["fallecidos"] or 0
        d[2] += r["suma_edad"] or 0.0
        d[3] += r["n_edad"] or 0
        por_sexo[r["sexo"]] += r["casos"]
        por_grupo[r["grupo_edad"]] += r["casos"]
        v = por_ventana[r["inicio"]]
        v[0] += r["casos"]
        v[1] += r["fallecidos"] or 0

    total = sum(v[0] for v in por_dep.values())
    muertos = sum(v[1] for v in por_dep.values())
    suma_edad = sum(v[2] for v in por_dep.values())
    n_edad = sum(v[3] for v in por_dep.values())

    deps = sorted(por_dep.items(), key=lambda kv: -kv[1][0])
    ventanas = sorted(por_ventana.items())[-15:]
    return {
        "actualizado": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "lote": lote,
        "kpis": {
            "total_casos": total,
            "total_fallecidos": muertos,
            "letalidad_pct": round(100 * muertos / total, 2) if total else 0,
            "edad_promedio": round(suma_edad / n_edad, 1) if n_edad else None,
            "departamentos": len(por_dep),
        },
        "por_departamento": [
            {"departamento": k, "casos": v[0], "fallecidos": v[1],
             "edad_prom": round(v[2] / v[3], 1) if v[3] else None}
            for k, v in deps[:15]
        ],
        "por_sexo": dict(por_sexo),
        "por_grupo_edad": [{"grupo": k, "casos": por_grupo[k]} for k in sorted(por_grupo)],
        "serie_ventanas": [{"inicio": k.strftime("%H:%M:%S"), "casos": v[0], "fallecidos": v[1]}
                           for k, v in ventanas],
    }


def crear_procesador(ruta_json):
    """Devuelve la función que Spark ejecuta con cada micro-batch (foreachBatch)."""
    os.makedirs(os.path.dirname(ruta_json) or ".", exist_ok=True)

    def procesar(batch_df, lote):
        filas = [dict(r.asDict(), inicio=r["window"]["start"]) for r in batch_df.collect()]
        if not filas:
            print(f"[Lote {lote}] sin datos todavía...")
            return
        resumen = resumir(filas, lote)

        # Escritura atómica para que el dashboard nunca lea un archivo a medias
        tmp = ruta_json + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(resumen, f, ensure_ascii=False)
        os.replace(tmp, ruta_json)

        k = resumen["kpis"]
        print(f"\n=== Lote {lote} | {resumen['actualizado']} ===")
        print(f"Casos: {k['total_casos']:,} | Fallecidos: {k['total_fallecidos']:,} | "
              f"Letalidad: {k['letalidad_pct']}% | Edad prom.: {k['edad_promedio']}")
        print(f"{'Departamento':<22}{'Casos':>8}{'Fallec.':>9}{'Edad prom.':>12}")
        for d in resumen["por_departamento"][:10]:
            print(f"{d['departamento']:<22}{d['casos']:>8}{d['fallecidos']:>9}{str(d['edad_prom']):>12}")

    return procesar


def main():
    ap = argparse.ArgumentParser(description="Spark Streaming + Kafka - casos COVID-19")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="casos_covid")
    ap.add_argument("--ventana", default="1 minute", help="duración de la ventana de tiempo")
    ap.add_argument("--intervalo", default="5 seconds", help="frecuencia de cada micro-batch")
    ap.add_argument("--offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--salida", default="salida_streaming/estado.json")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("KafkaSparkStreamingCOVID")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    parsed = parsear(leer_kafka(spark, args.bootstrap, args.topic, args.offsets))
    consulta = (agregar(parsed, args.ventana)
                .writeStream
                .outputMode("complete")
                .foreachBatch(crear_procesador(args.salida))
                .trigger(processingTime=args.intervalo)
                .start())
    print(f"Escuchando el topic '{args.topic}' en {args.bootstrap} ... (Ctrl+C para terminar)")
    consulta.awaitTermination()


if __name__ == "__main__":
    main()
