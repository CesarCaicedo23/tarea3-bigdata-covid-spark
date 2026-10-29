"""
Tarea 3 - Big Data (UNAD) | Generador de datos para Kafka (productor)

Lee el CSV real de casos de COVID-19 (datos.gov.co), limpia cada fila con las
mismas reglas del procesamiento batch y la envía como mensaje JSON a un topic
de Kafka, simulando la llegada de casos en tiempo real.

Uso (en la VM, con Kafka corriendo):
    python3 kafka_producer_covid.py --input casos_covid.csv --tasa 20

Prueba sin Kafka (solo imprime los mensajes):
    python3 kafka_producer_covid.py --input casos_covid.csv --dry-run --limite 5
"""
import argparse
import csv
import json
import re
import sys
import time
import unicodedata


def sin_tildes(texto: str) -> str:
    s = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in s if not unicodedata.combining(c))


def normalizar(nombre: str) -> str:
    """'Fecha de notificación' -> 'fecha_de_notificacion'."""
    return re.sub(r"[^a-z0-9]+", "_", sin_tildes(nombre).lower()).strip("_")


def resolver(columnas, candidatas, obligatoria=True):
    for c in candidatas:
        if c in columnas:
            return c
    if obligatoria:
        raise SystemExit(f"No se encontró ninguna de estas columnas: {candidatas}\n"
                         f"Columnas del archivo: {columnas}")
    return None


def grupo_de_edad(anios):
    if anios is None:
        return "Sin dato"
    for limite in range(10, 90, 10):
        if anios < limite:
            return f"{limite - 10:02d}-{limite - 1:02d}"
    return "80+"


def limpiar_fila(fila: dict, c: dict):
    """Aplica la misma limpieza del batch. Devuelve un dict o None si se descarta."""
    dep = sin_tildes((fila.get(c["dep"]) or "").strip().upper())
    if not dep:
        return None
    if dep.startswith("BOGOT"):
        dep = "BOGOTA D.C."

    try:
        anios = float(fila.get(c["edad"]) or "")
        unidad = (fila.get(c["uni"]) or "1").strip() if c["uni"] else "1"
        anios = anios / 12.0 if unidad == "2" else anios / 365.0 if unidad == "3" else anios
        anios = round(anios, 1) if 0 <= anios <= 120 else None
    except ValueError:
        anios = None

    sexo = (fila.get(c["sexo"]) or "").strip().upper()
    sexo = sexo if sexo in ("F", "M") else "SIN DATO"

    estado = (fila.get(c["est"]) or "").strip().title() if c["est"] else ""
    if estado.upper() in ("", "N/A", "NA"):
        estado = "Sin dato"

    muerte = (fila.get(c["mue"]) or "").strip() if c["mue"] else ""
    fallecido = 1 if (estado.upper() == "FALLECIDO" or muerte) else 0

    fecha = ""
    for k in ("not", "rep"):
        if c[k] and (fila.get(c[k]) or "").strip():
            fecha = fila[c[k]].strip()[:10]
            break

    return {
        "departamento": dep,
        "edad_anios": anios,
        "grupo_edad": grupo_de_edad(anios),
        "sexo": sexo,
        "estado": estado,
        "fallecido": fallecido,
        "fecha_caso": fecha,
        "ts_evento": int(time.time()),  # momento de llegada simulada (epoch, segundos)
    }


def main():
    ap = argparse.ArgumentParser(description="Productor Kafka - casos COVID-19 Colombia")
    ap.add_argument("--input", required=True, help="CSV del dataset (datos.gov.co)")
    ap.add_argument("--topic", default="casos_covid")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--tasa", type=float, default=20, help="mensajes por segundo")
    ap.add_argument("--limite", type=int, default=0, help="máx. de mensajes (0 = todo el archivo)")
    ap.add_argument("--dry-run", action="store_true", help="imprime sin enviar a Kafka")
    args = ap.parse_args()

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer  # pip install kafka-python
        producer = KafkaProducer(
            bootstrap_servers=[args.bootstrap],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

    pausa = 1.0 / args.tasa if args.tasa > 0 else 0
    enviados = 0
    try:
        with open(args.input, newline="", encoding="utf-8-sig") as f:
            lector = csv.reader(f)
            columnas = [normalizar(h) for h in next(lector)]
            c = {
                "dep": resolver(columnas, ["nombre_departamento", "departamento_nom", "departamento"]),
                "edad": resolver(columnas, ["edad"]),
                "uni": resolver(columnas, ["unidad_de_medida_de_edad", "unidad_medida"], False),
                "sexo": resolver(columnas, ["sexo"]),
                "est": resolver(columnas, ["estado", "atencion", "atenci_n"], False),
                "not": resolver(columnas, ["fecha_de_notificacion", "fecha_de_notificaci_n"], False),
                "rep": resolver(columnas, ["fecha_reporte_web"], False),
                "mue": resolver(columnas, ["fecha_de_muerte", "fecha_muerte"], False),
            }
            for valores in lector:
                msg = limpiar_fila(dict(zip(columnas, valores)), c)
                if msg is None:
                    continue
                if producer:
                    producer.send(args.topic, value=msg)
                print(f"Sent: {msg}")
                enviados += 1
                if args.limite and enviados >= args.limite:
                    break
                time.sleep(pausa)
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario (Ctrl+C).")
    finally:
        if producer:
            producer.flush()
            producer.close()
        print(f"Mensajes enviados: {enviados}", file=sys.stderr)


if __name__ == "__main__":
    main()
