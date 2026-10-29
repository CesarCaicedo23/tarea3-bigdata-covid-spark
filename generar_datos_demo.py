"""
Genera un CSV sintético con la MISMA estructura que el dataset
"Casos positivos de COVID-19 en Colombia" (datos.gov.co, id gt2j-8ykr).

Sirve únicamente para probar el pipeline cuando aún no se ha descargado el
archivo real. NO usar estos datos para el informe final.

Uso:
    python generar_datos_demo.py --filas 200000 --salida data/casos_demo.csv
"""
import argparse
import csv
import os
import random
from datetime import date, timedelta

DEPARTAMENTOS = [
    ("11", "BOGOTA", "BOGOTA D.C."), ("5", "ANTIOQUIA", "MEDELLIN"),
    ("76", "VALLE DEL CAUCA", "CALI"), ("8", "ATLANTICO", "BARRANQUILLA"),
    ("13", "BOLIVAR", "CARTAGENA"), ("68", "SANTANDER", "BUCARAMANGA"),
    ("25", "CUNDINAMARCA", "SOACHA"), ("54", "NORTE SANTANDER", "CUCUTA"),
    ("66", "RISARALDA", "PEREIRA"), ("17", "CALDAS", "MANIZALES"),
]
PESOS_DEP = [30, 18, 12, 7, 5, 6, 6, 4, 3, 3]
ESTADOS = ["Leve", "Moderado", "Grave", "Fallecido", "N/A"]
PESOS_EST = [88, 5, 1, 2.5, 3.5]


def fecha_txt(d):
    return d.strftime("%Y-%m-%d 00:00:00")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--filas", type=int, default=100_000)
    ap.add_argument("--salida", default="data/casos_demo.csv")
    ap.add_argument("--semilla", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.semilla)
    os.makedirs(os.path.dirname(args.salida) or ".", exist_ok=True)
    inicio = date(2020, 3, 6)

    cols = [
        "fecha reporte web", "ID de caso", "Fecha de notificación",
        "Código DIVIPOLA departamento", "Nombre departamento",
        "Código DIVIPOLA municipio", "Nombre municipio", "Edad",
        "Unidad de medida de edad", "Sexo", "Tipo de contagio",
        "Ubicación del caso", "Estado", "Recuperado",
        "Fecha de inicio de síntomas", "Fecha de muerte",
        "Fecha de diagnóstico", "Fecha de recuperación",
    ]
    with open(args.salida, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for i in range(1, args.filas + 1):
            cod, dep, mun = random.choices(DEPARTAMENTOS, PESOS_DEP)[0]
            f_notif = inicio + timedelta(days=int(random.triangular(0, 700, 250)))
            f_sint = f_notif - timedelta(days=random.randint(0, 12))
            f_diag = f_notif + timedelta(days=random.randint(0, 5))
            estado = random.choices(ESTADOS, PESOS_EST)[0]
            edad = max(0, int(random.gauss(40, 20)))
            unidad = "1"
            if random.random() < 0.015:
                unidad, edad = random.choice([("2", random.randint(1, 11)),
                                              ("3", random.randint(1, 30))])
            f_muerte = ""
            if estado == "Fallecido":
                f_muerte = fecha_txt(f_notif + timedelta(days=random.randint(1, 20)))
            f_recup = ""
            recuperado = "Fallecido" if estado == "Fallecido" else "Recuperado"
            if estado != "Fallecido":
                f_recup = fecha_txt(f_notif + timedelta(days=random.randint(7, 25)))
            # Ensuciar algunos datos a propósito (nulos / formatos) para probar la limpieza
            sexo = random.choice(["F", "M", "f", "m"]) if random.random() > 0.01 else ""
            if random.random() < 0.01:
                edad = ""
            w.writerow([
                fecha_txt(f_notif), i, fecha_txt(f_notif), cod, dep, cod + "001",
                mun, edad, unidad, sexo,
                random.choice(["Comunitaria", "Relacionado", "Importado"]),
                "Casa", estado, recuperado, fecha_txt(f_sint), f_muerte,
                fecha_txt(f_diag), f_recup,
            ])
    print(f"Archivo generado: {args.salida} ({args.filas} filas)")


if __name__ == "__main__":
    main()
