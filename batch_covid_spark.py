"""
Tarea 3 - Big Data (UNAD) | Procesamiento en batch con Apache Spark (PySpark)

Dataset : Casos positivos de COVID-19 en Colombia (datos.gov.co, id gt2j-8ykr)
Etapas  : 1) Carga  2) Limpieza y transformación  3) EDA  4) Visualización

Uso:
    python batch_covid_spark.py --input data/casos_covid.csv
    python batch_covid_spark.py --descargar            # descarga el CSV real
    python batch_covid_spark.py --input data/casos_demo.csv --salida resultados_demo
"""
import argparse
import os
import re
import unicodedata
import urllib.request

import matplotlib

matplotlib.use("Agg")  # permite generar gráficos sin entorno gráfico
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

URL_DATASET = "https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD"
RUTA_POR_DEFECTO = "data/casos_covid.csv"


# ----------------------------------------------------------------------------
# Utilidades
# ----------------------------------------------------------------------------
def normalizar(nombre: str) -> str:
    """'Fecha de notificación' -> 'fecha_de_notificacion'."""
    s = unicodedata.normalize("NFKD", nombre)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def resolver(columnas, candidatas, obligatoria=True):
    """Devuelve la primera columna existente de la lista de nombres posibles.
    El dataset ha cambiado de nombres de columnas entre versiones."""
    for c in candidatas:
        if c in columnas:
            return c
    if obligatoria:
        raise ValueError(f"No se encontró ninguna de estas columnas: {candidatas}\n"
                         f"Columnas disponibles: {columnas}")
    return None


def parsear_fecha(nombre_col: str):
    """Convierte texto a fecha aceptando 'yyyy-MM-dd...' y 'dd/MM/yyyy...'."""
    c = F.col(nombre_col)
    iso = F.regexp_extract(c, r"^(\d{4}-\d{2}-\d{2})", 1)
    dmy = F.regexp_extract(c, r"^(\d{2}/\d{2}/\d{4})", 1)
    return (F.when(iso != "", F.to_date(iso, "yyyy-MM-dd"))
             .when(dmy != "", F.to_date(dmy, "dd/MM/yyyy")))


def descargar(url: str, destino: str):
    os.makedirs(os.path.dirname(destino) or ".", exist_ok=True)
    print(f"Descargando {url}\n  -> {destino}  (puede tardar varios minutos)")
    urllib.request.urlretrieve(url, destino)
    print("Descarga completa.")


def seccion(titulo: str):
    print("\n" + "=" * 78 + f"\n{titulo}\n" + "=" * 78)


# ----------------------------------------------------------------------------
# 1) Carga
# ----------------------------------------------------------------------------
def cargar(spark, ruta):
    seccion("1) CARGA DE DATOS")
    # Se lee todo como texto: es más rápido y evita errores de inferencia.
    df = (spark.read.option("header", True)
                    .option("escape", '"')
                    .option("encoding", "UTF-8")
                    .csv(ruta))
    df = df.toDF(*[normalizar(c) for c in df.columns])
    print(f"Archivo: {ruta}")
    print(f"Columnas ({len(df.columns)}): {df.columns}")
    print(f"Particiones del DataFrame: {df.rdd.getNumPartitions()}")
    df.printSchema()
    return df


# ----------------------------------------------------------------------------
# 2) Limpieza y transformación
# ----------------------------------------------------------------------------
def limpiar(df):
    seccion("2) LIMPIEZA Y TRANSFORMACIÓN")
    cols = df.columns
    c_id = resolver(cols, ["id_de_caso", "id_caso"], obligatoria=False)
    c_dep = resolver(cols, ["nombre_departamento", "departamento_nom", "departamento"])
    c_edad = resolver(cols, ["edad"])
    c_uni = resolver(cols, ["unidad_de_medida_de_edad", "unidad_medida"], obligatoria=False)
    c_sexo = resolver(cols, ["sexo"])
    c_est = resolver(cols, ["estado", "atencion", "atenci_n"], obligatoria=False)
    c_not = resolver(cols, ["fecha_de_notificacion", "fecha_de_notificaci_n"], obligatoria=False)
    c_rep = resolver(cols, ["fecha_reporte_web"], obligatoria=False)
    c_sin = resolver(cols, ["fecha_de_inicio_de_sintomas", "fecha_inicio_sintomas", "fis"],
                     obligatoria=False)
    c_mue = resolver(cols, ["fecha_de_muerte", "fecha_muerte"], obligatoria=False)

    total_raw = df.count()
    print(f"Registros originales: {total_raw:,}")

    # Nulos por columna (datos crudos)
    nulos = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    print("\nNulos por columna en los datos originales:")
    for c in cols:
        if nulos[c]:
            print(f"  {c:<45}{nulos[c]:>12,}  ({nulos[c] / total_raw:6.1%})")

    # --- Duplicados ---------------------------------------------------------
    if c_id:
        df = df.dropDuplicates([c_id])
    else:
        df = df.dropDuplicates()

    # --- Departamento: mayúsculas, sin tildes, Bogotá unificada -------------
    dep = F.translate(F.upper(F.trim(F.col(c_dep))), "ÁÉÍÓÚÜÑ", "AEIOUUN")
    dep = F.when(dep.startswith("BOGOT"), F.lit("BOGOTA D.C.")).otherwise(dep)
    df = df.withColumn("departamento", F.when(dep == "", None).otherwise(dep))

    # --- Edad en años (unidad 1=años, 2=meses, 3=días) ----------------------
    edad = F.col(c_edad).cast("double")
    if c_uni:
        uni = F.trim(F.col(c_uni).cast("string"))
        anios = (F.when(uni == "2", edad / 12.0)
                  .when(uni == "3", edad / 365.0)
                  .otherwise(edad))
    else:
        anios = edad
    anios = F.when((anios >= 0) & (anios <= 120), anios)  # fuera de rango -> null
    df = df.withColumn("edad_anios", F.round(anios, 1))

    grupo = F.when(F.col("edad_anios").isNull(), "Sin dato")
    for limite in range(10, 90, 10):
        grupo = grupo.when(F.col("edad_anios") < limite, f"{limite - 10:02d}-{limite - 1:02d}")
    df = df.withColumn("grupo_edad", grupo.otherwise("80+"))

    # --- Sexo ---------------------------------------------------------------
    sexo = F.upper(F.trim(F.col(c_sexo)))
    df = df.withColumn("sexo", F.when(sexo.isin("F", "M"), sexo).otherwise("SIN DATO"))

    # --- Fechas -------------------------------------------------------------
    nuevas = {"f_notificacion": c_not, "f_reporte": c_rep,
              "f_sintomas": c_sin, "f_muerte": c_mue}
    for nuevo, origen in nuevas.items():
        df = df.withColumn("_tmp", F.col(origen) if origen else F.lit(None).cast("string"))
        df = df.withColumn(nuevo, parsear_fecha("_tmp")).drop("_tmp")
    df = df.withColumn("fecha_caso", F.coalesce("f_notificacion", "f_reporte", "f_sintomas"))

    # Se descartan fechas imposibles (antes de la pandemia o futuras)
    df = df.filter(F.col("fecha_caso").isNull() |
                   F.col("fecha_caso").between(F.lit("2020-01-01").cast("date"), F.current_date()))
    df = df.withColumn("mes", F.trunc("fecha_caso", "month"))

    # --- Estado y variable de fallecimiento ---------------------------------
    estado = F.initcap(F.trim(F.col(c_est))) if c_est else F.lit(None)
    sin_dato = estado.isNull() | F.upper(estado).isin("", "N/A", "NA")
    df = df.withColumn("estado", F.when(sin_dato, "Sin dato").otherwise(estado))
    df = df.withColumn("fallecido",
                       ((F.upper(F.col("estado")) == "FALLECIDO") | F.col("f_muerte").isNotNull())
                       .cast("int"))

    # --- Días entre inicio de síntomas y notificación -----------------------
    dias = F.datediff("f_notificacion", "f_sintomas")
    df = df.withColumn("dias_sintomas_a_notificacion", F.when((dias >= 0) & (dias <= 60), dias))

    # Solo las columnas que usará el EDA
    limpio = df.select("departamento", "edad_anios", "grupo_edad", "sexo", "estado",
                       "fallecido", "fecha_caso", "mes", "dias_sintomas_a_notificacion")
    limpio = limpio.filter(F.col("departamento").isNotNull()).cache()

    total_limpio = limpio.count()  # materializa la caché
    print(f"\nRegistros tras limpieza: {total_limpio:,} "
          f"(descartados: {total_raw - total_limpio:,}, {(total_raw - total_limpio) / total_raw:.2%})")
    limpio.show(5, truncate=False)
    return limpio


# ----------------------------------------------------------------------------
# 3) EDA
# ----------------------------------------------------------------------------
def eda(spark, df, carpeta):
    seccion("3) ANÁLISIS EXPLORATORIO (EDA)")
    res = {}

    print("Estadísticas de la edad (años):")
    df.select("edad_anios").describe().show()
    q = df.approxQuantile("edad_anios", [0.25, 0.5, 0.75], 0.01)
    print(f"Cuartiles de edad (Q1, mediana, Q3): {q}")

    # (a) Casos y fallecidos por departamento
    por_dep = (df.groupBy("departamento")
                 .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
                 .withColumn("letalidad_pct", F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
                 .orderBy(F.desc("casos")))
    print("\nTop 15 departamentos por número de casos:")
    por_dep.show(15, truncate=False)
    res["por_departamento"] = por_dep.toPandas()

    # (b) Casos por grupo de edad y sexo
    por_edad = (df.filter(F.col("sexo").isin("F", "M"))
                  .groupBy("grupo_edad", "sexo").count().orderBy("grupo_edad", "sexo"))
    print("Casos por grupo de edad y sexo:")
    por_edad.show(40)
    res["edad_sexo"] = por_edad.toPandas()

    # (c) Evolución mensual
    por_mes = (df.filter(F.col("mes").isNotNull())
                 .groupBy("mes")
                 .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
                 .orderBy("mes"))
    print("Evolución mensual (primeros meses):")
    por_mes.show(12)
    res["por_mes"] = por_mes.toPandas()

    # (d) Estado del caso
    por_estado = df.groupBy("estado").count().orderBy(F.desc("count"))
    print("Distribución por estado:")
    por_estado.show()
    res["por_estado"] = por_estado.toPandas()

    # (e) Retraso entre síntomas y notificación
    print("Días entre inicio de síntomas y notificación:")
    df.select("dias_sintomas_a_notificacion").describe().show()
    por_dias = (df.filter(F.col("dias_sintomas_a_notificacion").isNotNull())
                  .groupBy("dias_sintomas_a_notificacion").count()
                  .orderBy("dias_sintomas_a_notificacion"))
    res["retraso"] = por_dias.toPandas()

    # (f) Spark SQL: mismo tipo de consulta usando SQL
    df.createOrReplaceTempView("casos")
    print("Spark SQL - letalidad por sexo:")
    spark.sql("""
        SELECT sexo, COUNT(*) AS casos, SUM(fallecido) AS fallecidos,
               ROUND(100 * SUM(fallecido) / COUNT(*), 2) AS letalidad_pct
        FROM casos GROUP BY sexo ORDER BY casos DESC
    """).show()

    # (g) Ejemplo con RDD: conteo por departamento con map + reduceByKey
    print("RDD - conteo por departamento (map + reduceByKey + takeOrdered):")
    rdd = (df.select("departamento").rdd
             .map(lambda fila: (fila[0], 1))
             .reduceByKey(lambda a, b: a + b))
    for dep, n in rdd.takeOrdered(5, key=lambda x: -x[1]):
        print(f"  {dep:<25}{n:>12,}")

    # Guardar tablas agregadas (son pequeñas)
    os.makedirs(carpeta, exist_ok=True)
    for nombre, tabla in res.items():
        tabla.to_csv(os.path.join(carpeta, f"{nombre}.csv"), index=False)
    print(f"\nTablas agregadas guardadas en: {carpeta}/")
    return res


# ----------------------------------------------------------------------------
# 4) Visualización
# ----------------------------------------------------------------------------
def graficar(res, carpeta):
    seccion("4) VISUALIZACIÓN")
    figs = os.path.join(carpeta, "figuras")
    os.makedirs(figs, exist_ok=True)

    # Fig 1: casos por departamento (top 15)
    d = res["por_departamento"].head(15).iloc[::-1]
    plt.figure(figsize=(9, 6))
    plt.barh(d["departamento"], d["casos"], color="#1f77b4")
    plt.title("Casos de COVID-19 por departamento (top 15)")
    plt.xlabel("Número de casos")
    plt.tight_layout()
    plt.savefig(os.path.join(figs, "1_casos_por_departamento.png"), dpi=150)
    plt.close()

    # Fig 2: casos por grupo de edad y sexo
    t = res["edad_sexo"].pivot(index="grupo_edad", columns="sexo", values="count").fillna(0)
    t.plot(kind="bar", figsize=(9, 5), color={"F": "#d62728", "M": "#1f77b4"})
    plt.title("Casos por grupo de edad y sexo")
    plt.xlabel("Grupo de edad (años)")
    plt.ylabel("Número de casos")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(figs, "2_casos_por_edad_sexo.png"), dpi=150)
    plt.close()

    # Fig 3: evolución mensual
    m = res["por_mes"]
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(m["mes"], m["casos"], color="#1f77b4", label="Casos")
    ax1.set_ylabel("Casos", color="#1f77b4")
    ax2 = ax1.twinx()
    ax2.plot(m["mes"], m["fallecidos"], color="#d62728", label="Fallecidos")
    ax2.set_ylabel("Fallecidos", color="#d62728")
    plt.title("Evolución mensual de casos y fallecidos")
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(os.path.join(figs, "3_evolucion_mensual.png"), dpi=150)
    plt.close()

    # Fig 4: retraso síntomas -> notificación
    r = res["retraso"]
    r = r[r["dias_sintomas_a_notificacion"] <= 30]
    plt.figure(figsize=(9, 5))
    plt.bar(r["dias_sintomas_a_notificacion"], r["count"], color="#2ca02c")
    plt.title("Días entre inicio de síntomas y notificación")
    plt.xlabel("Días")
    plt.ylabel("Número de casos")
    plt.tight_layout()
    plt.savefig(os.path.join(figs, "4_retraso_notificacion.png"), dpi=150)
    plt.close()

    print(f"Gráficos guardados en: {figs}/")


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Procesamiento batch COVID-19 Colombia con PySpark")
    ap.add_argument("--input", default=RUTA_POR_DEFECTO, help="Ruta del CSV")
    ap.add_argument("--salida", default="resultados", help="Carpeta de resultados")
    ap.add_argument("--descargar", action="store_true", help="Descarga el CSV desde datos.gov.co")
    args = ap.parse_args()

    if args.descargar:
        descargar(URL_DATASET, args.input)
    if not os.path.exists(args.input):
        raise SystemExit(f"No existe {args.input}. Use --descargar o indique --input.")

    spark = (SparkSession.builder
             .appName("Tarea3_COVID_Batch")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.sql.ansi.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    try:
        crudo = cargar(spark, args.input)
        limpio = limpiar(crudo)
        resultados = eda(spark, limpio, args.salida)
        graficar(resultados, args.salida)
        seccion("PROCESO FINALIZADO")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
