# Tarea 3 – Procesamiento en batch con Apache Spark (PySpark)

**Curso:** Big Data (UNAD) · **Dataset:** Casos positivos de COVID-19 en Colombia
([datos.gov.co, id `gt2j-8ykr`](https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr/data))

## Problema
Caracterizar los casos de COVID-19 en Colombia por departamento, edad, sexo y tiempo, usando
procesamiento distribuido con Spark sobre un conjunto de datos de gran volumen.

Preguntas que responde el análisis:
1. ¿Cuántos casos y fallecidos hubo por departamento y cuál fue su letalidad?
2. ¿Cómo se distribuyen los casos por grupo de edad y sexo?
3. ¿Cómo evolucionaron los casos y fallecidos mes a mes?
4. ¿Cuántos días pasaron entre el inicio de síntomas y la notificación?

## Requisitos
- Python 3.9+
- Java 11, 17 o 21 (requerido por Spark). Verificar con `java -version`.
- Dependencias: `pip install -r requirements.txt`
- Windows: puede requerir `winutils.exe` y la variable `HADOOP_HOME`; con WSL2 (Ubuntu) no hace falta.

## Ejecución
```bash
pip install -r requirements.txt

# 1) Descargar el dataset real (varios cientos de MB) y procesarlo
python batch_covid_spark.py --descargar --input data/casos_covid.csv

# 2) O, si ya lo descargó manualmente desde datos.gov.co (Exportar > CSV)
python batch_covid_spark.py --input data/casos_covid.csv --salida resultados

# Prueba rápida con datos sintéticos (NO usar para el informe)
python generar_datos_demo.py --filas 100000 --salida data/casos_demo.csv
python batch_covid_spark.py --input data/casos_demo.csv --salida resultados_demo
```

## Qué hace el script
| Etapa | Descripción |
|---|---|
| 1. Carga | Lee el CSV con `spark.read`, normaliza nombres de columnas y muestra esquema y particiones. |
| 2. Limpieza | Elimina duplicados, unifica departamentos (mayúsculas, sin tildes, Bogotá), convierte la edad a años (años/meses/días), agrupa por rangos de edad, estandariza sexo y estado, convierte fechas de texto a `date`, descarta fechas imposibles y crea `fallecido`, `mes` y `dias_sintomas_a_notificacion`. |
| 3. EDA | Estadísticas descriptivas, cuartiles, agregaciones con DataFrames, una consulta con Spark SQL y un ejemplo con RDD (`map` + `reduceByKey` + `takeOrdered`). |
| 4. Visualización | Genera 4 gráficos PNG y guarda las tablas agregadas en CSV. |

## Resultados (carpeta `resultados/`)
- `por_departamento.csv`, `edad_sexo.csv`, `por_mes.csv`, `por_estado.csv`, `retraso.csv`
- `figuras/1_casos_por_departamento.png`
- `figuras/2_casos_por_edad_sexo.png`
- `figuras/3_evolucion_mensual.png`
- `figuras/4_retraso_notificacion.png`

## Notas
- El dataset ha cambiado de nombres de columnas entre versiones; el script reconoce las variantes más comunes. Si falla con "No se encontró ninguna de estas columnas", agregue el nombre nuevo a la lista de candidatas en `limpiar()`.
- Spark se ejecuta en modo local (`local[*]`), usando todos los núcleos del equipo.

## Procesamiento en tiempo real
Ver la carpeta `streaming/` y `streaming/README_STREAMING.md` (Kafka + Spark Streaming, Anexo 3).
