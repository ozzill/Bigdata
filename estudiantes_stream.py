#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def parse_args():
    parser = argparse.ArgumentParser("Streaming Spark -> Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Servidor Kafka")
    parser.add_argument("--topic", default="estudiantes_grupos_etnicos", help="Tópico Kafka")
    return parser.parse_args()

def main():
    args = parse_args()

    # Iniciar sesión Spark
    spark = (SparkSession.builder
             .appName("Streaming_Estudiantes_Local_TMP")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Esquema de los mensajes JSON que produce el producer
    schema = T.StructType([
        T.StructField("d_año", T.IntegerType()),
        T.StructField("d_muni", T.IntegerType()),
        T.StructField("d_nombmuni", T.StringType()),
        T.StructField("d_grado", T.IntegerType()),
        T.StructField("d_hombres", T.IntegerType()),
        T.StructField("d_mujeres", T.IntegerType())
    ])

    # Leer de Kafka
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", "latest")
           .load())

    # Convertir el valor binario en JSON con el esquema
    df = raw.select(
        F.col("timestamp"),
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("timestamp", "data.*")

    # Calcular el total de estudiantes (hombres + mujeres)
    df = df.withColumn("total_estudiantes", F.col("d_hombres") + F.col("d_mujeres"))

    # Agrupar por municipio y ventana de 1 minuto
    agg = (df.groupBy(F.window("timestamp", "1 minute"), "d_nombmuni")
             .agg(F.sum("total_estudiantes").alias("total"))
             .orderBy(F.col("window").desc(), F.col("total").desc()))

    # Mostrar los resultados en consola
    query = (agg.writeStream
             .outputMode("complete")
             .format("console")
             .option("truncate", False)
             .option("numRows", 30)
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()