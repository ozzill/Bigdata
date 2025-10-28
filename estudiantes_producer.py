#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import time
import argparse
from kafka import KafkaProducer

# Ruta local del CSV
CSV_LOCAL_DEFAULT = "/tmp/estudiantes.csv"

def to_int_safe(v):
    """Convierte a entero si se puede; si no, deja el valor igual."""
    try:
        return int(str(v).strip())
    except:
        return v

def open_csv_any(path):
    """Abre el CSV tolerando acentos (utf-8 o latin-1)."""
    try:
        return open(path, newline="", encoding="utf-8")
    except UnicodeDecodeError:
        return open(path, newline="", encoding="latin-1")

def main():
    parser = argparse.ArgumentParser("Producer local -> Kafka (Python 3)")
    parser.add_argument("--csv", default=CSV_LOCAL_DEFAULT, help="Ruta del CSV local")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Servidor Kafka")
    parser.add_argument("--topic", default="estudiantes_grupos_etnicos", help="Tópico Kafka")
    parser.add_argument("--delay", type=float, default=0.05, help="Pausa entre mensajes (segundos)")
    args = parser.parse_args()

    print("Leyendo archivo local: {args.csv}")
    print("Enviando datos al tópico Kafka: {args.topic}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )

    sent = 0
    with open_csv_any(args.csv) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convertir columnas numéricas
            for c in ("d_año","d_muni","d_grado","d_edad","d_hombres","d_mujeres"):
                if c in row:
                    row[c] = to_int_safe(row[c])

            # Enviar el registro a Kafka
            producer.send(args.topic, row)
            sent += 1
            if sent % 200 == 0:
                print(f"[Producer] Enviados {sent} mensajes...")
            time.sleep(args.delay)

    producer.flush()
    print("Envío completado. Total mensajes enviados")

if __name__ == "__main__":
    main()