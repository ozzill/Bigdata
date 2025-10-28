import csv, json, time
from kafka import KafkaProducer

# Configuración
BOOTSTRAP = "localhost:9092"
TOPIC = "estudiantes"
CSV_PATH = "/tmp/ESTUDIANTES_GRUPOS_ETNICOS_20251027.csv"

# Inicializa el producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open(CSV_PATH, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(f"Enviando: {row}")
        producer.send(TOPIC, row)
        time.sleep(0.1)  # Pequeña pausa entre envíos

producer.flush()
print("Envío completo")
