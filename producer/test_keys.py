import os

from confluent_kafka import Producer

# L'adresse de Kafka dépend d'où tourne le script :
#   - sur Windows        → localhost:9092 (valeur par défaut)
#   - dans un conteneur  → kafka:29092    (passée via variable d'environnement)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    """Appelée par Kafka pour confirmer (ou non) la réception de chaque message."""
    if err is not None:
        print(f"ÉCHEC : {err}")
    else:
        print(f"clé={msg.key().decode()}  →  partition {msg.partition()}, offset {msg.offset()}")


# On envoie plusieurs messages pour 3 stations différentes
stations = ["111", "222", "333", "111", "222", "111", "333", "111"]

for i, station_id in enumerate(stations):
    producer.produce(
        topic="velib-test",
        key=station_id,
        value=f"mesure n°{i} de la station {station_id}",
        callback=delivery_report,
    )

# produce() est asynchrone : les messages partent en arrière-plan.
# flush() attend que TOUS les messages soient bien arrivés avant de quitter.
producer.flush()
