import random
import time
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

from main import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)

if __name__ == "__main__":
    try:
        connexion = psycopg2.connect(host="localhost", database="voting", user="postgres", password="patron")
        cursor = connexion.cursor()

        candidats_query = cursor.execute("""
            SELECT row_to_json(col)
            FROM (
                SELECT * FROM candidats
            ) col;
        """)

        candidats = cursor.fetchall()
        candidats = [candidat[0] for candidat in candidats]
        if len(candidats) == 0:
            raise Exception("Pas de candidat dans la base de données")
        else:
            print(candidats)

        consumer.subscribe(['voters_topic'])
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                else:
                    voter = json.loads(msg.value().decode('utf-8'))
                    chosen_candidate = random.choice(candidats)
                    vote = voter | chosen_candidate | {
                        "temps_vote": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "vote": 1
                    }

                    try:
                        cursor.execute("""
                                INSERT INTO votes (votant_id, candidat_id, temps_vote)
                                VALUES (%s, %s, %s)
                            """, (vote['votant_id'], vote['candidat_id'], vote['temps_vote']))
                        print("User {} is voting for candidate: {}".format(vote['votant_id'], vote['candidat_id']))

                        connexion.commit()

                        producer.produce(
                            'votes_topic',
                            key=vote["votant_id"],
                            value=json.dumps(vote),
                            on_delivery=delivery_report
                        )
                        producer.poll(0)
                        # print(vote)
                    except Exception as e:
                        print("Error: {}".format(e))
                        continue
                time.sleep(0.2)
        except KafkaException as e:
            print(e)

    except Exception as e:
        print(e)