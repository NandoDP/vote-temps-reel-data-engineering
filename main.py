import random
import psycopg2
import requests
from confluent_kafka import SerializingProducer
import json

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Partie de Gauche", "Partie de Droite", "Partie du Milieu"]
random.seed(42)


def create_table(connexion, cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS candidats (
            candidat_id VARCHAR(255) PRIMARY KEY,
            candidat_nom VARCHAR(255),
            partie VARCHAR(255),
            biographie TEXT,
            platforme_campagne TEXT,
            url_photo TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS votants (
            votant_id VARCHAR(255) PRIMARY KEY,
            votant_nom VARCHAR(255),
            date_naissance DATE,
            genre VARCHAR(255),
            nationalite VARCHAR(255),
            numero_registre VARCHAR(255),
            adresse_rue VARCHAR(255),
            adresse_ville VARCHAR(255),
            adresse_region VARCHAR(255),
            adresse_pays VARCHAR(255),
            adresse_postal VARCHAR(255),
            email VARCHAR(255),
            numero_tel VARCHAR(255),
            photo TEXT,
            age_enregistrer INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            votant_id VARCHAR(255) UNIQUE,
            candidat_id VARCHAR(255),
            temps_vote TIMESTAMP,
            vote INT DEFAULT 1,
            PRIMARY KEY (votant_id, candidat_id)
        )
    """)

    connexion.commit()

def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            "candidat_id": user_data['login']['uuid'],
            "candidat_nom": f"{user_data['name']['first']} {user_data['name']['last']}",
            "partie": PARTIES[candidate_number % total_parties],
            "biographie": "Une brève biographie du candidat.",
            "platforme_campagne": "Promesses ou plate-forme clés de la campagne.",
            "url_photo": user_data['picture']['large']
        }
    else:
        return "Erreur lors de la récupération des données 'Candidats'"

def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "votant_id": user_data['login']['uuid'],
            "votant_nom": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_naissance": user_data['dob']['date'],
            "genre": user_data['gender'],
            "nationalite": user_data['nat'],
            "number_tel": user_data['login']['username'],
            "address": {
                "rue": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "ville": user_data['location']['city'],
                "region": user_data['location']['state'],
                "pays": user_data['location']['country'],
                "codePostal": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "photo": user_data['picture']['large'],
            "age": user_data['registered']['age']
        }
    else:
        return "Erreur lors de la récupération des données 'votants'"

def insert_voters(conn, cur, voter):
    cur.execute("""
                        INSERT INTO votants (votant_id, votant_nom, date_naissance, genre, nationalite, numero_registre, adresse_rue, adresse_ville, adresse_region, adresse_pays, adresse_postal, email, numero_tel, photo, age_enregistrer)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                (voter["votant_id"], voter['votant_nom'], voter['date_naissance'], voter['genre'],
                 voter['nationalite'], voter['number_tel'], voter['address']['rue'],
                 voter['address']['ville'], voter['address']['region'], voter['address']['pays'],
                 voter['address']['codePostal'], voter['email'], voter['phone_number'], voter['photo'],
                 voter['age']))
    conn.commit()


def delivery_report(err, msg):
    if err is not None:
        print(f"Echec de l'envoi du message: {err}")
    else:
        print(f'Message envoyé a {msg.topic()} [{msg.partition()}]')


# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'

if __name__ == "__main__":
    try:
        # Connexion à la base de données
        connexion = psycopg2.connect(host="localhost", database="voting", user="postgres", password="patron")

        # Création d'un curseur pour exécuter des requêtes SQL
        cursor = connexion.cursor()

        producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
        create_table(connexion, cursor)

        cursor.execute("""
            SELECT * FROM candidats
        """)

        candidats = cursor.fetchall()
        #print(candidats)

        if len(candidats) == 0:
            for i in range(3):
                candidat = generate_candidate_data(i, 3)
                print(candidat['candidat_id'])
                cursor.execute("""
                            INSERT INTO candidats (candidat_id, candidat_nom, partie, biographie, platforme_campagne, url_photo)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                    candidat['candidat_id'], candidat['candidat_nom'], candidat['partie'],
                    candidat['biographie'], candidat['platforme_campagne'], candidat['url_photo']))
                connexion.commit()

        for i in range(1000-1):
            voter_data = generate_voter_data()
            insert_voters(connexion, cursor, voter_data)
            print(voter_data["votant_nom"], i)
            producer.produce(
                voters_topic,
                key=voter_data["votant_id"],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )

            # print('Produced voter {}, data: {}'.format(i, voter_data))
            producer.flush()

    except Exception as e:
        print(e)