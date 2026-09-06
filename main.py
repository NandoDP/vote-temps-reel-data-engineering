"""Étape 1 du pipeline.

Crée les tables PostgreSQL (`candidats`, `votants`, `votes`), génère des
candidats et des votants synthétiques à partir de randomuser.me, puis publie
les votants sur le sujet Kafka `voters_topic`.

Usage :
    python main.py

Le nombre de votants se règle par la variable d'environnement `NB_VOTANTS`
(voir `.env.example`).
"""

import json
import logging
import random
import sys

import psycopg2
import requests
from confluent_kafka import Producer
from psycopg2.extras import execute_values

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
journal = logging.getLogger("main")

URL_RANDOMUSER = "https://randomuser.me/api/"
DELAI_HTTP = 30  # secondes

PARTIS = ["Parti de Gauche", "Parti de Droite", "Parti du Milieu"]

# Les votants sont répartis sur les 45 départements du Sénégal. La région est
# déduite du département : sans cette table, la région restait celle du profil
# randomuser.me (un comté britannique), ce qui rendait l'agrégation
# « participation par région » incohérente avec le reste des données.
REGION_PAR_DEPARTEMENT = {
    "Dakar": "Dakar", "Guédiawaye": "Dakar", "Pikine": "Dakar", "Rufisque": "Dakar",
    "Bambey": "Diourbel", "Diourbel": "Diourbel", "Mbacké": "Diourbel",
    "Fatick": "Fatick", "Foundiougne": "Fatick", "Gossas": "Fatick",
    "Birkilane": "Kaffrine", "Kaffrine": "Kaffrine", "Koungheul": "Kaffrine",
    "Malem Hodar": "Kaffrine",
    "Guinguinéo": "Kaolack", "Kaolack": "Kaolack", "Nioro du Rip": "Kaolack",
    "Kédougou": "Kédougou", "Salémata": "Kédougou", "Saraya": "Kédougou",
    "Kolda": "Kolda", "Médina Yoro Foulah": "Kolda", "Vélingara": "Kolda",
    "Kébémer": "Louga", "Linguère": "Louga", "Louga": "Louga",
    "Kanel": "Matam", "Matam": "Matam", "Ranérou": "Matam",
    "Dagana": "Saint-Louis", "Podor": "Saint-Louis", "Saint-Louis": "Saint-Louis",
    "Bounkiling": "Sédhiou", "Goudomp": "Sédhiou", "Sédhiou": "Sédhiou",
    "Bakel": "Tambacounda", "Goudiry": "Tambacounda", "Koumpentoum": "Tambacounda",
    "Tambacounda": "Tambacounda",
    "Mbour": "Thiès", "Thiès": "Thiès", "Tivaouane": "Thiès",
    "Bignona": "Ziguinchor", "Oussouye": "Ziguinchor", "Ziguinchor": "Ziguinchor",
}

DEPARTEMENTS = list(REGION_PAR_DEPARTEMENT)


def creer_tables(connexion):
    """Crée les trois tables du schéma si elles n'existent pas."""
    with connexion.cursor() as curseur:
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS candidats (
                candidat_id VARCHAR(255) PRIMARY KEY,
                candidat_nom VARCHAR(255),
                parti VARCHAR(255),
                biographie TEXT,
                plateforme_campagne TEXT,
                url_photo TEXT
            )
        """)
        curseur.execute("""
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
                age_enregistre INTEGER
            )
        """)
        # `votant_id` seul est clé primaire : un votant ne vote qu'une fois.
        # La version initiale déclarait une clé composite (votant_id,
        # candidat_id) doublée d'un UNIQUE sur votant_id — la clé composite
        # autorisait à elle seule le double vote pour deux candidats
        # différents, ce qui arrive dès qu'un message Kafka est rejoué.
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                votant_id VARCHAR(255) PRIMARY KEY
                    REFERENCES votants(votant_id),
                candidat_id VARCHAR(255) NOT NULL
                    REFERENCES candidats(candidat_id),
                temps_vote TIMESTAMP NOT NULL,
                vote INT NOT NULL DEFAULT 1
            )
        """)
    connexion.commit()


def recuperer_profils(nombre):
    """Récupère `nombre` profils sur randomuser.me, par lots.

    Lève `requests.HTTPError` si l'API répond en erreur : mieux vaut échouer
    franchement que retourner des données incomplètes sans le signaler.
    """
    profils = []
    while len(profils) < nombre:
        lot = min(config.TAILLE_LOT_PROFILS, nombre - len(profils))
        reponse = requests.get(
            URL_RANDOMUSER,
            params={"nat": "gb", "results": lot},
            timeout=DELAI_HTTP,
        )
        reponse.raise_for_status()
        resultats = reponse.json().get("results", [])
        if not resultats:
            raise RuntimeError("randomuser.me a répondu sans aucun profil")
        profils.extend(resultats)
        journal.info("Profils récupérés : %d / %d", len(profils), nombre)
    return profils[:nombre]


def construire_candidat(profil, numero):
    return {
        "candidat_id": profil["login"]["uuid"],
        "candidat_nom": f"{profil['name']['first']} {profil['name']['last']}",
        "parti": PARTIS[numero % len(PARTIS)],
        "biographie": "Une brève biographie du candidat.",
        "plateforme_campagne": "Promesses ou plateforme clés de la campagne.",
        "url_photo": profil["picture"]["large"],
    }


def construire_votant(profil):
    """Transforme un profil randomuser.me en votant.

    Le pays et le département sont réécrits pour situer la simulation au
    Sénégal ; le reste du profil est conservé tel quel.
    """
    departement = random.choice(DEPARTEMENTS)
    return {
        "votant_id": profil["login"]["uuid"],
        "votant_nom": f"{profil['name']['first']} {profil['name']['last']}",
        "date_naissance": profil["dob"]["date"],
        "genre": profil["gender"],
        "nationalite": profil["nat"],
        # `id.value` est un vrai numéro d'identification national dans les
        # profils randomuser.me, ce qui correspond à `numero_registre`.
        "numero_registre": profil["id"].get("value") or profil["login"]["uuid"],
        "adresse": {
            "rue": f"{profil['location']['street']['number']} {profil['location']['street']['name']}",
            "ville": departement,
            "region": REGION_PAR_DEPARTEMENT[departement],
            "pays": "Sénégal",
            "code_postal": str(profil["location"]["postcode"]),
        },
        "email": profil["email"],
        "numero_tel": profil["phone"],
        "photo": profil["picture"]["large"],
        "age_enregistre": profil["registered"]["age"],
    }


def inserer_candidats(connexion, candidats):
    with connexion.cursor() as curseur:
        execute_values(
            curseur,
            """
            INSERT INTO candidats
                (candidat_id, candidat_nom, parti, biographie,
                 plateforme_campagne, url_photo)
            VALUES %s
            ON CONFLICT (candidat_id) DO NOTHING
            """,
            [
                (c["candidat_id"], c["candidat_nom"], c["parti"],
                 c["biographie"], c["plateforme_campagne"], c["url_photo"])
                for c in candidats
            ],
        )
    connexion.commit()


def inserer_votants(connexion, votants):
    """Insère les votants en une seule requête.

    `ON CONFLICT DO NOTHING` rend le script rejouable : relancer `main.py` sur
    une base déjà remplie n'échoue plus sur la clé primaire.
    """
    with connexion.cursor() as curseur:
        execute_values(
            curseur,
            """
            INSERT INTO votants
                (votant_id, votant_nom, date_naissance, genre, nationalite,
                 numero_registre, adresse_rue, adresse_ville, adresse_region,
                 adresse_pays, adresse_postal, email, numero_tel, photo,
                 age_enregistre)
            VALUES %s
            ON CONFLICT (votant_id) DO NOTHING
            """,
            [
                (v["votant_id"], v["votant_nom"], v["date_naissance"], v["genre"],
                 v["nationalite"], v["numero_registre"], v["adresse"]["rue"],
                 v["adresse"]["ville"], v["adresse"]["region"], v["adresse"]["pays"],
                 v["adresse"]["code_postal"], v["email"], v["numero_tel"],
                 v["photo"], v["age_enregistre"])
                for v in votants
            ],
        )
    connexion.commit()


def compte_rendu_livraison(erreur, message):
    """Rappel de livraison Kafka : ne journalise que les échecs."""
    if erreur is not None:
        journal.error("Échec de l'envoi du message : %s", erreur)


def main():
    try:
        connexion = config.connexion_postgres()
    except psycopg2.OperationalError as erreur:
        journal.error(
            "Connexion à PostgreSQL impossible sur %s:%s — le conteneur est-il "
            "démarré ? (`docker compose up -d`)\n%s",
            config.POSTGRES["host"], config.POSTGRES["port"], erreur,
        )
        return 1

    config.garantir_sujets(journal)
    producteur = Producer({"bootstrap.servers": config.KAFKA_SERVEURS})

    try:
        creer_tables(connexion)
        journal.info("Tables PostgreSQL prêtes")

        with connexion.cursor() as curseur:
            curseur.execute("SELECT count(*) FROM candidats")
            nb_candidats_existants = curseur.fetchone()[0]

        if nb_candidats_existants == 0:
            profils = recuperer_profils(config.NB_CANDIDATS)
            candidats = [
                construire_candidat(profil, numero)
                for numero, profil in enumerate(profils)
            ]
            inserer_candidats(connexion, candidats)
            journal.info("%d candidats créés", len(candidats))
        else:
            journal.info(
                "%d candidats déjà en base, génération ignorée",
                nb_candidats_existants,
            )

        profils = recuperer_profils(config.NB_VOTANTS)
        votants = [construire_votant(profil) for profil in profils]

        inserer_votants(connexion, votants)
        journal.info("%d votants insérés en base", len(votants))

        for votant in votants:
            producteur.produce(
                config.SUJET_VOTANTS,
                key=votant["votant_id"],
                value=json.dumps(votant),
                on_delivery=compte_rendu_livraison,
            )
            # `poll(0)` traite les rappels de livraison sans bloquer.
            # Un `flush()` par message, comme dans la version initiale,
            # sérialise les envois et effondre le débit.
            producteur.poll(0)

        restants = producteur.flush(timeout=60)
        if restants:
            journal.warning("%d messages non confirmés par Kafka", restants)
        journal.info(
            "%d votants publiés sur le sujet %s",
            len(votants) - restants, config.SUJET_VOTANTS,
        )
    finally:
        connexion.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
