"""Étape 2 du pipeline.

Consomme les votants depuis le sujet Kafka `voters_topic`, tire un candidat au
hasard pour chacun, enregistre le vote en PostgreSQL et publie le vote enrichi
sur le sujet `votes_topic`.

C'est ici — et non dans Spark — que le vote est enrichi des informations du
candidat, lues une seule fois au démarrage depuis PostgreSQL.

Usage :
    python voting.py          # s'arrête avec Ctrl+C

Le débit se règle par la variable d'environnement `DELAI_ENTRE_VOTES`
(0 = débit maximal ; voir `.env.example`).
"""

import logging
import random
import sys
import time
from datetime import datetime, timezone

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaError, Producer

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
journal = logging.getLogger("voting")

# Les offsets ne sont validés que par paquets : un commit par message ajoute un
# aller-retour réseau à chaque vote. La contrepartie est une garantie
# « au moins une fois » — un redémarrage peut rejouer jusqu'à cent votants,
# que le `ON CONFLICT` en base rend inoffensifs.
VOTES_PAR_COMMIT = 100


def charger_candidats(connexion):
    """Charge les candidats depuis PostgreSQL, une fois pour toutes."""
    with connexion.cursor() as curseur:
        curseur.execute("""
            SELECT candidat_id, candidat_nom, parti, biographie,
                   plateforme_campagne, url_photo
            FROM candidats
        """)
        colonnes = [description[0] for description in curseur.description]
        candidats = [dict(zip(colonnes, ligne)) for ligne in curseur.fetchall()]

    if not candidats:
        raise RuntimeError(
            "Aucun candidat en base. Lancer `python main.py` au préalable."
        )
    return candidats


def enregistrer_vote(connexion, vote):
    """Insère le vote. Retourne True si le vote est nouveau.

    `ON CONFLICT DO NOTHING` traduit la règle « un votant ne vote qu'une
    fois » : un votant rejoué par Kafka ne compte pas deux fois.
    """
    with connexion.cursor() as curseur:
        curseur.execute(
            """
            INSERT INTO votes (votant_id, candidat_id, temps_vote, vote)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (votant_id) DO NOTHING
            """,
            (vote["votant_id"], vote["candidat_id"], vote["temps_vote"],
             vote["vote"]),
        )
        insere = curseur.rowcount == 1
    connexion.commit()
    return insere


def compte_rendu_livraison(erreur, message):
    """Rappel de livraison Kafka : ne journalise que les échecs."""
    if erreur is not None:
        journal.error("Échec de l'envoi du vote : %s", erreur)


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
    consommateur = Consumer({
        "bootstrap.servers": config.KAFKA_SERVEURS,
        "group.id": "voting-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producteur = Producer({"bootstrap.servers": config.KAFKA_SERVEURS})

    nb_votes = 0
    try:
        candidats = charger_candidats(connexion)
        journal.info(
            "%d candidats chargés : %s",
            len(candidats), ", ".join(c["candidat_nom"] for c in candidats),
        )

        consommateur.subscribe([config.SUJET_VOTANTS])
        journal.info(
            "En attente de votants sur %s (Ctrl+C pour arrêter)",
            config.SUJET_VOTANTS,
        )

        depuis_dernier_commit = 0
        while True:
            message = consommateur.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                journal.error("Erreur Kafka : %s", message.error())
                break

            try:
                votant = json.loads(message.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as erreur:
                # Un message illisible ne doit pas arrêter le pipeline, mais il
                # ne doit pas non plus disparaître sans trace.
                journal.warning("Message illisible ignoré : %s", erreur)
                consommateur.commit(message, asynchronous=True)
                continue

            candidat = random.choice(candidats)
            vote = votant | candidat | {
                "temps_vote": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "vote": 1,
            }

            try:
                nouveau = enregistrer_vote(connexion, vote)
            except psycopg2.Error as erreur:
                journal.warning(
                    "Vote non enregistré pour %s : %s",
                    vote.get("votant_id"), erreur,
                )
                connexion.rollback()
                continue

            if nouveau:
                producteur.produce(
                    config.SUJET_VOTES,
                    key=vote["votant_id"],
                    value=json.dumps(vote),
                    on_delivery=compte_rendu_livraison,
                )
                producteur.poll(0)
                nb_votes += 1
                if nb_votes % 100 == 0:
                    journal.info("%d votes émis", nb_votes)

            depuis_dernier_commit += 1
            if depuis_dernier_commit >= VOTES_PAR_COMMIT:
                consommateur.commit(asynchronous=False)
                depuis_dernier_commit = 0

            if config.DELAI_ENTRE_VOTES > 0:
                time.sleep(config.DELAI_ENTRE_VOTES)

    except KeyboardInterrupt:
        journal.info("Arrêt demandé")
    except RuntimeError as erreur:
        journal.error("%s", erreur)
        return 1
    finally:
        # Sans ce bloc, les derniers votes restaient dans le tampon du
        # producteur et les offsets consommés n'étaient jamais validés.
        restants = producteur.flush(timeout=30)
        if restants:
            journal.warning("%d votes non confirmés par Kafka", restants)
        try:
            consommateur.commit(asynchronous=False)
        except Exception:
            pass  # rien à valider : aucun message consommé
        consommateur.close()
        connexion.close()
        journal.info("%d votes émis au total", nb_votes)

    return 0


if __name__ == "__main__":
    sys.exit(main())
