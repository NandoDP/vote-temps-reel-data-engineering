"""Configuration centralisée du pipeline.

Toutes les valeurs sont lues depuis l'environnement, avec des valeurs par défaut
qui correspondent au `docker-compose.yml` du dépôt. Copier `.env.example` vers
`.env` suffit à démarrer ; aucun identifiant n'est écrit en dur dans le code.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Racine du dépôt, résolue depuis l'emplacement de ce fichier et non depuis le
# répertoire courant : les scripts fonctionnent quel que soit l'endroit d'où on
# lance la commande.
RACINE = Path(__file__).resolve().parent

load_dotenv(RACINE / ".env")


def _entier(nom, defaut):
    valeur = os.getenv(nom)
    if valeur is None or valeur.strip() == "":
        return defaut
    try:
        return int(valeur)
    except ValueError:
        raise ValueError(f"La variable {nom} doit être un entier, reçu : {valeur!r}")


def _flottant(nom, defaut):
    valeur = os.getenv(nom)
    if valeur is None or valeur.strip() == "":
        return defaut
    try:
        return float(valeur)
    except ValueError:
        raise ValueError(f"La variable {nom} doit être un nombre, reçu : {valeur!r}")


# --- Kafka -----------------------------------------------------------------

KAFKA_SERVEURS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SUJET_VOTANTS = "voters_topic"
SUJET_VOTES = "votes_topic"
SUJET_VOTES_PAR_CANDIDAT = "aggregated_votes_per_candidate"
SUJET_PARTICIPATION_PAR_REGION = "aggregated_turnout_by_location"

SUJETS = [
    SUJET_VOTANTS,
    SUJET_VOTES,
    SUJET_VOTES_PAR_CANDIDAT,
    SUJET_PARTICIPATION_PAR_REGION,
]


def garantir_sujets(journal=None):
    """Crée les quatre sujets Kafka s'ils n'existent pas. Idempotent.

    Kafka crée bien un sujet à la première écriture, mais pas à la première
    lecture : la source Kafka de Spark échoue sur
    `UnknownTopicOrPartitionException` si `votes_topic` n'existe pas encore.
    Créer les sujets explicitement supprime cette dépendance à l'ordre de
    démarrage des étages — et vaut mieux, de toute façon, que de s'appuyer sur
    l'auto-création.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    administrateur = AdminClient({"bootstrap.servers": KAFKA_SERVEURS})
    existants = set(administrateur.list_topics(timeout=15).topics)
    manquants = [s for s in SUJETS if s not in existants]
    if not manquants:
        return []

    resultats = administrateur.create_topics([
        NewTopic(sujet, num_partitions=1, replication_factor=1)
        for sujet in manquants
    ])
    crees = []
    for sujet, future in resultats.items():
        try:
            future.result(timeout=30)
            crees.append(sujet)
        except Exception as erreur:
            # « Topic already exists » est bénin : deux étages ont pu démarrer
            # en même temps.
            if "already exists" not in str(erreur):
                raise
    if crees and journal is not None:
        journal.info("Sujets Kafka créés : %s", ", ".join(sorted(crees)))
    return crees

# --- PostgreSQL ------------------------------------------------------------

POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": _entier("POSTGRES_PORT", 5433),
    "dbname": os.getenv("POSTGRES_DB", "voting"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


def connexion_postgres():
    """Ouvre une connexion PostgreSQL. À utiliser dans un `with`, qui la ferme."""
    import psycopg2

    return psycopg2.connect(**POSTGRES)


# --- Paramètres de la simulation ------------------------------------------

# Nombre de votants générés par `main.py`.
NB_VOTANTS = _entier("NB_VOTANTS", 1000)

# Nombre de candidats générés par `main.py`.
NB_CANDIDATS = _entier("NB_CANDIDATS", 3)

# randomuser.me accepte jusqu'à 5000 profils par requête. Récupérer par lots
# évite un aller-retour HTTP par votant, qui dominait entièrement le temps
# d'exécution (~0,7 s par appel, soit près de deux heures pour 10 000 votants).
TAILLE_LOT_PROFILS = _entier("TAILLE_LOT_PROFILS", 500)

# Pause entre deux votes dans `voting.py`, en secondes. 0 = débit maximal.
# Une valeur non nulle sert à observer le tableau de bord se remplir lentement.
DELAI_ENTRE_VOTES = _flottant("DELAI_ENTRE_VOTES", 0.0)

# --- Spark -----------------------------------------------------------------

# Répertoire des points de reprise Spark, résolu depuis la racine du dépôt.
REPERTOIRE_CHECKPOINTS = RACINE / "checkpoints"
