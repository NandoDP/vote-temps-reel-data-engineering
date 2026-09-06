"""Étape 3 du pipeline.

Lit les votes depuis le sujet Kafka `votes_topic`, en tient deux agrégats
cumulés, et republie chaque mise à jour sur Kafka :

  - `aggregated_votes_per_candidate` : nombre de votes par candidat
  - `aggregated_turnout_by_location` : participation par région

Usage :
    python spark-streaming.py       # s'arrête avec Ctrl+C

Prérequis : un JDK 8, 11 ou 17 accessible via JAVA_HOME (Spark 3.5 ne
fonctionne pas avec un JDK plus récent). Le connecteur Kafka est téléchargé
au premier lancement depuis Maven Central : prévoir un accès réseau.
"""

import logging
import os
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import sum as somme
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
journal = logging.getLogger("spark-streaming")

# La version du connecteur Kafka doit correspondre à celle de Spark. La dériver
# de `pyspark.__version__` rend la dérive impossible : la version initiale
# épinglait 3.5.1 alors que `requirements.txt` installait PySpark 3.5.0.
CONNECTEUR_KAFKA = (
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
)

# Schéma des messages de `votes_topic`, tel que produit par `voting.py`
# (votant fusionné avec son candidat).
SCHEMA_VOTE = StructType([
    StructField("votant_id", StringType(), True),
    StructField("votant_nom", StringType(), True),
    StructField("date_naissance", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("nationalite", StringType(), True),
    StructField("numero_registre", StringType(), True),
    StructField("adresse", StructType([
        StructField("rue", StringType(), True),
        StructField("ville", StringType(), True),
        StructField("region", StringType(), True),
        StructField("pays", StringType(), True),
        StructField("code_postal", StringType(), True),
    ]), True),
    StructField("email", StringType(), True),
    StructField("numero_tel", StringType(), True),
    StructField("photo", StringType(), True),
    StructField("age_enregistre", IntegerType(), True),
    StructField("candidat_id", StringType(), True),
    StructField("candidat_nom", StringType(), True),
    StructField("parti", StringType(), True),
    StructField("biographie", StringType(), True),
    StructField("plateforme_campagne", StringType(), True),
    StructField("url_photo", StringType(), True),
    StructField("temps_vote", TimestampType(), True),
    StructField("vote", IntegerType(), True),
])


def preparer_hadoop_windows():
    """Rend `hadoop.dll` trouvable par la JVM sous Windows.

    Structured Streaming écrit ses points de reprise via le système de
    fichiers local de Hadoop, qui appelle du code natif. Sans `hadoop.dll` sur
    le chemin de recherche de la JVM, la requête échoue sur
    `UnsatisfiedLinkError: NativeIO$Windows.access0`. La JVM cherche cette
    bibliothèque dans les répertoires du `PATH`, où le sous-répertoire `bin`
    de `HADOOP_HOME` ne figure pas forcément — d'où cette amorce.
    """
    if sys.platform != "win32":
        return

    racine = os.environ.get("HADOOP_HOME")
    if not racine:
        journal.warning(
            "HADOOP_HOME n'est pas défini. Sous Windows, Spark a besoin de "
            "winutils.exe et hadoop.dll — voir la section « Dépannage » du "
            "README."
        )
        return

    binaires = Path(racine) / "bin"
    if not (binaires / "hadoop.dll").exists():
        journal.warning(
            "hadoop.dll est introuvable dans %s — voir la section "
            "« Dépannage » du README.", binaires,
        )
        return

    if str(binaires) not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{binaires}{os.pathsep}{os.environ.get('PATH', '')}"
        journal.info("%s ajouté au PATH pour y trouver hadoop.dll", binaires)


def creer_session():
    return (
        SparkSession.builder
        .appName("AnalyseElection")
        .master("local[*]")  # exécution locale, tous les cœurs disponibles
        .config("spark.jars.packages", CONNECTEUR_KAFKA)
        # Les horodatages sont émis en UTC par `voting.py` ; sans cette ligne
        # Spark les interpréterait dans le fuseau de la machine.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def lire_votes(spark):
    """Flux des votes, désérialisés et débarrassés des messages invalides."""
    brut = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_SERVEURS)
        .option("subscribe", config.SUJET_VOTES)
        .option("startingOffsets", "earliest")
        # Sans cette option, recréer le sujet Kafka fait échouer la requête au
        # lieu de la laisser repartir des offsets disponibles.
        .option("failOnDataLoss", "false")
        .load()
    )

    votes = (
        brut.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), SCHEMA_VOTE).alias("donnees"))
        .select("donnees.*")
    )

    # `from_json` ne valide rien : un message malformé produit une ligne
    # entièrement nulle, qui polluerait silencieusement les agrégats. On écarte
    # donc ce qui n'a pas les deux champs indispensables.
    return votes.filter(
        col("votant_id").isNotNull() & col("candidat_id").isNotNull()
    )


def ecrire_sur_kafka(df, sujet, nom_checkpoint):
    """Publie chaque mise à jour d'agrégat sur un sujet Kafka."""
    chemin = config.REPERTOIRE_CHECKPOINTS / nom_checkpoint
    chemin.mkdir(parents=True, exist_ok=True)
    return (
        df.selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_SERVEURS)
        .option("topic", sujet)
        # Chemin absolu résolu depuis l'emplacement du script : la version
        # initiale utilisait un chemin en dur propre à une machine, puis un
        # chemin relatif au répertoire courant.
        .option("checkpointLocation", chemin.as_uri())
        .outputMode("update")
        .start()
    )


def main():
    # Doit précéder la création de la session : le PATH est lu au lancement de
    # la JVM.
    preparer_hadoop_windows()

    # Garantit que `votes_topic` existe : la source Kafka de Spark échoue sur
    # un sujet absent, quel que soit l'ordre de démarrage des étages.
    config.garantir_sujets(journal)

    spark = creer_session()
    spark.sparkContext.setLogLevel("WARN")
    journal.info("Spark %s, connecteur %s", pyspark.__version__, CONNECTEUR_KAFKA)

    votes = lire_votes(spark)

    # Agrégats cumulés depuis le début du flux, sans fenêtre temporelle :
    # le tableau de bord affiche un total courant, pas une tranche de temps.
    # L'état reste borné parce que les clés de regroupement sont peu nombreuses
    # (autant que de candidats, puis que de régions).
    votes_par_candidat = votes.groupBy(
        "candidat_id", "candidat_nom", "parti", "url_photo"
    ).agg(somme("vote").alias("total_votes"))

    # `.count()` nomme sa colonne « count » ; la version initiale appelait
    # `.alias("total_votes")` sur le DataFrame, ce qui renomme le jeu de
    # données et non la colonne — le champ publié restait « count ».
    participation_par_region = (
        votes.groupBy(col("adresse.region").alias("region"))
        .count()
        .withColumnRenamed("count", "total_votes")
    )

    requetes = [
        ecrire_sur_kafka(
            votes_par_candidat,
            config.SUJET_VOTES_PAR_CANDIDAT,
            "votes_par_candidat",
        ),
        ecrire_sur_kafka(
            participation_par_region,
            config.SUJET_PARTICIPATION_PAR_REGION,
            "participation_par_region",
        ),
    ]

    journal.info(
        "Deux agrégations en cours vers %s et %s (Ctrl+C pour arrêter)",
        config.SUJET_VOTES_PAR_CANDIDAT, config.SUJET_PARTICIPATION_PAR_REGION,
    )

    try:
        # `awaitAnyTermination` rend la main dès qu'une requête s'arrête. La
        # version initiale attendait les requêtes l'une après l'autre : l'échec
        # de la seconde passait inaperçu tant que la première tournait.
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        journal.info("Arrêt demandé")
    finally:
        for requete in requetes:
            if requete.isActive:
                requete.stop()
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
