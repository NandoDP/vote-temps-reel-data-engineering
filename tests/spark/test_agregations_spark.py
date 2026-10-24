"""Tests des deux agrégations de `spark_streaming.py`, sur jeu de données statique.

POURQUOI CES TESTS SONT POSSIBLES
---------------------------------
Un DataFrame de streaming et un DataFrame batch exposent la même API. Comme
l'étape B a extrait les transformations en fonctions DataFrame → DataFrame, on
peut leur donner un DataFrame batch construit sur place : elles ne savent pas
d'où viennent leurs lignes, et s'en moquent.

C'est tout l'intérêt du refactoring précédent, et c'est la réponse à la question
« comment teste-t-on un pipeline de streaming ? ». On ne teste pas le streaming.
On teste le calcul, et on laisse au streaming le seul branchement.

Ces tests démarrent une vraie JVM. Ils sont lents, et c'est assumé : ils
vérifient ce que Spark fait réellement, pas ce qu'on croit qu'il fait.
"""

import datetime
import itertools
import json

import pytest

import spark_streaming


@pytest.fixture
def fabriquer_votes(tmp_path):
    """Fournit un fabricant de DataFrames de votes pour un test.

    Le fichier reste présent jusqu'à la fin du test, lorsque Spark a terminé
    l'évaluation paresseuse du DataFrame.
    """
    numero_fichier = itertools.count()

    def fabriquer(spark, messages):
        lignes = [
            json.dumps(message) if isinstance(message, dict) else message
            for message in messages
        ]
        chemin = tmp_path / f"messages-{next(numero_fichier)}.jsonl"
        chemin.write_text(
            "\n".join(lignes) + ("\n" if lignes else ""),
            encoding="utf-8",
        )
        brut = spark.read.text(str(chemin))
        return spark_streaming.deserialiser_votes(brut)

    return fabriquer


class TestComptageParCandidat:
    """Vérifie le calcul candidat et les colonnes utilisées par le tableau de bord."""

    def test_somme_votes_par_candidat(self, spark, fabriquer_votes):
        candidat1 = {"votant_id": "v1", "candidat_id": "c1", "candidat_nom": "Alice", "parti": "PartiA", "url_photo": "url1", "vote": 2}
        candidat2 = {"votant_id": "v2", "candidat_id": "c2", "candidat_nom": "Bob", "parti": "PartiB", "url_photo": "url2", "vote": 6}
        candidat3 = {"votant_id": "v3", "candidat_id": "c1", "candidat_nom": "Alice", "parti": "PartiA", "url_photo": "url1", "vote": 3}
        messages = [candidat1, candidat2, candidat3]
        votes = fabriquer_votes(spark, messages)
        resultats = spark_streaming.calculer_votes_par_candidat(votes).collect()
        assert {ligne.candidat_id: ligne.total_votes for ligne in resultats} == {
            "c1": 5,
            "c2": 6,
        }

    def test_conserve_les_colonnes_de_groupement(self, spark, fabriquer_votes):
        """Un parti distinct doit conserver sa propre ligne d'agrégat."""
        messages = [
            {
                "votant_id": "v1",
                "candidat_id": "c1",
                "candidat_nom": "Alice",
                "parti": "PartiA",
                "url_photo": "url1",
                "vote": 1,
            },
            {
                "votant_id": "v2",
                "candidat_id": "c1",
                "candidat_nom": "Alice",
                "parti": "PartiB",
                "url_photo": "url1",
                "vote": 1,
            },
        ]

        resultats = spark_streaming.calculer_votes_par_candidat(
            fabriquer_votes(spark, messages)
        ).collect()

        assert {
            (
                ligne.candidat_id,
                ligne.candidat_nom,
                ligne.parti,
                ligne.url_photo,
                ligne.total_votes,
            )
            for ligne in resultats
        } == {
            ("c1", "Alice", "PartiA", "url1", 1),
            ("c1", "Alice", "PartiB", "url1", 1),
        }


class TestFiltreDesMessagesInvalides:
    """Vérifie que la désérialisation élimine les messages sans les deux clés."""

    def test_rejette_messages_malformes_ou_sans_champ_requis(
        self, spark, fabriquer_votes
    ):
        messages = [
            '{"votant_id":"v1","candidat_id":"c1"}',
            '{"votant_id":"v2","candidat_id":"c2"',  # JSON syntaxiquement invalide
            '{"structure":"inattendue"}',
            '{"votant_id":"v3"}',
            '{"candidat_id":"c4"}',
        ]

        resultats = fabriquer_votes(spark, messages).collect()

        assert [(ligne.votant_id, ligne.candidat_id) for ligne in resultats] == [
            ("v1", "c1")
        ]


class TestTempsVote:
    """Vérifie les formats de date acceptés par le schéma Spark."""

    def test_parse_temps_vote(self, spark, fabriquer_votes):
        messages = [
            {
                "votant_id": "v1",
                "candidat_id": "c1",
                "temps_vote": "2025-10-15 12:34:56",
            },
            {
                "votant_id": "v2",
                "candidat_id": "c2",
                "temps_vote": "2025-10-15T12:34:56Z",
            },
            {
                "votant_id": "v3",
                "candidat_id": "c3",
                "temps_vote": "15/10/2025 12:34:56",
            },
        ]

        resultats = {
            ligne.votant_id: ligne.temps_vote
            for ligne in fabriquer_votes(spark, messages).collect()
        }

        assert resultats == {
            "v1": datetime.datetime(2025, 10, 15, 12, 34, 56),
            "v2": datetime.datetime(2025, 10, 15, 12, 34, 56),
            "v3": None,
        }


def test_participation_compte_les_bulletins_avec_vote_absent_ou_nul(
    spark, fabriquer_votes
):
    """La participation compte les lignes même si `vote` est absent ou nul."""
    messages = [
        {
            "votant_id": "v1",
            "candidat_id": "c1",
            "adresse": {"region": "Nord"},
            "vote": 1,
        },
        {
            "votant_id": "v2",
            "candidat_id": "c1",
            "adresse": {"region": "Nord"},
        },
        {
            "votant_id": "v3",
            "candidat_id": "c1",
            "adresse": {"region": "Sud"},
            "vote": None,
        },
    ]

    participation = spark_streaming.calculer_participation_par_region(
        fabriquer_votes(spark, messages)
    ).collect()

    assert {ligne.region: ligne.total_votes for ligne in participation} == {
        "Nord": 2,
        "Sud": 1,
    }
