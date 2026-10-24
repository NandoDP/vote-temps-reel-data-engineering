"""Test de contrat : SCHEMA_VOTE doit décrire le message réellement publié.

POURQUOI CE TEST EXISTE
-----------------------
`spark_streaming.lire_votes` désérialise chaque message Kafka avec
`from_json(col("json"), SCHEMA_VOTE)`. Or `from_json` ne valide rien : un champ
déclaré dans le schéma mais absent du message est rempli à `null`, sans
avertissement, sans exception, sans ligne dans les journaux.

Conséquence concrète : le jour où quelqu'un renomme `parti` en `parti_politique`
dans `main.py`, le pipeline continue de tourner. Simplement, la colonne `parti`
vaut `null` pour tous les votes, le `groupBy("candidat_id", "candidat_nom",
"parti", "url_photo")` regroupe sur un `null` constant, et le tableau de bord
affiche des résultats faux que personne ne sait expliquer.

Ce test transforme cette panne silencieuse en échec bruyant, à l'endroit et au
moment où elle est la moins chère à corriger : au `git push`.

Il ne démarre aucune session Spark. Un `StructType` est un objet Python
ordinaire, qui s'introspecte comme n'importe quel autre.
"""

import pytest
import datetime

import main
import spark_streaming


@pytest.fixture
def message_publie(profil_randomuser):
    """Le message tel que `voting.py` le publie sur `votes_topic`.
    """
    votant = main.construire_votant(profil_randomuser)
    candidat = main.construire_candidat(profil_randomuser, 0)
    return votant | candidat | {
        "vote": 1,
        "temps_vote": datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }


class TestContratDesChampsDePremierNiveau:
    """Le schéma et le message doivent déclarer exactement les mêmes champs."""

    def test_le_schema_declare_exactement_les_champs_publies(self, message_publie):
        message_champs = set(message_publie.keys())
        schema_champs = set(f.name for f in spark_streaming.SCHEMA_VOTE.fields)
        assert message_champs == schema_champs, (
            f"Champs manquants dans le message : {schema_champs - message_champs}, "
            f"Champs manquants dans le schéma : {message_champs - schema_champs}"
        )


class TestContratDuSousObjetAdresse:
    """`adresse` est un `StructType` imbriqué, et la participation en dépend.

    `participation_par_region` regroupe sur `adresse.region`. Si ce sous-objet
    dérive, c'est l'agrégat de participation entier qui devient faux.
    """

    def test_le_schema_de_l_adresse_declare_exactement_les_champs_publies(
        self, message_publie
    ):
        adresse_champs = set(message_publie["adresse"].keys())
        schema_adresse = next(f for f in spark_streaming.SCHEMA_VOTE.fields if f.name == "adresse")
        assert isinstance(schema_adresse.dataType, spark_streaming.StructType), "Le champ 'adresse' n'est pas un StructType"
        schema_adresse_champs = set(f.name for f in schema_adresse.dataType.fields)
        assert adresse_champs == schema_adresse_champs, (
            f"Champs manquants dans l'adresse du message : {schema_adresse_champs - adresse_champs}, "
            f"Champs manquants dans l'adresse du schéma : {adresse_champs - schema_adresse_champs}"
        )


class TestTypesDesChampsQuiComptent:

    def test_les_champs_utilises_par_les_agregats_ont_un_type_exploitable(self, message_publie):
        vote_champ = next(f for f in spark_streaming.SCHEMA_VOTE.fields if f.name == "vote")
        age_enregistre_champ = next(f for f in spark_streaming.SCHEMA_VOTE.fields if f.name == "age_enregistre")
        temps_vote_champ = next(f for f in spark_streaming.SCHEMA_VOTE.fields if f.name == "temps_vote")

        assert isinstance(vote_champ.dataType, spark_streaming.IntegerType), "Le champ 'vote' n'a pas le bon type"
        assert isinstance(age_enregistre_champ.dataType, spark_streaming.IntegerType), "Le champ 'age_enregistre' n'a pas le bon type"
        assert isinstance(temps_vote_champ.dataType, spark_streaming.TimestampType), "Le champ 'temps_vote' n'a pas le bon type"
