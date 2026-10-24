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

    TODO 1 — Reconstituer ce dictionnaire.

    Va lire `voting.py` (cherche la ligne qui construit la variable `vote`,
    autour de la ligne 143). Tu y verras que le message publié est la fusion de
    trois choses. Reconstitue les trois ici, en réutilisant les fonctions de
    `main.py` qui produisent les deux premières — surtout ne recopie pas la
    liste des champs à la main : un test qui répète le code testé ne teste
    rien, il se contente de vérifier que tu sais copier-coller.

    Note pour plus tard, à ne pas traiter maintenant : dans le vrai pipeline, la
    partie « candidat » du message ne sort pas directement de `main.py`, elle
    fait un aller-retour par PostgreSQL. On approxime ici. Demande-toi, une fois
    l'exercice fini, ce que cette approximation laisse passer.
    """
    # pytest.fail("TODO 1 : à écrire")
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
        """TODO 2 — Comparer les deux ensembles de noms de champs.

        Côté message : les clés du dictionnaire.
        Côté schéma : `spark_streaming.SCHEMA_VOTE` expose ses champs. À toi de
        trouver l'accesseur — `dir()` sur l'objet, ou la doc de `StructType`.
        Il y a plusieurs chemins possibles ; n'importe lequel fait l'affaire.

        Attention au sens de la comparaison. Une inclusion dans un seul sens ne
        suffit pas : les deux dérives sont dangereuses, et pas pour la même
        raison. Un champ publié mais absent du schéma est perdu ; un champ
        déclaré mais plus publié vaut `null`. Demande-toi laquelle des deux ce
        test doit attraper — puis attrape les deux.

        TODO 3 — Rendre l'échec lisible.

        `assert a == b` sur deux ensembles de dix-neuf noms produit un mur de
        texte dans lequel on ne voit pas ce qui manque. Fais en sorte que
        l'échec nomme le ou les champs fautifs, et de quel côté ils sont.
        Un test dont personne ne sait lire l'échec est un test qu'on désactive.
        """
        # pytest.fail("TODO 2 et 3 : à écrire")
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
        """TODO 4 — Même comparaison, un cran plus bas.

        Il te faut d'abord récupérer, dans `SCHEMA_VOTE`, le champ nommé
        `adresse`, puis le `StructType` qu'il contient. Un `StructField` a un
        nom et un type ; c'est le type qui t'intéresse ici.
        """
        # pytest.fail("TODO 4 : à écrire")
        adresse_champs = set(message_publie["adresse"].keys())
        schema_adresse = next(f for f in spark_streaming.SCHEMA_VOTE.fields if f.name == "adresse")
        assert isinstance(schema_adresse.dataType, spark_streaming.StructType), "Le champ 'adresse' n'est pas un StructType"
        schema_adresse_champs = set(f.name for f in schema_adresse.dataType.fields)
        assert adresse_champs == schema_adresse_champs, (
            f"Champs manquants dans l'adresse du message : {schema_adresse_champs - adresse_champs}, "
            f"Champs manquants dans l'adresse du schéma : {adresse_champs - schema_adresse_champs}"
        )


class TestTypesDesChampsQuiComptent:
    """Bonus, à ne faire qu'une fois les quatre premiers TODO validés.

    Jusqu'ici on n'a comparé que des NOMS. Un nom juste avec un type faux casse
    tout aussi silencieusement : `from_json` met `null` quand il n'arrive pas à
    convertir la valeur reçue vers le type déclaré.
    """

    def test_les_champs_utilises_par_les_agregats_ont_un_type_exploitable(self, message_publie):
        """TODO 5 — Verrouiller le type des champs dont dépendent les agrégats.

        Trois champs méritent ce verrou, et un seul est vraiment intéressant :
        - `vote`, parce qu'on en fait une somme ;
        - `age_enregistre`, du même acabit ;
        - `temps_vote`. Regarde son type dans `SCHEMA_VOTE`, puis regarde la
          valeur que `voting.py` met dedans (ligne 144). Ce n'est pas le même
          monde. Vérifie par toi-même si Spark s'en sort, et écris le test qui
          documente la réponse que tu auras trouvée — pas celle que tu supposes.
        """
        # pytest.fail("TODO 5 : à écrire")
        vote_champ = next(f for f in spark_streaming.SCHEMA_VOTE.fields if f.name == "vote")
        age_enregistre_champ = next(f for f in spark_streaming.SCHEMA_VOTE.fields if f.name == "age_enregistre")
        temps_vote_champ = next(f for f in spark_streaming.SCHEMA_VOTE.fields if f.name == "temps_vote")

        assert isinstance(vote_champ.dataType, spark_streaming.IntegerType), "Le champ 'vote' n'a pas le bon type"
        assert isinstance(age_enregistre_champ.dataType, spark_streaming.IntegerType), "Le champ 'age_enregistre' n'a pas le bon type"
        assert isinstance(temps_vote_champ.dataType, spark_streaming.TimestampType), "Le champ 'temps_vote' n'a pas le bon type"
