"""Tests de la règle « un votant ne vote qu'une fois », au rejeu Kafka.

Kafka rejoue les messages : après un redémarrage, `voting.py` relit des votes
déjà enregistrés. `enregistrer_vote` doit alors renvoyer `False` pour que le
vote ne soit pas republié et compté deux fois dans l'agrégat.

Ces tests n'ouvrent aucune base de données. Ils remplacent la connexion par un
**double de test** : un objet qui expose la même surface que `psycopg2`
(`cursor()`, `execute()`, `rowcount`, `commit()`) et simule `ON CONFLICT DO
NOTHING` avec un simple ensemble.

⚠️ Ce que ces tests prouvent, et ce qu'ils ne prouvent pas.
    Ils prouvent que *notre* code interprète correctement `rowcount` et
    n'oublie pas de valider la transaction. Ils **ne prouvent pas** que la
    requête SQL est juste : si `ON CONFLICT (votant_id)` visait la mauvaise
    colonne, ce double ne le verrait pas. Vérifier cela demande une vraie base,
    donc un test d'intégration — un autre étage de la pyramide, à ajouter plus
    tard avec un conteneur PostgreSQL jetable.
    Un test qui ment sur sa portée est pire que pas de test du tout.
"""

import pytest

import voting


class CurseurFactice:
    """Imite juste ce que `enregistrer_vote` utilise d'un curseur psycopg2."""

    def __init__(self, base):
        self.base = base
        self.rowcount = 0
        self.requetes = []

    # `with connexion.cursor() as curseur:` exige ces deux méthodes.
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, parametres):
        self.requetes.append((sql, parametres))
        votant_id = parametres[0]
        # Le cœur de la simulation : c'est la clé primaire qui décide.
        if votant_id in self.base.votants_ayant_vote:
            self.rowcount = 0          # ON CONFLICT DO NOTHING
        else:
            self.base.votants_ayant_vote.add(votant_id)
            self.rowcount = 1


class ConnexionFactice:
    def __init__(self):
        self.votants_ayant_vote = set()
        self.commits = 0
        self.dernier_curseur = None

    def cursor(self):
        self.dernier_curseur = CurseurFactice(self)
        return self.dernier_curseur

    def commit(self):
        self.commits += 1


@pytest.fixture
def connexion():
    return ConnexionFactice()


@pytest.fixture
def vote():
    return {
        "votant_id": "11111111-2222-3333-4444-555555555555",
        "candidat_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "temps_vote": "2026-09-15T10:00:00+00:00",
        "vote": 1,
    }


def test_un_vote_nouveau_est_accepte(connexion, vote):
    assert voting.enregistrer_vote(connexion, vote) is True


def test_le_meme_vote_rejoue_est_refuse(connexion, vote):
    """Le scénario réel : Kafka redistribue un message déjà traité."""
    voting.enregistrer_vote(connexion, vote)

    assert voting.enregistrer_vote(connexion, vote) is False


def test_dix_rejeux_ne_comptent_toujours_qu_une_voix(connexion, vote):
    resultats = [voting.enregistrer_vote(connexion, vote) for _ in range(10)]

    assert resultats.count(True) == 1
    assert len(connexion.votants_ayant_vote) == 1


def test_un_votant_ne_peut_pas_changer_d_avis(connexion, vote):
    """La contrainte porte sur le votant, pas sur le couple votant/candidat.

    Rejouer le même votant avec un *autre* candidat doit aussi être refusé,
    sinon un votant pourrait voter une fois par candidat.
    """
    voting.enregistrer_vote(connexion, vote)

    autre_candidat = dict(vote, candidat_id="99999999-9999-9999-9999-999999999999")

    assert voting.enregistrer_vote(connexion, autre_candidat) is False


def test_deux_votants_distincts_passent_tous_les_deux(connexion, vote):
    """Le garde-fou du garde-fou : vérifier qu'on ne rejette pas tout.

    Sans ce test, une fonction qui renverrait toujours `False` passerait les
    quatre tests précédents.
    """
    autre_votant = dict(vote, votant_id="00000000-0000-0000-0000-000000000000")

    assert voting.enregistrer_vote(connexion, vote) is True
    assert voting.enregistrer_vote(connexion, autre_votant) is True


def test_la_transaction_est_validee_meme_quand_le_vote_est_refuse(connexion, vote):
    """Sans `commit`, la transaction resterait ouverte et tiendrait un verrou.

    Le cas du rejeu est le plus facile à oublier, parce que « rien n'a été
    inséré » donne l'impression qu'il n'y a rien à valider.
    """
    voting.enregistrer_vote(connexion, vote)
    voting.enregistrer_vote(connexion, vote)

    assert connexion.commits == 2


def test_la_contrainte_porte_bien_sur_le_votant(connexion, vote):
    """Vérifie la requête émise, pas seulement son effet simulé.

    C'est la parade partielle à la limite annoncée en tête de fichier : on ne
    prouve pas que PostgreSQL fera ce qu'on croit, mais on détecte au moins un
    changement de colonne dans le `ON CONFLICT`.
    """
    voting.enregistrer_vote(connexion, vote)

    sql, _ = connexion.dernier_curseur.requetes[0]
    normalise = " ".join(sql.split()).lower()

    assert "on conflict (votant_id) do nothing" in normalise
