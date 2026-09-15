"""Tests de la transformation « profil randomuser.me → votant ».

C'est la fonction la plus exposée du pipeline : elle est la seule à réécrire la
géographie des profils pour situer la simulation au Sénégal. Une erreur ici ne
lève aucune exception — elle produit silencieusement une agrégation
« participation par région » incohérente. C'est précisément le défaut qui avait
été trouvé dans ce dépôt : la région restait celle du profil britannique.

Un test qui échoue est plus utile qu'un commentaire qui prévient.
"""

import pytest

import main

# Les 14 régions administratives du Sénégal. Écrite ici plutôt que déduite de
# `main.REGION_PAR_DEPARTEMENT`, cette liste sert de référence indépendante :
# un test qui recalcule sa valeur attendue à partir du code testé ne teste rien.
REGIONS_OFFICIELLES = {
    "Dakar", "Diourbel", "Fatick", "Kaffrine", "Kaolack", "Kédougou",
    "Kolda", "Louga", "Matam", "Saint-Louis", "Sédhiou", "Tambacounda",
    "Thiès", "Ziguinchor",
}


class TestGeographie:
    """Ce que `construire_votant` doit garantir sur l'adresse."""

    def test_le_pays_est_reecrit_en_senegal(self, profil_randomuser):
        votant = main.construire_votant(profil_randomuser)
        assert votant["adresse"]["pays"] == "Sénégal"

    def test_la_region_britannique_du_profil_ne_fuit_pas(self, profil_randomuser):
        """Le cœur du sujet : « Greater Manchester » ne doit jamais ressortir.

        C'est le test qui aurait attrapé le défaut d'origine. Il ne vérifie pas
        une valeur précise — la ville est tirée au hasard — mais une propriété :
        rien de la géographie d'entrée ne doit survivre.
        """
        votant = main.construire_votant(profil_randomuser)
        adresse = votant["adresse"]

        assert adresse["region"] != "Greater Manchester"
        assert adresse["ville"] != "Manchester"
        assert adresse["region"] in REGIONS_OFFICIELLES

    def test_la_region_correspond_bien_au_departement_tire(self, profil_randomuser):
        """La région n'est pas tirée au hasard : elle découle du département."""
        votant = main.construire_votant(profil_randomuser)
        adresse = votant["adresse"]

        assert adresse["region"] == main.REGION_PAR_DEPARTEMENT[adresse["ville"]]

    def test_la_rue_est_conservee_telle_quelle(self, profil_randomuser):
        """Tout n'est pas réécrit : la docstring promet que le reste est gardé.

        Un test de non-régression sur ce qui ne doit *pas* changer vaut autant
        qu'un test sur ce qui change.
        """
        votant = main.construire_votant(profil_randomuser)
        assert votant["adresse"]["rue"] == "42 Baker Street"


class TestTableDepartementRegion:
    """Invariants de la table elle-même, indépendamment de tout profil."""

    def test_les_45_departements_sont_presents(self):
        assert len(main.DEPARTEMENTS) == 45

    @pytest.mark.parametrize("departement", sorted(main.REGION_PAR_DEPARTEMENT))
    def test_chaque_departement_pointe_vers_une_region_officielle(self, departement):
        """`parametrize` engendre un test par département : 45 cas, un seul bloc.

        L'intérêt par rapport à une boucle `for` avec un seul `assert` : pytest
        signale *quel* département est fautif au lieu de s'arrêter au premier.
        """
        assert main.REGION_PAR_DEPARTEMENT[departement] in REGIONS_OFFICIELLES

    def test_les_14_regions_sont_toutes_couvertes(self):
        """Aucune région ne doit être orpheline : sinon elle n'apparaîtrait
        jamais dans le tableau de bord, et personne ne le remarquerait."""
        couvertes = set(main.REGION_PAR_DEPARTEMENT.values())
        assert couvertes == REGIONS_OFFICIELLES


class TestIdentite:
    """Les champs qui servent de clé en base ou dans Kafka."""

    def test_l_identifiant_du_votant_vient_du_profil(self, profil_randomuser):
        votant = main.construire_votant(profil_randomuser)
        assert votant["votant_id"] == profil_randomuser["login"]["uuid"]

    def test_le_numero_de_registre_retombe_sur_l_uuid_si_absent(self, profil_randomuser):
        """randomuser.me renvoie parfois `id.value` à `null`.

        Le code prévoit ce repli ; sans test, une régression le supprimerait
        sans bruit et `numero_registre` deviendrait `None` pour une partie des
        votants.
        """
        profil_randomuser["id"]["value"] = None

        votant = main.construire_votant(profil_randomuser)

        assert votant["numero_registre"] == profil_randomuser["login"]["uuid"]
