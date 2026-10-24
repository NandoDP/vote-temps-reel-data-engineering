import pytest

import main


class TestChampsDuCandidat:
    """Ce que la fonction copie depuis le profil."""

    def test_l_identifiant_vient_du_profil(self, profil_randomuser):
        candidat = main.construire_candidat(profil_randomuser, 0)
        assert candidat["candidat_id"] == profil_randomuser["login"]["uuid"]

    def test_le_nom_complet_est_compose_du_prenom_et_du_nom(self, profil_randomuser):
        candidat = main.construire_candidat(profil_randomuser, 0)
        assert candidat["candidat_nom"] == "Awa Ndiaye"


class TestAttributionDuParti:
    """Le seul vrai calcul de la fonction : `PARTIS[numero % len(PARTIS)]`.

    C'est là qu'est l'intérêt. Trois partis, mais `numero` n'est pas borné :
    la fonction doit donc tourner en boucle. Une erreur ici ne lève aucune
    exception — elle produit une répartition silencieusement fausse.
    """

    @pytest.mark.parametrize("numero,parti_attendu", [
        (0, "Parti de Gauche"),
        (1, "Parti de Droite"),
        (2, "Parti du Milieu"),
        (3, "Parti de Gauche"),
        (4, "Parti de Droite"),
        (5, "Parti du Milieu"),
    ])
    def test_le_parti_est_correctement_attribue(self, profil_randomuser, numero, parti_attendu):
        candidat = main.construire_candidat(profil_randomuser, numero)
        #  est-ce un comportement voulu, ou un effet de bord ?
        assert candidat["parti"] == parti_attendu
        # Réponse : c'est un comportement voulu, car le code est conçu pour tourner en boucle. Le modulo est là pour ça. Le test est donc légitime.


class TestUrlPhoto:
    """Le champ `url_photo` est un URL, mais la fonction ne le construit pas : elle
    le copie depuis le profil. Le test vérifie juste que la copie est correcte.
    """

    def test_l_url_de_la_photo_est_copiee_depuis_le_profil(self, profil_randomuser):
        candidat = main.construire_candidat(profil_randomuser, 0)
        assert candidat["url_photo"] == profil_randomuser["picture"]["large"]
