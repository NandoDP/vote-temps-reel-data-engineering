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

    def test_le_premier_candidat_recoit_le_premier_parti(self, profil_randomuser):
        candidat = main.construire_candidat(profil_randomuser, 0)
        assert candidat["parti"] == "Parti de Gauche"

    def test_le_parti_tourne_en_boucle_au_dela_du_nombre_de_partis(self, profil_randomuser):
        #  est-ce un comportement voulu, ou un effet de bord ?
        candidats = [main.construire_candidat(profil_randomuser, i) for i in range(4)]
        assert candidats[0]["parti"] == candidats[3]["parti"]
        # Réponse : c'est un comportement voulu, car le code est conçu pour

    # Remplace les deux tests ci-dessus par UN SEUL test paramétré qui
    # couvre les numéros 0, 1, 2, 3, 4, 5 et le parti attendu pour
    # chacun. Six cas, un bloc. C'est le but de `parametrize`.
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
        assert candidat["parti"] == parti_attendu