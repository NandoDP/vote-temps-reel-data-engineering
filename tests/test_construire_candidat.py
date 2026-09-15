"""EXERCICE — à écrire par Maodo. Aucune solution dans ce fichier.

Cible : `main.construire_candidat(profil, numero)`.

Va lire la fonction (`main.py`, vers la ligne 137) et la liste `PARTIS`
(vers la ligne 36) avant de commencer. Tout ce dont tu as besoin est là.

Comment travailler :
  1. Lance la suite, tu dois voir des échecs :
         .venv/Scripts/python -m pytest tests/test_construire_candidat.py -q
  2. Écris UN test, relance, regarde-le passer.
  3. Pour chaque test, pose-toi la question : « si je casse la fonction, ce
     test le voit-il ? » Si non, le test ne sert à rien. Vérifie-le pour de
     vrai : modifie `construire_candidat` dans `main.py`, relance, puis
     `git checkout -- main.py` pour revenir en arrière.

Rappels de syntaxe, pour ne pas avoir à chercher :
  - une fixture s'utilise en la nommant en argument : `def test_x(profil_randomuser):`
    (celle-ci est définie dans `conftest.py`, à la racine)
  - un cas attendu : `assert resultat == valeur_attendue`
  - plusieurs cas d'un coup :
        @pytest.mark.parametrize("entree,attendu", [(0, "a"), (1, "b")])
        def test_y(self, entree, attendu):
            ...
"""

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


class TestCeQueTuTrouves:
    """À toi de décider ce qui manque.

    Relis `construire_candidat`. Trois champs sont renvoyés en dur
    ("biographie", "plateforme_campagne"). Faut-il les tester ?

    Il n'y a pas de bonne réponse automatique. L'argument dans un sens :
    un test fige le contrat et attrape une suppression accidentelle.
    Dans l'autre : tester une constante ne fait que recopier le code, et le
    test devra changer à chaque fois que la valeur change — sans jamais rien
    avoir attrapé.

    Tranche, écris ce que tu décides ici en commentaire, et assume.
    """
