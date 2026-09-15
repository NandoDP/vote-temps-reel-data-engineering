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
        # TODO 1 — appelle main.construire_candidat(profil_randomuser, 0)
        #          et vérifie que "candidat_id" vaut bien l'uuid du profil.
        #          Modèle : regarde test_l_identifiant_du_votant_vient_du_profil
        #          dans tests/test_construire_votant.py.
        pytest.fail("à écrire")

    def test_le_nom_complet_est_compose_du_prenom_et_du_nom(self, profil_randomuser):
        # TODO 2 — le profil donne name.first = "Awa" et name.last = "Ndiaye".
        #          Qu'attends-tu dans "candidat_nom" ? Écris-le en dur : un test
        #          qui recalcule la valeur avec la même f-string que le code
        #          testé ne teste rien (il passerait même si le code était faux).
        pytest.fail("à écrire")


class TestAttributionDuParti:
    """Le seul vrai calcul de la fonction : `PARTIS[numero % len(PARTIS)]`.

    C'est là qu'est l'intérêt. Trois partis, mais `numero` n'est pas borné :
    la fonction doit donc tourner en boucle. Une erreur ici ne lève aucune
    exception — elle produit une répartition silencieusement fausse.
    """

    def test_le_premier_candidat_recoit_le_premier_parti(self, profil_randomuser):
        # TODO 3
        pytest.fail("à écrire")

    def test_le_parti_tourne_en_boucle_au_dela_du_nombre_de_partis(self, profil_randomuser):
        # TODO 4 — il y a 3 partis. Les candidats 0 et 3 doivent donc recevoir
        #          le MÊME parti. Vérifie-le.
        #          Puis demande-toi : est-ce un comportement voulu, ou un effet
        #          de bord ? Écris ta réponse en une ligne de commentaire —
        #          c'est la vraie question d'ingénierie de ce fichier.
        pytest.fail("à écrire")

    # TODO 5 — remplace les deux tests ci-dessus par UN SEUL test paramétré qui
    #          couvre les numéros 0, 1, 2, 3, 4, 5 et le parti attendu pour
    #          chacun. Six cas, un bloc. C'est le but de `parametrize`.
    #          Supprime ensuite les deux tests devenus redondants.


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
