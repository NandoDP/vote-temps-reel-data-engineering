"""Régénère `docs/dashboard.png` à partir du tableau de bord en fonctionnement.

Outil de documentation, pas une dépendance du pipeline : Playwright n'est
volontairement pas dans `requirements.txt`.

    pip install playwright
    python docs/capture_dashboard.py

Prérequis : le pipeline tourne (voir README) et `streamlit run app.py` répond
sur le port 8501. Playwright pilote le Chrome déjà installé sur la machine
(`channel="chrome"`), il n'y a donc pas de navigateur à télécharger.

Pourquoi un script plutôt qu'une capture à la main : le tableau de bord met une
dizaine de secondes à afficher ses données, le temps de relire les sujets Kafka.
Une capture prise trop tôt ne montre que des blocs de chargement — et une
capture manuelle est à refaire à chaque retouche de l'interface.
"""

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

URL = "http://localhost:8501/"
SORTIE = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/dashboard.png")

# Fenêtre volontairement très haute : Streamlit défile dans un conteneur
# interne, donc `full_page` ne capture que la hauteur de la fenêtre.
LARGEUR, HAUTEUR = 1500, 2450


def main():
    SORTIE.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pilote:
        navigateur = pilote.chromium.launch(channel="chrome", headless=True)
        page = navigateur.new_page(viewport={"width": LARGEUR, "height": HAUTEUR})
        page.goto(URL, wait_until="load", timeout=60_000)

        # Attendre que les données soient réellement affichées, et non le
        # squelette de chargement.
        page.wait_for_selector("text=Candidat en tête", timeout=120_000)
        page.wait_for_selector("text=Participation par région", timeout=120_000)
        page.wait_for_timeout(3_000)  # laisser les graphiques se peindre

        # Masquer la barre d'outils Streamlit (Stop / Deploy), sans intérêt
        # dans une capture de documentation.
        page.add_style_tag(content="""
            header[data-testid="stHeader"], div[data-testid="stToolbar"],
            div[data-testid="stDecoration"] { display: none !important; }
        """)
        page.wait_for_timeout(500)

        page.screenshot(path=str(SORTIE), full_page=True)
        navigateur.close()

    print(f"capture écrite : {SORTIE} ({SORTIE.stat().st_size // 1024} Ko)")


if __name__ == "__main__":
    main()
