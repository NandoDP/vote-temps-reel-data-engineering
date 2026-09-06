"""Étape 4 du pipeline : tableau de bord temps réel.

Consomme les deux sujets Kafka d'agrégats produits par `spark-streaming.py`,
complète avec les compteurs de PostgreSQL, et rafraîchit l'affichage
périodiquement.

Usage :
    streamlit run app.py
"""

import time
import uuid
from contextlib import closing

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
import simplejson as json
import streamlit as st
from confluent_kafka import Consumer
from streamlit_autorefresh import st_autorefresh

import config

# Backend sans fenêtre : Streamlit rend les figures en image, il n'y a pas
# d'interface graphique disponible côté serveur.
matplotlib.use("Agg")

# Les graphiques doivent s'accorder au thème sombre défini dans
# `.streamlit/config.toml`, sinon ils apparaissent comme des rectangles blancs.
plt.style.use("dark_background")
FOND_GRAPHIQUE = "#1A1A1A"
plt.rcParams.update({
    "figure.facecolor": FOND_GRAPHIQUE,
    "axes.facecolor": FOND_GRAPHIQUE,
    "savefig.facecolor": FOND_GRAPHIQUE,
    "axes.edgecolor": "#555555",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "text.color": "#FAFAFA",
    "axes.labelcolor": "#BBBBBB",
    "xtick.color": "#BBBBBB",
    "ytick.color": "#BBBBBB",
})

st.set_page_config(
    page_title="Élections en temps réel",
    page_icon="🗳️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Durée maximale de lecture d'un sujet Kafka, en secondes.
DUREE_LECTURE_KAFKA = 8.0


def format_milliers(nombre):
    """3391 -> « 3 391 » : espace insécable comme séparateur, usage français."""
    return f"{int(nombre):,}".replace(",", " ")


def lire_sujet(sujet):
    """Lit l'intégralité d'un sujet Kafka et retourne les messages décodés.

    Un identifiant de groupe neuf à chaque appel, combiné à
    `auto.offset.reset=earliest`, garantit de relire tout le sujet : les
    agrégats sont publiés en mode `update`, donc l'état courant se reconstruit
    à partir de l'ensemble des messages.
    """
    consommateur = Consumer({
        "bootstrap.servers": config.KAFKA_SERVEURS,
        "group.id": f"tableau-de-bord-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    messages = []
    try:
        consommateur.subscribe([sujet])
        debut = time.monotonic()
        sondages_vides = 0
        while time.monotonic() - debut < DUREE_LECTURE_KAFKA:
            message = consommateur.poll(timeout=1.0)
            if message is None:
                # Deux sondages vides d'affilée : le sujet est épuisé.
                sondages_vides += 1
                if sondages_vides >= 2:
                    break
                continue
            if message.error():
                continue
            sondages_vides = 0
            try:
                messages.append(json.loads(message.value().decode("utf-8")))
            except (ValueError, UnicodeDecodeError):
                continue
    finally:
        # Sans fermeture explicite, chaque rafraîchissement laissait un
        # consommateur et son fil de fond derrière lui.
        consommateur.close()
    return messages


@st.cache_data(ttl=10, show_spinner=False)
def compteurs_postgres():
    """Nombre de votants et de candidats en base.

    Le cache est limité à dix secondes : la version initiale mettait ces
    compteurs en cache indéfiniment sur un tableau de bord temps réel.
    """
    # `closing` est indispensable : le gestionnaire de contexte natif de
    # psycopg2 valide la transaction mais ne ferme pas la connexion, ce qui
    # en accumulerait une par rafraichissement.
    with closing(config.connexion_postgres()) as connexion:
        with connexion.cursor() as curseur:
            curseur.execute("SELECT count(*) FROM votants")
            nb_votants = curseur.fetchone()[0]
            curseur.execute("SELECT count(*) FROM candidats")
            nb_candidats = curseur.fetchone()[0]
            curseur.execute("SELECT count(*) FROM votes")
            nb_votes = curseur.fetchone()[0]
    return nb_votants, nb_candidats, nb_votes


def graphique_barres(resultats):
    figure, axes = plt.subplots(figsize=(6, 4), facecolor=FOND_GRAPHIQUE)
    axes.set_facecolor(FOND_GRAPHIQUE)
    couleurs = plt.cm.viridis(np.linspace(0, 1, len(resultats)))
    axes.bar(resultats["candidat_nom"], resultats["total_votes"], color=couleurs)
    axes.set_xlabel("Candidat")
    axes.set_ylabel("Total des votes")
    axes.set_title("Nombre de votes par candidat")
    axes.tick_params(axis="x", labelrotation=20)
    for etiquette in axes.get_xticklabels():
        etiquette.set_horizontalalignment("right")
    figure.tight_layout()
    return figure


def graphique_anneau(resultats):
    figure, axes = plt.subplots(figsize=(6, 4), facecolor=FOND_GRAPHIQUE)
    axes.set_facecolor(FOND_GRAPHIQUE)
    tranches, _, _ = axes.pie(
        resultats["total_votes"],
        autopct="%1.1f %%",
        pctdistance=0.78,
        # Les tranches du jeu de couleurs sont claires : un texte blanc y
        # devenait illisible.
        textprops={"color": "#1A1A1A", "fontweight": "bold"},
        startangle=90,
        counterclock=False,
        wedgeprops={"width": 0.42},
    )
    axes.axis("equal")
    axes.set_title("Répartition des votes")
    # Légende plutôt qu'étiquettes collées aux tranches, qui chevauchaient le
    # titre dès que les noms de candidats étaient longs.
    axes.legend(
        tranches, resultats["candidat_nom"],
        loc="center left", bbox_to_anchor=(0.98, 0.5), frameon=False,
    )
    figure.tight_layout()
    return figure


def afficher_tableau_pagine(donnees, cle):
    """Affiche un tableau paginé, avec tri optionnel."""
    haut = st.columns(3)
    with haut[0]:
        trier = st.radio(
            "Trier", options=["Non", "Oui"], horizontal=True, key=f"tri-{cle}"
        )
    if trier == "Oui":
        with haut[1]:
            colonne = st.selectbox(
                "Trier par", options=donnees.columns, key=f"colonne-{cle}"
            )
        with haut[2]:
            sens = st.radio(
                "Sens", options=["Croissant", "Décroissant"],
                horizontal=True, key=f"sens-{cle}",
            )
        donnees = donnees.sort_values(
            by=colonne, ascending=sens == "Croissant", ignore_index=True
        )

    zone_tableau = st.container()
    bas = st.columns((4, 1, 1))
    with bas[2]:
        taille_page = st.selectbox(
            "Lignes par page", options=[10, 25, 50, 100], key=f"taille-{cle}"
        )
    with bas[1]:
        nb_pages = max(1, -(-len(donnees) // taille_page))  # division arrondie au sup.
        page = st.number_input(
            "Page", min_value=1, max_value=nb_pages, step=1, key=f"page-{cle}"
        )
    with bas[0]:
        st.markdown(f"Page **{page}** sur **{nb_pages}**")

    debut = (page - 1) * taille_page
    zone_tableau.dataframe(
        donnees.iloc[debut:debut + taille_page],
        use_container_width=True,
        hide_index=True,
    )


def afficher_tableau_de_bord():
    st.title("Tableau de bord des élections en temps réel")
    st.caption(
        f"Dernière actualisation : {time.strftime('%Y-%m-%d %H:%M:%S')} — "
        "données synthétiques, scrutin simulé"
    )

    try:
        nb_votants, nb_candidats, nb_votes = compteurs_postgres()
    except psycopg2.Error as erreur:
        st.error(
            f"PostgreSQL injoignable sur {config.POSTGRES['host']}:"
            f"{config.POSTGRES['port']}. Les conteneurs sont-ils démarrés "
            f"(`docker compose up -d`) ?\n\n{erreur}"
        )
        return

    colonnes = st.columns(3)
    colonnes[0].metric("Votants inscrits", format_milliers(nb_votants))
    colonnes[1].metric("Candidats", nb_candidats)
    colonnes[2].metric("Votes enregistrés", format_milliers(nb_votes))

    st.divider()

    donnees_candidats = lire_sujet(config.SUJET_VOTES_PAR_CANDIDAT)
    if not donnees_candidats:
        st.info(
            "Aucun agrégat sur le sujet "
            f"`{config.SUJET_VOTES_PAR_CANDIDAT}` pour le moment. "
            "Vérifier que `voting.py` et `spark-streaming.py` tournent ; "
            "les premiers agrégats apparaissent après quelques secondes."
        )
        return

    resultats = pd.DataFrame(donnees_candidats)
    # Les agrégats sont publiés en mode `update` : plusieurs messages existent
    # par candidat, le total le plus élevé est le plus récent.
    resultats = resultats.loc[
        resultats.groupby("candidat_id")["total_votes"].idxmax()
    ].reset_index(drop=True)
    resultats = resultats.sort_values("total_votes", ascending=False)

    tete = resultats.iloc[0]
    st.subheader("Candidat en tête")
    gauche, droite = st.columns([1, 3])
    with gauche:
        if tete.get("url_photo"):
            st.image(tete["url_photo"], width=160)
    with droite:
        st.markdown(f"### {tete['candidat_nom']}")
        st.markdown(f"**{tete['parti']}**")
        st.markdown(f"**{format_milliers(tete['total_votes'])} votes**")

    st.divider()
    st.subheader("Statistiques")

    gauche, droite = st.columns(2)
    with gauche:
        figure = graphique_barres(resultats)
        st.pyplot(figure)
        plt.close(figure)  # sinon les figures s'accumulent à chaque rafraîchissement
    with droite:
        figure = graphique_anneau(resultats)
        st.pyplot(figure)
        plt.close(figure)

    tableau_candidats = resultats[["candidat_nom", "parti", "total_votes"]].copy()
    tableau_candidats["total_votes"] = tableau_candidats["total_votes"].map(
        format_milliers
    )
    st.dataframe(
        tableau_candidats.rename(columns={
            "candidat_nom": "Candidat",
            "parti": "Parti",
            "total_votes": "Total des votes",
        }),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("Participation par région")

    donnees_regions = lire_sujet(config.SUJET_PARTICIPATION_PAR_REGION)
    if not donnees_regions:
        st.info("Pas encore d'agrégat de participation par région.")
        return

    participation = pd.DataFrame(donnees_regions)
    participation = participation.loc[
        participation.groupby("region")["total_votes"].idxmax()
    ].reset_index(drop=True)
    participation = participation.sort_values(
        "total_votes", ascending=False, ignore_index=True
    )
    participation = participation.rename(
        columns={"region": "Région", "total_votes": "Votes"}
    )

    afficher_tableau_pagine(participation, cle="regions")


intervalle = st.sidebar.slider(
    "Intervalle d'actualisation (secondes)", min_value=10, max_value=60, value=15
)
st_autorefresh(interval=intervalle * 1000, key="actualisation")
st.sidebar.caption(
    "Le tableau de bord relit les sujets Kafka d'agrégats à chaque "
    "actualisation."
)

afficher_tableau_de_bord()
