import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import psycopg2
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

def create_kafka_consumer(topic_name):
    # Configurer un consommateur Kafka avec un sujet et des configurations spécifiés
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

@st.cache_data
def fetch_voting_stats():
    connexion = psycopg2.connect(host="localhost", database="voting", user="postgres", password="patron")
    cursor = connexion.cursor()

    # Recuperer le nombre total de votants
    cursor.execute("""
        SELECT count(*) voters_count FROM votants
    """)
    voters_count = cursor.fetchone()[0]

    # Recuperer le nombre total de candidats
    cursor.execute("""
        SELECT count(*) candidates_count FROM candidats
    """)
    candidates_count = cursor.fetchone()[0]

    return voters_count, candidates_count

def fetch_data_from_kafka(consumer):
    # Interroger le consommateur Kafka pour les messages dans un délai d'attente
    messages = consumer.poll(timeout_ms=1000)
    data = []

    # Extraire les données des messages reçus
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data


# Fonction pour diviser une trame de données en morceaux
@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df

# Fonction pour paginer un tableau
def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio(
                "Direction", options=["⬆️", "⬇️"], horizontal=True
            )
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )
    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data) / batch_size) if int(len(table_data) / batch_size) > 0 else 1
        )
        current_page = st.number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)


def update_data():
    # Espace réservé pour afficher l'heure du dernier rafraîchissement
    last_refresh = st.empty()
    last_refresh.text(f"Dernière actualisation le: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_stats()

    # Afficher les statistiques
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total votants", voters_count)
    col2.metric("Total Candidats", candidates_count)

    # Récupérer les données de Kafka sur les votes agrégés par candidat
    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)

    # Identifier le candidat leader
    results = results.loc[results.groupby('candidat_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    # Afficher les infos sur le candidat leader
    st.markdown("""---""")
    st.header('Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['url_photo'], width=200)
    with col2:
        st.header(leading_candidate['candidat_nom'])
        st.subheader(leading_candidate['partie'])
        st.subheader("Total Vote: {}".format(leading_candidate['total_votes']))

    # Visualisation des stats
    st.markdown("""---""")
    st.header('Statistics')
    results = results[['candidat_id', 'candidat_nom', 'partie', 'total_votes']]
    results = results.reset_index(drop=True)
    col1, col2 = st.columns(2)

    # Afficher un graphique à barres et un graphique en anneau
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)

    with col2:
        donut_fig = plot_donut_chart(results, title='Vote Distribution')
        st.pyplot(donut_fig)

    # Afficher le tableau avec les statistiques des candidats
    st.table(results)

    # Récupérer des données de Kafka sur la participation agrégée par emplacement
    location_consumer = create_kafka_consumer("aggregated_turnout_by_location")
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data)

    # Identifiez les regions avec une participation maximale
    location_result = location_result.loc[location_result.groupby('region')['count'].idxmax()]
    location_result = location_result.reset_index(drop=True)

    # Display location-based voter information with pagination
    st.header("Lacalisation des votants")
    paginate_table(location_result)

    # Update the last refresh time
    st.session_state['last_update'] = time.time()


def plot_colored_bar_chart(results):
    data_type = results['candidat_nom']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel('Candidat')
    plt.ylabel('Votes Total')
    plt.title('Nombre de votes par candidat')
    plt.xticks(rotation=90)
    return plt

# Function to plot a donut chart for vote distribution
def plot_donut_chart(data: pd.DataFrame, title='Donut Chart', type='candidat'):
    if type == 'candidat':
        labels = list(data['candidat_nom'])
    elif type == 'genre':
        labels = list(data['genre'])

    sizes = list(data['total_votes'])
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title(title)
    return fig

# Function to plot a pie chart for vote distribution
def plot_pie_chart(data, title='Répartition par sexe des électeurs', labels=None):
    sizes = list(data.values())
    if labels is None:
        labels = list(data.keys())

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title(title)
    return fig



# Sidebar layout
def sidebar():
    # Initialize last update time if not present in session state
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    # Slider to control refresh interval
    refresh_interval = st.sidebar.slider("Intervalle d'actualisation (secondes)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    # Button to manually refresh data
    if st.sidebar.button('Actualiser les données'):
        update_data()


st.title('Tableau de bord des élections en temps réel')
topic_name = 'aggregated_votes_per_candidate'

# Display sidebar
sidebar()

update_data()