# vote-temps-reel-data-engineering

Système de vote en temps réel construit avec Apache Kafka, Apache Zookeeper, Apache Spark, Postgres et Streamlit le tous conteneurisé avec Docker pour un déploiement et une évolutivité faciles.

## Tester l'application

Prerequis
- Clonez ce référentiel.
- Python: version recente
- Docker

1 - Créér et démarrera les conteneurs Zookeeper, Kafka et Postgres en mode détaché (-d):
```bash
docker-compose up -d
```

2 - Installez les packages Python requis:
```bash
pip install -r requirements.txt
```

3 - Créer des tables sur postgres ( `candidats`, `votants` et `votes` ), génèrer des données de votant, les envoie à un sujet Kafka (`voters_topic`)
```bash
python main.py
```

4 - Consommer les votes du sujet Kafka (`voters_topic`), générer des données de vote et envoie des données sur le sujet Kafka `votes_topic`
```bash
python voting.py
```

5 - Consommer les votes du sujet Kafka (`votes_topic`), enrichir les données de postgres, agréger les votes et produire des données sur des sujets spécifiques sur Kafka (`aggregated_votes_per_candidate`: Nombre de votes par candidat, `aggregated_turnout_by_location`: Nombre de vote par Region)
```bash
python spark-streaming.py
```

6 - Consommer les données de vote agrégées du sujet Kafka ainsi que de postgres et d'afficher les données de vote en temps réel à l'aide de Streamlit
```bash
streamlit run app.py
```
