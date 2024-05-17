# import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, countDistinct, window
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType


if __name__ == "__main__":
    #print(pyspark.__version__)
    spark = (SparkSession.builder
             .appName("ElectionAnalysis")
             .master("local[*]")  # Utiliser l'exécution Spark locale avec tous les cœurs disponibles
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")  # integration de Spark-Kafka
             .config("spark.jars",
                     "C:/Users/yacine/Documents/Etudes/DIT Big-Data/Data Engineering/RealtimeVoting/postgresql-42.7.3.jar")  # Pilote PostgreSQL
             .config("spark.sql.adaptive.enabled", "false")  # Désactiver l'exécution de requêtes adaptatives
             .getOrCreate())

    # Définir des schémas pour les sujets Kafka
    vote_schema = StructType([
        StructField("votant_id", StringType(), True),
        StructField("candidat_id", StringType(), True),
        StructField("temps_vote", TimestampType(), True),
        StructField("votant_nom", StringType(), True),
        StructField("partie", StringType(), True),
        StructField("biographie", StringType(), True),
        StructField("platforme_campagne", StringType(), True),
        StructField("url_photo", StringType(), True),
        StructField("candidat_nom", StringType(), True),
        StructField("date_naissance", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("nationalite", StringType(), True),
        StructField("numero_registre", StringType(), True),
        StructField("address", StructType([
            StructField("rue", StringType(), True),
            StructField("ville", StringType(), True),
            StructField("region", StringType(), True),
            StructField("pays", StringType(), True),
            StructField("codePostal", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("photo", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

    # Lire les données de Kafka 'votes_topic' et les traiter
    votes_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "votes_topic") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), vote_schema).alias("data")) \
        .select("data.*")

    # Prétraitement des données : typage et filigrane
    votes_df = votes_df.withColumn("temps_vote", col("temps_vote").cast(TimestampType())) \
        .withColumn('vote', col('vote').cast(IntegerType()))
    enriched_votes_df = votes_df.withWatermark("temps_vote", "1 minute")

    # Votes cumulés par candidat et taux de participation par emplacement
    votes_per_candidate = enriched_votes_df.groupBy("candidat_id", "candidat_nom", "partie",
                                                    "url_photo").agg(_sum("vote").alias("total_votes"))
    turnout_by_location = enriched_votes_df.groupBy("address.region").count().alias("total_votes")

    # Écrire des données agrégées dans des sujets Kafka
    votes_per_candidate_to_kafka = votes_per_candidate.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_votes_per_candidate") \
        .option("checkpointLocation", "C:/Users/yacine/Documents/Etudes/DIT Big-Data/Data Engineering/RealtimeVoting/checkpoints/checkpoint1") \
        .outputMode("update") \
        .start()

    turnout_by_location_to_kafka = turnout_by_location.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_turnout_by_location") \
        .option("checkpointLocation", "C:/Users/yacine/Documents/Etudes/DIT Big-Data/Data Engineering/RealtimeVoting/checkpoints/checkpoint2") \
        .outputMode("update") \
        .start()

    # Attendez la fin des requêtes de streaming
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()




