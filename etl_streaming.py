from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, expr, year, month, lower, trim, when
import os

# --------------------------
# 1. Création de la session Spark
# --------------------------
spark = SparkSession.builder \
    .appName("ETL Streaming CSV") \
    .getOrCreate()

# Pour afficher les logs Spark moins verbeux
spark.sparkContext.setLogLevel("WARN")

# --------------------------
# 2. Définition du schéma des données
# --------------------------
schema = StructType([
    StructField("ID_produit", IntegerType(), True),
    StructField("Nom_produit", StringType(), True),
    StructField("Quantite_vendue", IntegerType(), True),
    StructField("Prix_unitaire", DoubleType(), True),
    StructField("Date_vente", StringType(), True)  # On va la convertir en timestamp plus tard
])

# --------------------------
# 3. Chargement du flux de fichiers CSV dans un dossier "input"

# --------------------------
input_path = "C:\Users\medaz\OneDrive\Desktop\TP-Conception-pip\input"

# Lecture en streaming
df_stream = spark.readStream \
    .option("header", True) \
    .schema(schema) \
    .csv(input_path)

# --------------------------
# 4. Traitement / nettoyage des données en streaming
# --------------------------

from pyspark.sql.functions import to_timestamp

# Conversion de la date en timestamp
df_clean = df_stream.withColumn("Date_vente", to_timestamp(col("Date_vente"), "yyyy-MM-dd HH:mm:ss"))

# Suppression des valeurs aberrantes
df_clean = df_clean.filter(
    (col("Quantite_vendue") > 0) & 
    (col("Prix_unitaire") > 0) & 
    (col("Date_vente").isNotNull())
)

# Nettoyage du nom du produit : minuscules et trim
df_clean = df_clean.withColumn("Nom_produit", trim(lower(col("Nom_produit"))))

# Calcul du montant total
df_clean = df_clean.withColumn("Montant_total", col("Quantite_vendue") * col("Prix_unitaire"))

# Ajout des colonnes mois et année
df_clean = df_clean.withColumn("Mois_vente", month(col("Date_vente")))
df_clean = df_clean.withColumn("Annee_vente", year(col("Date_vente")))

# Catégorisation du montant total
df_clean = df_clean.withColumn(
    "Categorie_montant",
    when(col("Montant_total") <= 50, "faible")
    .when((col("Montant_total") > 50) & (col("Montant_total") <= 200), "moyen")
    .otherwise("élevé")
)

# Catégorisation du type de produit selon le nom
df_clean = df_clean.withColumn(
    "Categorie_produit",
    when(col("Nom_produit").rlike("chemise|pantalon|veste"), "habillement")
    .when(col("Nom_produit").rlike("ordinateur|écran"), "électronique")
    .otherwise("autre")
)

# --------------------------
# 5. Analyse temps réel : calcul du CA cumulé pour l'année en cours
# --------------------------
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum

# On filtre sur l'année en cours
from pyspark.sql.functions import current_date

current_year = df_clean.select(year(current_date())).collect()[0][0]

df_ca_annee = df_clean.filter(col("Annee_vente") == current_year) \
    .groupBy("Annee_vente") \
    .agg(_sum("Montant_total").alias("CA_annee_cumule"))

# --------------------------
# 6. Écriture du flux traité dans un dossier "output"
# Le fichier plat est généré avec un format de partition par date pour la sauvegarde datée
# --------------------------
output_path = "C:\Users\medaz\OneDrive\Desktop\TP-Conception-pip\donnees_streaming resultats"

query = df_clean.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "chemin/vers/checkpoint") \
    .option("path", output_path) \
    .partitionBy("Annee_vente", "Mois_vente") \
    .start()

# Affichage du CA cumulé en console en temps réel
query_ca = df_ca_annee.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# --------------------------
# 7. Gestion des erreurs simple avec try-except autour de la boucle de streaming
# --------------------------

try:
    query.awaitTermination()   # Attend la fin du streaming (CTRL+C pour interrompre)
    query_ca.awaitTermination()
except Exception as e:
    print("Erreur dans le streaming :", e)
    spark.stop()

