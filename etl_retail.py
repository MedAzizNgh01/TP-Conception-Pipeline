# Importation des bibliothèques nécessaires
import pandas as pd              # Pour manipuler les données sous forme de DataFrame
import numpy as np               # Pour les opérations mathématiques et numériques
import logging                   # Pour journaliser les événements du pipeline
from sklearn.preprocessing import MinMaxScaler  # Pour normaliser les données numériques

# Configuration du journal de log pour suivre les étapes et erreurs du pipeline
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fonction pour charger les données depuis un fichier CSV
def charger_donnees(fichier_csv):
    try:
        df = pd.read_csv(fichier_csv)  # Lecture du fichier CSV
        colonnes_attendues = {'ID_produit', 'Nom_produit', 'Quantite_vendue', 'Prix_unitaire', 'Date_vente'}
        
        # Vérification que toutes les colonnes nécessaires sont présentes
        if not colonnes_attendues.issubset(df.columns):
            raise ValueError("Colonnes manquantes dans le fichier CSV.")
        
        logging.info("Chargement du fichier réussi.")
        return df
    except FileNotFoundError:
        logging.error("Fichier non trouvé.")
        raise
    except pd.errors.ParserError:
        logging.error("Erreur de parsing du fichier CSV.")
        raise
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données : {str(e)}")
        raise

# Analyse exploratoire des données
def analyse_donnees(df):
    print("Aperçu des données :")
    print(df.head())  # Affiche les premières lignes du dataset
    print("\nInfos générales :")
    print(df.info())  # Donne des infos sur les types et colonnes
    print("\nValeurs manquantes par colonne :")
    print(df.isnull().sum())  # Affiche le nombre de valeurs manquantes
    print("\nStatistiques descriptives :")
    print(df.describe())  # Statistiques de base sur les colonnes numériques

# Traitement des valeurs manquantes
def traitement_valeurs_manquantes(df):
    # Suppression des lignes où les colonnes critiques sont vides
    colonnes_critiques = ['ID_produit', 'Nom_produit', 'Quantite_vendue', 'Prix_unitaire', 'Date_vente']
    df.dropna(subset=colonnes_critiques, inplace=True)

    # Remplir les valeurs manquantes restantes
    df['Quantite_vendue'] = df['Quantite_vendue'].fillna(0)
    df['Prix_unitaire'] = df['Prix_unitaire'].fillna(df['Prix_unitaire'].median())

    logging.info("Traitement des valeurs manquantes terminé.")
    return df

# Suppression des valeurs aberrantes
def gestion_valeurs_aberrantes(df):
    # Filtrage des lignes où la quantité ou le prix sont invalides (<= 0)
    df = df[(df['Quantite_vendue'] > 0) & (df['Prix_unitaire'] > 0)].copy()

    # Conversion des dates, suppression si conversion échoue
    df['Date_vente'] = pd.to_datetime(df['Date_vente'], errors='coerce')
    df.dropna(subset=['Date_vente'], inplace=True)

    logging.info("Traitement des valeurs aberrantes terminé.")
    return df

# Suppression des doublons exacts
def suppression_doublons(df):
    avant = len(df)
    df.drop_duplicates(inplace=True)
    apres = len(df)
    logging.info(f"{avant - apres} doublons supprimés.")
    return df

# Application de transformations enrichies
def transformations(df):
    # 1. Création d'une colonne 'Montant_total'
    df['Montant_total'] = df['Quantite_vendue'] * df['Prix_unitaire']

    # 2. Normalisation du prix unitaire entre 0 et 1
    scaler = MinMaxScaler()
    df['Prix_unitaire_normalisé'] = scaler.fit_transform(df[['Prix_unitaire']]).round(3)

    # 3. Extraction du mois et de l’année de la date de vente
    df['Mois_vente'] = df['Date_vente'].dt.month
    df['Annee_vente'] = df['Date_vente'].dt.year

    # 4. Nettoyage des noms de produits (suppression espaces, mise en minuscules)
    df['Nom_produit'] = df['Nom_produit'].str.strip().str.lower()

    # 5. Classification du montant total en catégories
    df['Categorie_montant'] = pd.cut(
        df['Montant_total'],
        bins=[-1, 50, 200, np.inf],
        labels=['faible', 'moyen', 'élevé']
    )

    # 6. Catégorisation des produits selon le nom
    def categorie_produit(nom):
        if any(x in nom for x in ['chemise', 'pantalon', 'veste']):
            return 'habillement'
        elif any(x in nom for x in ['ordinateur', 'écran']):
            return 'électronique'
        else:
            return 'autre'

    df['Categorie_produit'] = df['Nom_produit'].apply(categorie_produit)

    logging.info("Transformations enrichies appliquées.")
    return df

# Validation des données nettoyées
def validation_croisée(df):
    # Vérifie qu’il ne reste plus de valeurs manquantes
    assert df.isnull().sum().sum() == 0, "Des valeurs manquantes subsistent."
    # Vérifie que toutes les quantités sont strictement positives
    assert (df['Quantite_vendue'] > 0).all(), "Des quantités invalides."
    # Vérifie que la date est bien au format datetime
    assert pd.api.types.is_datetime64_any_dtype(df['Date_vente']), "Dates mal converties."
    
    logging.info("Validation croisée réussie.")
    print("✅ Validation croisée OK.")

# Sauvegarde du DataFrame nettoyé dans un nouveau fichier CSV
def sauvegarde(df, chemin_sortie):
    df.to_csv(chemin_sortie, index=False, encoding='utf-8-sig')
    logging.info(f"Données sauvegardées dans {chemin_sortie}.")

# Pipeline complet : orchestration des étapes ETL
def pipeline_etl(fichier_entree, fichier_sortie):
    try:
        logging.info("Début du pipeline ETL.")
        df = charger_donnees(fichier_entree)         # Étape 1 : Chargement
        analyse_donnees(df)                          # Étape 2 : Exploration
        df = traitement_valeurs_manquantes(df)       # Étape 3 : Valeurs manquantes
        df = gestion_valeurs_aberrantes(df)          # Étape 4 : Valeurs aberrantes
        df = suppression_doublons(df)                # Étape 5 : Doublons
        df = transformations(df)                     # Étape 6 : Transformations
        validation_croisée(df)                       # Étape 7 : Validation
        sauvegarde(df, fichier_sortie)               # Étape 8 : Sauvegarde
        logging.info("Fin du pipeline ETL avec succès.")
        print(f"🎉 ETL terminé avec succès. {len(df)} lignes sauvegardées dans '{fichier_sortie}'.")
    except Exception as e:
        logging.error(f"Erreur dans le pipeline ETL : {str(e)}")
        print("❌ Une erreur est survenue dans le pipeline ETL.")

# Lancement du pipeline si le script est exécuté directement
if __name__ == "__main__":
    pipeline_etl("ventes.csv", "vente_clean.csv")
