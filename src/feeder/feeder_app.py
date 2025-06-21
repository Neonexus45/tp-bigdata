import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from datetime import datetime
import os

class USAccidentsFeeder:
    def __init__(self):
        print("FEEDER: Initialisation du Feeder avec Spark Session")
        
        self.spark = SparkSession.builder \
            .appName("USAccidents-BronzeFeeder") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        print("FEEDER: Configuration des chemins")
        # Chemins de configuration - MODIFIE pour sample.csv
        self.input_path = "/app/data/sample.csv"
        self.bronze_output_path = "/app/output/bronze"
        
        print("FEEDER: Definition des colonnes a selectionner")
        # Colonnes à sélectionner - NOUVEAU
        self.selected_columns = [
            "ID", "Source", "Severity", "Start_Time", "End_Time",
            "Start_Lat", "Start_Lng", "City", "County", "State", "Zipcode",
            "Timezone", "Temperature(F)", "Humidity(%)", "Pressure(in)",
            "Visibility(mi)", "Wind_Direction", "Wind_Speed(mph)", 
            "Precipitation(in)", "Weather_Condition", "Bump", "Crossing",
            "Give_Way", "Junction", "No_Exit", "Railway", "Roundabout",
            "Station", "Stop", "Traffic_Calming", "Traffic_Signal", "Turning_Loop"
        ]
        
        # Variables de classe pour les partitions (simulation d'un cache)
        self._partitions_cache = None
        
        print("FEEDER: Spark Session initialise avec succes")

    def load_source_data(self):
        print(f"FEEDER: Chargement des donnees source depuis {self.input_path}")
        
        try:
            print("FEEDER: Lecture du CSV complet")
            # Lecture du CSV complet
            df_full = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("escape", '"') \
                .csv(self.input_path)
            
            # Vérification des colonnes disponibles
            available_columns = df_full.columns
            print(f"FEEDER: Colonnes disponibles: {len(available_columns)}")
            print(f"FEEDER: Liste des colonnes: {available_columns}")
            
            # Sélection des colonnes désirées
            missing_columns = [col for col in self.selected_columns if col not in available_columns]
            if missing_columns:
                print(f"FEEDER: ATTENTION - Colonnes manquantes: {missing_columns}")
                # Filtrer les colonnes manquantes
                self.selected_columns = [col for col in self.selected_columns if col in available_columns]
            
            print("FEEDER: Selection des colonnes desirees")
            # Sélection finale
            df = df_full.select(*self.selected_columns)
            
            total_records = df.count()
            print(f"FEEDER: {total_records} enregistrements charges")
            print(f"FEEDER: {len(self.selected_columns)} colonnes selectionnees:")
            for i, col in enumerate(self.selected_columns):
                print(f"FEEDER:   {i:2d}. {col}")
            
            return df
            
        except Exception as e:
            print(f"FEEDER: ERREUR lors du chargement des donnees: {str(e)}")
            raise

    def split_dataset_incrementally(self, df):
        print("FEEDER: Division du dataset en 3 parties incrementales")
        
        if self._partitions_cache is None:
            print("FEEDER: Creation des partitions avec randomSplit")
            
            # Utilisation de randomSplit avec un seed fixe pour la reproductibilité
            self._partitions_cache = df.randomSplit([0.33, 0.33, 0.34], seed=42)
            
            # Log des tailles de chaque partition
            for i, partition in enumerate(self._partitions_cache, 1):
                count = partition.count()
                print(f"FEEDER:   Partie {i}: {count} enregistrements")
        
        return self._partitions_cache

    def create_incremental_data(self, partitions, target_date):
        print(f"FEEDER: Creation des donnees incrementales pour {target_date}")
        
        part1, part2, part3 = partitions
        
        # Logique incrémentale glissante
        if target_date == "2025-01-01":
            # Jour 1: seulement partie 1
            result_df = part1
            print("FEEDER: Jour 1: Partie 1 uniquement")
            
        elif target_date == "2025-01-02":
            # Jour 2: partie 1 + partie 2 (historique + nouvelles données)
            result_df = part1.union(part2)
            print("FEEDER: Jour 2: Partie 1 + Partie 2 (cumulatif)")
            
        elif target_date == "2025-01-03":
            # Jour 3: partie 1 + partie 2 + partie 3 (historique complet)
            result_df = part1.union(part2).union(part3)
            print("FEEDER: Jour 3: Partie 1 + Partie 2 + Partie 3 (historique complet)")
            
        else:
            print(f"FEEDER: ERREUR - Date invalide: {target_date}")
            raise ValueError(f"Invalid target_date: {target_date}")
        
        # Ajouter la colonne d'ingestion
        result_df = result_df.withColumn("ingestion_date", lit(target_date))
        
        record_count = result_df.count()
        print(f"FEEDER: Total des enregistrements pour {target_date}: {record_count}")
        
        return result_df

    def save_to_bronze(self, df, target_date):
        print(f"FEEDER: Sauvegarde en Bronze Layer pour {target_date}")
        
        # Extraction de la date pour le partitioning
        year, month, day = target_date.split("-")
        output_path = f"{self.bronze_output_path}/{year}/{month}/{day}"
        
        print(f"FEEDER: Chemin de sortie: {output_path}")
        
        try:
            print("FEEDER: Debut de l'ecriture en mode overwrite")
            # Sauvegarde en mode overwrite pour simulation du batch quotidien
            df.coalesce(2) \
              .write \
              .mode("overwrite") \
              .option("compression", "snappy") \
              .parquet(output_path)
            
            print(f"FEEDER: Sauvegarde reussie dans {output_path}")
            
            # Affichage des métadonnées du fichier
            self._log_output_metadata(output_path)
            
        except Exception as e:
            print(f"FEEDER: ERREUR lors de la sauvegarde Bronze: {str(e)}")
            raise

    def _log_output_metadata(self, output_path):
        print("FEEDER: Verification des metadonnees de sortie")
        try:
            # Vérification de la structure de sortie
            if os.path.exists(output_path):
                parquet_files = [f for f in os.listdir(output_path) if f.endswith('.parquet')]
                print(f"FEEDER: {len(parquet_files)} fichiers parquet crees")
                
                # Taille totale approximative
                total_size = sum(os.path.getsize(os.path.join(output_path, f)) 
                               for f in parquet_files)
                print(f"FEEDER: Taille totale: {total_size / (1024*1024):.2f} MB")
                
        except Exception as e:
            print(f"FEEDER: ATTENTION - Impossible de lire les metadonnees: {str(e)}")

    def run_feeder(self, target_date):
        start_time = datetime.now()
        print(f"FEEDER: Demarrage du Bronze Feeder pour {target_date}")
        
        try:
            print("FEEDER: Etape 1 - Chargement des donnees source")
            # 1. Charger les données source
            source_df = self.load_source_data()
            
            print("FEEDER: Etape 2 - Division du dataset en partitions")
            # 2. Diviser le dataset en partitions
            partitions = self.split_dataset_incrementally(source_df)
            
            print("FEEDER: Etape 3 - Creation des donnees incrementales")
            # 3. Créer les données incrémentales
            incremental_df = self.create_incremental_data(partitions, target_date)
            
            print("FEEDER: Etape 4 - Sauvegarde en Bronze")
            # 4. Sauvegarder en Bronze
            self.save_to_bronze(incremental_df, target_date)
            
            # Calcul du temps d'exécution
            execution_time = datetime.now() - start_time
            print(f"FEEDER: Termine en {execution_time.total_seconds():.2f} secondes")
            
        except Exception as e:
            print(f"FEEDER: ERREUR - Echec du feeder: {str(e)}")
            raise
        finally:
            print("FEEDER: Arret de Spark Session")
            self.spark.stop()

def main():
    print("FEEDER: Point d'entree principal")
    parser = argparse.ArgumentParser(description='US Accidents Bronze Feeder')
    parser.add_argument('--date', required=True, 
                       choices=['2025-01-01', '2025-01-02', '2025-01-03'],
                       help='Target date for incremental feeding')
    
    args = parser.parse_args()
    print(f"FEEDER: Date cible: {args.date}")
    
    # Créer et exécuter le feeder
    feeder = USAccidentsFeeder()
    feeder.run_feeder(args.date)
    
    print("FEEDER: Execution terminee")

if __name__ == "__main__":
    main()