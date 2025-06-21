import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, isnan, isnull, trim, regexp_replace, 
    to_timestamp, unix_timestamp, round as spark_round,
    upper, lower, split, size, array_contains,
    coalesce, date_format, hour, dayofweek, month,
    count, sum as spark_sum, avg, stddev, min as spark_min, max as spark_max
)
from pyspark.sql.types import *
from datetime import datetime
import os

class USAccidentsPreprocessor:
    def __init__(self):
        print("PREPROCESSOR: Initialisation du Preprocessor avec Spark Session")
        
        self.spark = SparkSession.builder \
            .appName("USAccidents-SilverPreprocessor") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        print("PREPROCESSOR: Configuration des chemins")
        self.bronze_path = "/app/output/bronze"
        self.silver_path = "/app/output/silver"
        
        print("PREPROCESSOR: Spark Session initialise pour le preprocessing")

    def get_latest_bronze_partition(self, target_date):
        print(f"PREPROCESSOR: ETAPE 0 - Recuperation de la partition Bronze pour {target_date}")
        year, month, day = target_date.split("-")
        bronze_date_path = f"{self.bronze_path}/{year}/{month}/{day}"
        
        print(f"PREPROCESSOR: Chemin Bronze: {bronze_date_path}")
        
        if not os.path.exists(bronze_date_path):
            print(f"PREPROCESSOR: ERREUR - Partition Bronze non trouvee: {bronze_date_path}")
            raise FileNotFoundError(f"Bronze partition not found: {bronze_date_path}")
        
        return bronze_date_path

    def load_bronze_data(self, bronze_path):
        print("PREPROCESSOR: ETAPE 1 - Chargement des donnees depuis la couche Bronze")
        try:
            df = self.spark.read.parquet(bronze_path)
            record_count = df.count()
            column_count = len(df.columns)
            
            print(f"PREPROCESSOR: ETAPE 1 - {record_count} enregistrements charges avec {column_count} colonnes")
            print(f"PREPROCESSOR: ETAPE 1 - Colonnes chargees: {df.columns}")
            
            # Analyse des types de données
            print("PREPROCESSOR: ETAPE 1 - Analyse des types de donnees:")
            df.printSchema()
            
            return df
            
        except Exception as e:
            print(f"PREPROCESSOR: ETAPE 1 - ERREUR lors du chargement Bronze: {str(e)}")
            raise

    def analyze_missing_values(self, df):
        print("PREPROCESSOR: ETAPE 2 - Analyse des valeurs manquantes")
        
        total_records = df.count()
        print(f"PREPROCESSOR: ETAPE 2 - Total enregistrements: {total_records}")
        
        missing_analysis = []
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            missing_percentage = (null_count / total_records) * 100
            missing_analysis.append((column, null_count, missing_percentage))
            
            if null_count > 0:
                print(f"PREPROCESSOR: ETAPE 2 - {column}: {null_count} valeurs manquantes ({missing_percentage:.2f}%)")
        
        return missing_analysis

    def handle_missing_values(self, df):
        print("PREPROCESSOR: ETAPE 3 - Gestion systematique des valeurs manquantes")
        
        # Définition des colonnes par type
        boolean_columns = [
            "Bump", "Crossing", "Give_Way", "Junction", "No_Exit", 
            "Railway", "Roundabout", "Station", "Stop", 
            "Traffic_Calming", "Traffic_Signal", "Turning_Loop"
        ]
        
        text_columns = [
            "ID", "Source", "City", "County", "State", "Zipcode", 
            "Timezone", "Wind_Direction", "Weather_Condition"
        ]
        
        numeric_columns = [
            "Severity", "Start_Lat", "Start_Lng", "Temperature(F)", 
            "Humidity(%)", "Pressure(in)", "Visibility(mi)", 
            "Wind_Speed(mph)", "Precipitation(in)"
        ]
        
        datetime_columns = ["Start_Time", "End_Time"]
        
        print("PREPROCESSOR: ETAPE 3.1 - Traitement des colonnes booleennes")
        for col_name in boolean_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, coalesce(col(col_name), lit(False)))
                print(f"PREPROCESSOR: ETAPE 3.1 - {col_name}: valeurs manquantes -> False")
        
        print("PREPROCESSOR: ETAPE 3.2 - Traitement des colonnes texte")
        for col_name in text_columns:
            if col_name in df.columns:
                if col_name == "ID":
                    # ID ne doit jamais être null
                    df = df.filter(col(col_name).isNotNull())
                    print(f"PREPROCESSOR: ETAPE 3.2 - {col_name}: suppression des enregistrements avec ID null")
                else:
                    df = df.withColumn(col_name, coalesce(col(col_name), lit("Unknown")))
                    print(f"PREPROCESSOR: ETAPE 3.2 - {col_name}: valeurs manquantes -> 'Unknown'")
        
        print("PREPROCESSOR: ETAPE 3.3 - Traitement des colonnes numeriques")
        for col_name in numeric_columns:
            if col_name in df.columns:
                if col_name in ["Start_Lat", "Start_Lng"]:
                    # Coordonnées critiques - supprimer si manquantes
                    df = df.filter(col(col_name).isNotNull())
                    print(f"PREPROCESSOR: ETAPE 3.3 - {col_name}: suppression des enregistrements avec coordonnees null")
                elif col_name == "Severity":
                    # Severity critique - valeur par défaut
                    df = df.withColumn(col_name, coalesce(col(col_name), lit(2)))
                    print(f"PREPROCESSOR: ETAPE 3.3 - {col_name}: valeurs manquantes -> 2 (moderate)")
                else:
                    # Autres valeurs numériques - remplacer par 0 ou valeur logique
                    if col_name == "Temperature(F)":
                        default_val = lit(70.0)  # Température moyenne
                    elif col_name == "Humidity(%)":
                        default_val = lit(50.0)  # Humidité moyenne
                    elif col_name == "Pressure(in)":
                        default_val = lit(30.0)  # Pression standard
                    elif col_name == "Visibility(mi)":
                        default_val = lit(10.0)  # Visibilité standard
                    else:
                        default_val = lit(0.0)   # Défaut pour vitesse vent, précipitations
                    
                    df = df.withColumn(col_name, coalesce(col(col_name), default_val))
                    print(f"PREPROCESSOR: ETAPE 3.3 - {col_name}: valeurs manquantes -> {default_val}")
        
        print("PREPROCESSOR: ETAPE 3.4 - Traitement des colonnes datetime")
        for col_name in datetime_columns:
            if col_name in df.columns:
                df = df.filter(col(col_name).isNotNull())
                print(f"PREPROCESSOR: ETAPE 3.4 - {col_name}: suppression des enregistrements avec datetime null")
        
        final_count = df.count()
        print(f"PREPROCESSOR: ETAPE 3 - Finalise: {final_count} enregistrements apres gestion des valeurs manquantes")
        
        return df

    def validate_data_quality(self, df):
        print("PREPROCESSOR: ETAPE 4 - Validation de la qualite des donnees")
        
        initial_count = df.count()
        print(f"PREPROCESSOR: ETAPE 4 - Nombre initial: {initial_count}")
        
        print("PREPROCESSOR: ETAPE 4.1 - Suppression des doublons")
        df_dedup = df.dropDuplicates(["ID"])
        dedup_count = df_dedup.count()
        duplicates_removed = initial_count - dedup_count
        print(f"PREPROCESSOR: ETAPE 4.1 - Supprime {duplicates_removed} doublons")
        
        print("PREPROCESSOR: ETAPE 4.2 - Validation des coordonnees geographiques")
        df_geo = df_dedup.filter(
            (col("Start_Lat").between(-90, 90)) &
            (col("Start_Lng").between(-180, 180)) &
            (col("Start_Lat") != 0) &
            (col("Start_Lng") != 0)
        )
        geo_count = df_geo.count()
        geo_invalid = dedup_count - geo_count
        print(f"PREPROCESSOR: ETAPE 4.2 - Supprime {geo_invalid} enregistrements avec coordonnees invalides")
        
        print("PREPROCESSOR: ETAPE 4.3 - Validation des valeurs de severite")
        df_severity = df_geo.filter(col("Severity").between(1, 4))
        severity_count = df_severity.count()
        severity_invalid = geo_count - severity_count
        print(f"PREPROCESSOR: ETAPE 4.3 - Supprime {severity_invalid} enregistrements avec severite invalide")
        
        return df_severity

    def standardize_and_clean_data(self, df):
        print("PREPROCESSOR: ETAPE 5 - Standardisation et nettoyage des donnees")
        
        print("PREPROCESSOR: ETAPE 5.1 - Conversion des timestamps")
        df = df.withColumn(
            "Start_Time_Clean",
            to_timestamp(col("Start_Time"), "yyyy-MM-dd HH:mm:ss")
        ).withColumn(
            "End_Time_Clean", 
            to_timestamp(col("End_Time"), "yyyy-MM-dd HH:mm:ss")
        )
        
        print("PREPROCESSOR: ETAPE 5.2 - Calcul de la duree")
        df = df.withColumn(
            "Duration_Hours",
            (unix_timestamp(col("End_Time_Clean")) - unix_timestamp(col("Start_Time_Clean"))) / 3600
        )
        
        print("PREPROCESSOR: ETAPE 5.3 - Filtrage des durees aberrantes")
        df = df.filter(
            col("Duration_Hours").isNotNull() &
            (col("Duration_Hours") >= 0) &
            (col("Duration_Hours") <= 168)  # Max 7 jours
        )
        
        print("PREPROCESSOR: ETAPE 5.4 - Standardisation des champs texte")
        df = df.withColumn("State_Clean", upper(trim(col("State")))) \
              .withColumn("City_Clean", trim(col("City"))) \
              .withColumn("County_Clean", trim(col("County"))) \
              .withColumn("Source_Clean", trim(col("Source"))) \
              .withColumn("Weather_Condition_Clean", trim(col("Weather_Condition"))) \
              .withColumn("Wind_Direction_Clean", trim(col("Wind_Direction")))
        
        print("PREPROCESSOR: ETAPE 5.5 - Validation des donnees meteorologiques")
        df = self._validate_weather_data(df)
        
        final_count = df.count()
        print(f"PREPROCESSOR: ETAPE 5 - Finalise: {final_count} enregistrements apres standardisation")
        
        return df

    def _validate_weather_data(self, df):
        print("PREPROCESSOR: ETAPE 5.5 - Validation specifique des donnees meteorologiques")
        
        print("PREPROCESSOR: ETAPE 5.5.1 - Validation temperature (-50F a 130F)")
        df = df.withColumn(
            "Temperature_F_Clean",
            when(col("Temperature(F)").between(-50, 130), col("Temperature(F)"))
            .otherwise(lit(70.0))  # Valeur par défaut au lieu de null
        )
        
        print("PREPROCESSOR: ETAPE 5.5.2 - Validation humidite (0-100%)")
        df = df.withColumn(
            "Humidity_Pct_Clean",
            when(col("Humidity(%)").between(0, 100), col("Humidity(%)"))
            .otherwise(lit(50.0))  # Valeur par défaut
        )
        
        print("PREPROCESSOR: ETAPE 5.5.3 - Validation pression (25-35 inches)")
        df = df.withColumn(
            "Pressure_In_Clean",
            when(col("Pressure(in)").between(25, 35), col("Pressure(in)"))
            .otherwise(lit(30.0))  # Valeur par défaut
        )
        
        print("PREPROCESSOR: ETAPE 5.5.4 - Validation visibilite (0-15 miles)")
        df = df.withColumn(
            "Visibility_Mi_Clean",
            when(col("Visibility(mi)").between(0, 15), col("Visibility(mi)"))
            .otherwise(lit(10.0))  # Valeur par défaut
        )
        
        print("PREPROCESSOR: ETAPE 5.5.5 - Validation vitesse du vent (0-200 mph)")
        df = df.withColumn(
            "Wind_Speed_Mph_Clean",
            when(col("Wind_Speed(mph)").between(0, 200), col("Wind_Speed(mph)"))
            .otherwise(lit(0.0))  # Valeur par défaut
        )
        
        print("PREPROCESSOR: ETAPE 5.5.6 - Validation precipitations (0-10 inches)")
        df = df.withColumn(
            "Precipitation_In_Clean",
            when(col("Precipitation(in)").between(0, 10), col("Precipitation(in)"))
            .otherwise(lit(0.0))  # Valeur par défaut
        )
        
        return df

    def create_weather_dataset(self, df):
        print("PREPROCESSOR: ETAPE 6 - Creation du dataset meteorologique normalise")
        
        weather_df = df.select(
            col("ID"),
            col("State_Clean").alias("State"),
            col("Start_Time_Clean").alias("Timestamp"),
            col("Temperature_F_Clean").alias("Temperature_F"),
            col("Humidity_Pct_Clean").alias("Humidity_Pct"),
            col("Pressure_In_Clean").alias("Pressure_In"),
            col("Visibility_Mi_Clean").alias("Visibility_Mi"),
            col("Wind_Speed_Mph_Clean").alias("Wind_Speed_Mph"),
            col("Precipitation_In_Clean").alias("Precipitation_In"),
            col("Weather_Condition_Clean").alias("Weather_Condition"),
            col("Wind_Direction_Clean").alias("Wind_Direction"),
            col("ingestion_date")
        )
        
        print("PREPROCESSOR: ETAPE 6.1 - Ajout de categories meteorologiques")
        weather_df = weather_df.withColumn(
            "Weather_Category",
            when(col("Weather_Condition").rlike("(?i).*(clear|fair|sunny).*"), "Clear")
            .when(col("Weather_Condition").rlike("(?i).*(cloud|overcast).*"), "Cloudy") 
            .when(col("Weather_Condition").rlike("(?i).*(rain|drizzle|shower).*"), "Rainy")
            .when(col("Weather_Condition").rlike("(?i).*(snow|sleet|ice|hail).*"), "Snow/Ice")
            .when(col("Weather_Condition").rlike("(?i).*(fog|mist|haze).*"), "Fog/Mist")
            .when(col("Weather_Condition").rlike("(?i).*(thunder|storm).*"), "Storm")
            .otherwise("Other")
        )
        
        print("PREPROCESSOR: ETAPE 6.2 - Ajout de categories de temperature")
        weather_df = weather_df.withColumn(
            "Temperature_Category",
            when(col("Temperature_F") < 32, "Freezing")
            .when(col("Temperature_F") < 50, "Cold")
            .when(col("Temperature_F") < 70, "Mild")
            .when(col("Temperature_F") < 85, "Warm")
            .otherwise("Hot")
        )
        
        print("PREPROCESSOR: ETAPE 6.3 - Ajout d'indicateurs meteorologiques")
        weather_df = weather_df.withColumn(
            "Weather_Severity_Score",
            when(col("Weather_Category") == "Clear", 1)
            .when(col("Weather_Category") == "Cloudy", 2)
            .when(col("Weather_Category") == "Fog/Mist", 3)
            .when(col("Weather_Category") == "Rainy", 4)
            .when(col("Weather_Category") == "Snow/Ice", 5)
            .when(col("Weather_Category") == "Storm", 6)
            .otherwise(2)
        )
        
        record_count = weather_df.count()
        print(f"PREPROCESSOR: ETAPE 6 - Dataset weather: {record_count} enregistrements")
        
        return weather_df

    def create_location_dataset(self, df):
        print("PREPROCESSOR: ETAPE 7 - Creation du dataset geographique enrichi")
        
        location_df = df.select(
            col("ID"),
            col("Start_Lat").alias("Latitude"),
            col("Start_Lng").alias("Longitude"), 
            col("State_Clean").alias("State"),
            col("City_Clean").alias("City"),
            col("County_Clean").alias("County"),
            trim(col("Zipcode")).alias("Zipcode"),
            trim(col("Timezone")).alias("Timezone"),
            col("ingestion_date")
        )
        
        print("PREPROCESSOR: ETAPE 7.1 - Enrichissement geographique - regions US")
        location_df = location_df.withColumn(
            "Region",
            when(col("State").isin("ME", "NH", "VT", "MA", "RI", "CT"), "Northeast")
            .when(col("State").isin("NY", "NJ", "PA"), "Mid-Atlantic") 
            .when(col("State").isin("OH", "MI", "IN", "WI", "IL", "MN", "IA", "MO", "ND", "SD", "NE", "KS"), "Midwest")
            .when(col("State").isin("DE", "MD", "DC", "VA", "WV", "KY", "TN", "NC", "SC", "GA", "FL", "AL", "MS", "AR", "LA", "OK", "TX"), "South")
            .when(col("State").isin("MT", "WY", "CO", "NM", "ID", "UT", "NV", "AZ", "WA", "OR", "CA", "AK", "HI"), "West")
            .otherwise("Other")
        )
        
        print("PREPROCESSOR: ETAPE 7.2 - Classification zones urbaines/rurales")
        location_df = location_df.withColumn(
            "Area_Type",
            when((col("City") != "Unknown") & (col("City").isNotNull()), "Urban")
            .otherwise("Rural")
        )
        
        print("PREPROCESSOR: ETAPE 7.3 - Ajout de coordonnees arrondies pour clustering")
        location_df = location_df.withColumn(
            "Latitude_Rounded",
            spark_round(col("Latitude"), 2)
        ).withColumn(
            "Longitude_Rounded", 
            spark_round(col("Longitude"), 2)
        )
        
        record_count = location_df.count()
        print(f"PREPROCESSOR: ETAPE 7 - Dataset location: {record_count} enregistrements")
        
        return location_df

    def create_accidents_clean_dataset(self, df):
        print("PREPROCESSOR: ETAPE 8 - Creation du dataset principal d'accidents nettoye")
        
        accidents_clean = df.select(
            col("ID"),
            col("Source_Clean").alias("Source"),
            col("Severity"),
            col("Start_Time_Clean").alias("Start_Time"),
            col("End_Time_Clean").alias("End_Time"),
            col("Duration_Hours"),
            col("State_Clean").alias("State"),
            col("City_Clean").alias("City"),
            col("County_Clean").alias("County"),
            col("Start_Lat").alias("Latitude"),
            col("Start_Lng").alias("Longitude"),
            
            # Conditions de route - nettoyees
            col("Bump"),
            col("Crossing"),
            col("Give_Way"),
            col("Junction"),
            col("No_Exit"),
            col("Railway"),
            col("Roundabout"),
            col("Station"),
            col("Stop"),
            col("Traffic_Calming"),
            col("Traffic_Signal"),
            col("Turning_Loop"),
            
            col("ingestion_date")
        )
        
        print("PREPROCESSOR: ETAPE 8.1 - Ajout de features temporelles")
        accidents_clean = accidents_clean.withColumn(
            "Hour_of_Day", hour(col("Start_Time"))
        ).withColumn(
            "Day_of_Week", dayofweek(col("Start_Time"))
        ).withColumn(
            "Month", month(col("Start_Time"))
        ).withColumn(
            "Year", date_format(col("Start_Time"), "yyyy")
        )
        
        print("PREPROCESSOR: ETAPE 8.2 - Ajout de features derivees")
        accidents_clean = accidents_clean.withColumn(
            "Is_Weekend",
            when(col("Day_of_Week").isin(1, 7), True).otherwise(False)
        ).withColumn(
            "Time_Period",
            when(col("Hour_of_Day").between(6, 11), "Morning")
            .when(col("Hour_of_Day").between(12, 17), "Afternoon")
            .when(col("Hour_of_Day").between(18, 21), "Evening") 
            .otherwise("Night")
        ).withColumn(
            "Severity_Category",
            when(col("Severity") == 1, "Minor")
            .when(col("Severity") == 2, "Moderate")
            .when(col("Severity") == 3, "Serious")
            .when(col("Severity") == 4, "Severe")
            .otherwise("Unknown")
        )
        
        print("PREPROCESSOR: ETAPE 8.3 - Calcul du score de complexite routiere")
        accidents_clean = accidents_clean.withColumn(
            "Road_Complexity_Score",
            (col("Bump").cast("int") + 
             col("Crossing").cast("int") + 
             col("Give_Way").cast("int") + 
             col("Junction").cast("int") + 
             col("Railway").cast("int") + 
             col("Roundabout").cast("int") + 
             col("Station").cast("int") + 
             col("Stop").cast("int") + 
             col("Traffic_Calming").cast("int") + 
             col("Traffic_Signal").cast("int") + 
             col("Turning_Loop").cast("int"))
        )
        
        print("PREPROCESSOR: ETAPE 8.4 - Categorisation de la complexite routiere")
        accidents_clean = accidents_clean.withColumn(
            "Road_Complexity_Category",
            when(col("Road_Complexity_Score") == 0, "Simple")
            .when(col("Road_Complexity_Score").between(1, 2), "Moderate")
            .when(col("Road_Complexity_Score").between(3, 5), "Complex")
            .otherwise("Very_Complex")
        )
        
        record_count = accidents_clean.count()
        print(f"PREPROCESSOR: ETAPE 8 - Dataset accidents nettoye: {record_count} enregistrements")
        
        return accidents_clean

    def save_to_silver(self, df, dataset_name, target_date):
        print(f"PREPROCESSOR: ETAPE 9 - Sauvegarde de {dataset_name} en Silver Layer")
        year, month, day = target_date.split("-")
        output_path = f"{self.silver_path}/{dataset_name}/{year}/{month}/{day}"
        
        print(f"PREPROCESSOR: ETAPE 9 - Chemin de sortie: {output_path}")
        
        try:
            print("PREPROCESSOR: ETAPE 9 - Ecriture avec optimisation Parquet/Snappy")
            df.coalesce(2) \
              .write \
              .mode("overwrite") \
              .option("compression", "snappy") \
              .parquet(output_path)
            
            # Vérification post-sauvegarde
            saved_count = self.spark.read.parquet(output_path).count()
            print(f"PREPROCESSOR: ETAPE 9 - Sauvegarde reussie: {saved_count} enregistrements dans {dataset_name}")
            
        except Exception as e:
            print(f"PREPROCESSOR: ETAPE 9 - ERREUR lors de la sauvegarde de {dataset_name}: {str(e)}")
            raise

    def run_preprocessing(self, target_date):
        start_time = datetime.now()
        print(f"PREPROCESSOR: DEBUT - Preprocessing Silver Layer pour {target_date}")
        print("PREPROCESSOR: =============================================================")
        
        try:
            # ETAPE 0 et 1: Chargement
            bronze_path = self.get_latest_bronze_partition(target_date)
            raw_df = self.load_bronze_data(bronze_path)
            
            # ETAPE 2: Analyse des valeurs manquantes
            missing_analysis = self.analyze_missing_values(raw_df)
            
            # ETAPE 3: Gestion des valeurs manquantes
            clean_df = self.handle_missing_values(raw_df)
            
            # ETAPE 4: Validation de la qualité
            validated_df = self.validate_data_quality(clean_df)
            
            # ETAPE 5: Standardisation et nettoyage
            standardized_df = self.standardize_and_clean_data(validated_df)
            
            # ETAPES 6-8: Création des datasets spécialisés
            print("PREPROCESSOR: Creation des datasets specialises Silver")
            weather_clean = self.create_weather_dataset(standardized_df)
            location_clean = self.create_location_dataset(standardized_df)
            accidents_clean = self.create_accidents_clean_dataset(standardized_df)
            
            # ETAPE 9: Sauvegarde
            print("PREPROCESSOR: Sauvegarde des datasets en Silver Layer")
            self.save_to_silver(accidents_clean, "accidents_clean", target_date)
            self.save_to_silver(weather_clean, "weather_clean", target_date)
            self.save_to_silver(location_clean, "location_clean", target_date)
            
            # Statistiques finales
            execution_time = datetime.now() - start_time
            print("PREPROCESSOR: =============================================================")
            print(f"PREPROCESSOR: FIN - Preprocessing termine en {execution_time.total_seconds():.2f} secondes")
            print(f"PREPROCESSOR: FIN - 3 datasets Silver crees pour {target_date}")
            
        except Exception as e:
            print(f"PREPROCESSOR: ERREUR FATALE - Echec du preprocessing: {str(e)}")
            raise
        finally:
            print("PREPROCESSOR: Arret de Spark Session")
            self.spark.stop()

def main():
    print("PREPROCESSOR: Point d'entree principal")
    parser = argparse.ArgumentParser(description='US Accidents Silver Preprocessor Ameliore')
    parser.add_argument('--date', required=True,
                       choices=['2025-01-01', '2025-01-02', '2025-01-03'],
                       help='Target date for preprocessing')
    
    args = parser.parse_args()
    print(f"PREPROCESSOR: Date cible: {args.date}")
    
    # Créer et exécuter le preprocessor
    preprocessor = USAccidentsPreprocessor()
    preprocessor.run_preprocessing(args.date)
    
    print("PREPROCESSOR: Execution terminee")

if __name__ == "__main__":
    main()