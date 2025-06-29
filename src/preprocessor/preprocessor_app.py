import argparse
import sys
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
from typing import Dict, Any, List, Tuple

from src.core.config import Config
from src.core.logger import get_logger
from src.core.spark_manager import SparkManager
from src.utils.helpers import (
    get_partition_path,
    format_execution_time,
    validate_file_path,
    analyze_dataframe_quality
)

class USAccidentsPreprocessor:
    """US Accidents Silver Layer Preprocessor with comprehensive data cleaning."""
    
    def __init__(self) -> None:
        """Initialize the preprocessor with core classes."""
        self.config = Config()
        self.logger = get_logger(__name__)
        self.spark_manager = SparkManager()
        
        self.logger.info("Initializing US Accidents Preprocessor")
        self.logger.info("Preprocessor initialized successfully")

    def get_latest_bronze_partition(self, target_date: str) -> str:
        """Get the Bronze partition path for the target date.
        
        Args:
            target_date: Target date in YYYY-MM-DD format
            
        Returns:
            Path to Bronze partition
            
        Raises:
            FileNotFoundError: If Bronze partition doesn't exist
        """
        self.logger.info(f"STEP 0 - Getting Bronze partition for {target_date}")
        
        bronze_date_path = get_partition_path(self.config.bronze_output_path, target_date)
        
        self.logger.info(f"Bronze path: {bronze_date_path}")
        
        if not validate_file_path(bronze_date_path):
            self.logger.error(f"Bronze partition not found: {bronze_date_path}")
            raise FileNotFoundError(f"Bronze partition not found: {bronze_date_path}")
        
        return bronze_date_path

    def load_bronze_data(self, bronze_path: str):
        """Load data from Bronze layer.
        
        Args:
            bronze_path: Path to Bronze layer data
            
        Returns:
            DataFrame: Loaded Bronze data
        """
        self.logger.info("STEP 1 - Loading data from Bronze layer")
        try:
            df = self.spark_manager.read_parquet(bronze_path)
            record_count = df.count()
            column_count = len(df.columns)
            
            self.logger.info(f"STEP 1 - Loaded {record_count} records with {column_count} columns")
            self.logger.debug(f"STEP 1 - Loaded columns: {df.columns}")
            
            self.logger.debug("STEP 1 - Data types analysis:")
            for field in df.schema.fields:
                self.logger.debug(f"  {field.name}: {field.dataType}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"STEP 1 - Error loading Bronze data: {str(e)}")
            raise

    def analyze_missing_values(self, df) -> List[Tuple[str, int, float]]:
        """Analyze missing values in the DataFrame.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of tuples containing (column_name, null_count, missing_percentage)
        """
        self.logger.info("STEP 2 - Analyzing missing values")
        
        total_records = df.count()
        self.logger.info(f"STEP 2 - Total records: {total_records}")
        
        missing_analysis = []
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            missing_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
            missing_analysis.append((column, null_count, missing_percentage))
            
            if null_count > 0:
                self.logger.info(f"STEP 2 - {column}: {null_count} missing values ({missing_percentage:.2f}%)")
        
        return missing_analysis

    def handle_missing_values(self, df):
        """Handle missing values systematically by column type.
        
        Args:
            df: DataFrame with missing values
            
        Returns:
            DataFrame: DataFrame with missing values handled
        """
        self.logger.info("STEP 3 - Systematic handling of missing values")
        
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
        
        self.logger.info("STEP 3.1 - Processing boolean columns")
        for col_name in boolean_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, coalesce(col(col_name), lit(False)))
                self.logger.debug(f"STEP 3.1 - {col_name}: missing values -> False")
        
        self.logger.info("STEP 3.2 - Processing text columns")
        for col_name in text_columns:
            if col_name in df.columns:
                if col_name == "ID":
                    df = df.filter(col(col_name).isNotNull())
                    self.logger.info(f"STEP 3.2 - {col_name}: removing records with null ID")
                else:
                    df = df.withColumn(col_name, coalesce(col(col_name), lit("Unknown")))
                    self.logger.debug(f"STEP 3.2 - {col_name}: missing values -> 'Unknown'")
        
        self.logger.info("STEP 3.3 - Processing numeric columns")
        for col_name in numeric_columns:
            if col_name in df.columns:
                if col_name in ["Start_Lat", "Start_Lng"]:
                    df = df.filter(col(col_name).isNotNull())
                    self.logger.info(f"STEP 3.3 - {col_name}: removing records with null coordinates")
                elif col_name == "Severity":
                    df = df.withColumn(col_name, coalesce(col(col_name), lit(2)))
                    self.logger.debug(f"STEP 3.3 - {col_name}: missing values -> 2 (moderate)")
                else:
                    if col_name == "Temperature(F)":
                        default_val = lit(70.0)  # Average temperature
                    elif col_name == "Humidity(%)":
                        default_val = lit(50.0)  # Average humidity
                    elif col_name == "Pressure(in)":
                        default_val = lit(30.0)  # Standard pressure
                    elif col_name == "Visibility(mi)":
                        default_val = lit(10.0)  # Standard visibility
                    else:
                        default_val = lit(0.0)   # Default for wind speed, precipitation
                    
                    df = df.withColumn(col_name, coalesce(col(col_name), default_val))
                    self.logger.debug(f"STEP 3.3 - {col_name}: missing values -> {default_val}")
        
        self.logger.info("STEP 3.4 - Processing datetime columns")
        for col_name in datetime_columns:
            if col_name in df.columns:
                df = df.filter(col(col_name).isNotNull())
                self.logger.info(f"STEP 3.4 - {col_name}: removing records with null datetime")
        
        final_count = df.count()
        self.logger.info(f"STEP 3 - Completed: {final_count} records after handling missing values")
        
        return df

    def validate_data_quality(self, df):
        """Validate data quality by removing duplicates and invalid values.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame: Validated DataFrame
        """
        self.logger.info("STEP 4 - Data quality validation")
        
        initial_count = df.count()
        self.logger.info(f"STEP 4 - Initial count: {initial_count}")
        
        self.logger.info("STEP 4.1 - Removing duplicates")
        df_dedup = df.dropDuplicates(["ID"])
        dedup_count = df_dedup.count()
        duplicates_removed = initial_count - dedup_count
        self.logger.info(f"STEP 4.1 - Removed {duplicates_removed} duplicates")
        
        self.logger.info("STEP 4.2 - Validating geographic coordinates")
        df_geo = df_dedup.filter(
            (col("Start_Lat").between(-90, 90)) &
            (col("Start_Lng").between(-180, 180)) &
            (col("Start_Lat") != 0) &
            (col("Start_Lng") != 0)
        )
        geo_count = df_geo.count()
        geo_invalid = dedup_count - geo_count
        self.logger.info(f"STEP 4.2 - Removed {geo_invalid} records with invalid coordinates")
        
        self.logger.info("STEP 4.3 - Validating severity values")
        df_severity = df_geo.filter(col("Severity").between(1, 4))
        severity_count = df_severity.count()
        severity_invalid = geo_count - severity_count
        self.logger.info(f"STEP 4.3 - Removed {severity_invalid} records with invalid severity")
        
        return df_severity

    def standardize_and_clean_data(self, df):
        """Standardize and clean data with timestamp conversion and text normalization.
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            DataFrame: Standardized DataFrame
        """
        self.logger.info("STEP 5 - Data standardization and cleaning")
        
        self.logger.info("STEP 5.1 - Converting timestamps")
        df = df.withColumn(
            "Start_Time_Clean",
            to_timestamp(col("Start_Time"), "yyyy-MM-dd HH:mm:ss")
        ).withColumn(
            "End_Time_Clean",
            to_timestamp(col("End_Time"), "yyyy-MM-dd HH:mm:ss")
        )
        
        self.logger.info("STEP 5.2 - Calculating duration")
        df = df.withColumn(
            "Duration_Hours",
            (unix_timestamp(col("End_Time_Clean")) - unix_timestamp(col("Start_Time_Clean"))) / 3600
        )
        
        self.logger.info("STEP 5.3 - Filtering aberrant durations")
        df = df.filter(
            col("Duration_Hours").isNotNull() &
            (col("Duration_Hours") >= 0) &
            (col("Duration_Hours") <= 168)  # Max 7 days
        )
        
        self.logger.info("STEP 5.4 - Standardizing text fields")
        df = df.withColumn("State_Clean", upper(trim(col("State")))) \
              .withColumn("City_Clean", trim(col("City"))) \
              .withColumn("County_Clean", trim(col("County"))) \
              .withColumn("Source_Clean", trim(col("Source"))) \
              .withColumn("Weather_Condition_Clean", trim(col("Weather_Condition"))) \
              .withColumn("Wind_Direction_Clean", trim(col("Wind_Direction")))
        
        self.logger.info("STEP 5.5 - Validating weather data")
        df = self._validate_weather_data(df)
        
        final_count = df.count()
        self.logger.info(f"STEP 5 - Completed: {final_count} records after standardization")
        
        return df

    def _validate_weather_data(self, df):
        """Validate weather data with realistic value ranges.
        
        Args:
            df: DataFrame with weather data
            
        Returns:
            DataFrame: DataFrame with validated weather data
        """
        self.logger.debug("STEP 5.5 - Specific weather data validation")
        
        self.logger.debug("STEP 5.5.1 - Validating temperature (-50F to 130F)")
        df = df.withColumn(
            "Temperature_F_Clean",
            when(col("Temperature(F)").between(-50, 130), col("Temperature(F)"))
            .otherwise(lit(70.0))  # Default value instead of null
        )
        
        self.logger.debug("STEP 5.5.2 - Validating humidity (0-100%)")
        df = df.withColumn(
            "Humidity_Pct_Clean",
            when(col("Humidity(%)").between(0, 100), col("Humidity(%)"))
            .otherwise(lit(50.0))  # Default value
        )
        
        self.logger.debug("STEP 5.5.3 - Validating pressure (25-35 inches)")
        df = df.withColumn(
            "Pressure_In_Clean",
            when(col("Pressure(in)").between(25, 35), col("Pressure(in)"))
            .otherwise(lit(30.0))  # Default value
        )
        
        self.logger.debug("STEP 5.5.4 - Validating visibility (0-15 miles)")
        df = df.withColumn(
            "Visibility_Mi_Clean",
            when(col("Visibility(mi)").between(0, 15), col("Visibility(mi)"))
            .otherwise(lit(10.0))  # Default value
        )
        
        self.logger.debug("STEP 5.5.5 - Validating wind speed (0-200 mph)")
        df = df.withColumn(
            "Wind_Speed_Mph_Clean",
            when(col("Wind_Speed(mph)").between(0, 200), col("Wind_Speed(mph)"))
            .otherwise(lit(0.0))  # Default value
        )
        
        self.logger.debug("STEP 5.5.6 - Validating precipitation (0-10 inches)")
        df = df.withColumn(
            "Precipitation_In_Clean",
            when(col("Precipitation(in)").between(0, 10), col("Precipitation(in)"))
            .otherwise(lit(0.0))  # Default value
        )
        
        return df

    def create_weather_dataset(self, df):
        """Create normalized weather dataset with categorization.
        
        Args:
            df: Standardized DataFrame
            
        Returns:
            DataFrame: Weather dataset with categories and scores
        """
        self.logger.info("STEP 6 - Creating normalized weather dataset")
        
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
        
        self.logger.info("STEP 6.1 - Adding weather categories")
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
        
        self.logger.info("STEP 6.2 - Adding temperature categories")
        weather_df = weather_df.withColumn(
            "Temperature_Category",
            when(col("Temperature_F") < 32, "Freezing")
            .when(col("Temperature_F") < 50, "Cold")
            .when(col("Temperature_F") < 70, "Mild")
            .when(col("Temperature_F") < 85, "Warm")
            .otherwise("Hot")
        )
        
        self.logger.info("STEP 6.3 - Adding weather indicators")
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
        self.logger.info(f"STEP 6 - Weather dataset: {record_count} records")
        
        return weather_df

    def create_location_dataset(self, df):
        """Create enriched geographic dataset with regional classification.
        
        Args:
            df: Standardized DataFrame
            
        Returns:
            DataFrame: Location dataset with geographic enrichment
        """
        self.logger.info("STEP 7 - Creating enriched geographic dataset")
        
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
        
        self.logger.info("STEP 7.1 - Geographic enrichment - US regions")
        location_df = location_df.withColumn(
            "Region",
            when(col("State").isin("ME", "NH", "VT", "MA", "RI", "CT"), "Northeast")
            .when(col("State").isin("NY", "NJ", "PA"), "Mid-Atlantic")
            .when(col("State").isin("OH", "MI", "IN", "WI", "IL", "MN", "IA", "MO", "ND", "SD", "NE", "KS"), "Midwest")
            .when(col("State").isin("DE", "MD", "DC", "VA", "WV", "KY", "TN", "NC", "SC", "GA", "FL", "AL", "MS", "AR", "LA", "OK", "TX"), "South")
            .when(col("State").isin("MT", "WY", "CO", "NM", "ID", "UT", "NV", "AZ", "WA", "OR", "CA", "AK", "HI"), "West")
            .otherwise("Other")
        )
        
        self.logger.info("STEP 7.2 - Urban/rural area classification")
        location_df = location_df.withColumn(
            "Area_Type",
            when((col("City") != "Unknown") & (col("City").isNotNull()), "Urban")
            .otherwise("Rural")
        )
        
        self.logger.info("STEP 7.3 - Adding rounded coordinates for clustering")
        location_df = location_df.withColumn(
            "Latitude_Rounded",
            spark_round(col("Latitude"), 2)
        ).withColumn(
            "Longitude_Rounded",
            spark_round(col("Longitude"), 2)
        )
        
        record_count = location_df.count()
        self.logger.info(f"STEP 7 - Location dataset: {record_count} records")
        
        return location_df

    def create_accidents_clean_dataset(self, df):
        """Create main cleaned accidents dataset with derived features.
        
        Args:
            df: Standardized DataFrame
            
        Returns:
            DataFrame: Clean accidents dataset with temporal and road complexity features
        """
        self.logger.info("STEP 8 - Creating main cleaned accidents dataset")
        
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
            
            # Road conditions - cleaned
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
        
        self.logger.info("STEP 8.1 - Adding temporal features")
        accidents_clean = accidents_clean.withColumn(
            "Hour_of_Day", hour(col("Start_Time"))
        ).withColumn(
            "Day_of_Week", dayofweek(col("Start_Time"))
        ).withColumn(
            "Month", month(col("Start_Time"))
        ).withColumn(
            "Year", date_format(col("Start_Time"), "yyyy")
        )
        
        self.logger.info("STEP 8.2 - Adding derived features")
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
        
        self.logger.info("STEP 8.3 - Calculating road complexity score")
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
        
        self.logger.info("STEP 8.4 - Road complexity categorization")
        accidents_clean = accidents_clean.withColumn(
            "Road_Complexity_Category",
            when(col("Road_Complexity_Score") == 0, "Simple")
            .when(col("Road_Complexity_Score").between(1, 2), "Moderate")
            .when(col("Road_Complexity_Score").between(3, 5), "Complex")
            .otherwise("Very_Complex")
        )
        
        record_count = accidents_clean.count()
        self.logger.info(f"STEP 8 - Clean accidents dataset: {record_count} records")
        
        return accidents_clean

    def save_to_silver(self, df, dataset_name: str, target_date: str) -> None:
        """Save dataset to Silver layer with date partitioning.
        
        Args:
            df: DataFrame to save
            dataset_name: Name of the dataset
            target_date: Target date for partitioning
        """
        self.logger.info(f"STEP 9 - Saving {dataset_name} to Silver Layer")
        
        # Generate partitioned output path
        base_path = f"{self.config.silver_output_path}/{dataset_name}"
        output_path = get_partition_path(base_path, target_date)
        
        self.logger.info(f"STEP 9 - Output path: {output_path}")
        
        try:
            self.logger.info("STEP 9 - Writing with Parquet/Snappy optimization")
            # Use SparkManager to write parquet
            self.spark_manager.write_parquet(df, output_path, mode="overwrite")
            
            # Post-save verification
            saved_count = self.spark_manager.read_parquet(output_path).count()
            self.logger.info(f"STEP 9 - Successfully saved: {saved_count} records in {dataset_name}")
            
        except Exception as e:
            self.logger.error(f"STEP 9 - Error saving {dataset_name}: {str(e)}")
            raise

    def run_preprocessing(self, target_date: str) -> None:
        """Run the complete preprocessing pipeline for the target date.
        
        Args:
            target_date: Target date for preprocessing
        """
        start_time = datetime.now()
        self.logger.info(f"START - Silver Layer preprocessing for {target_date}")
        self.logger.info("=" * 65)
        
        try:

            bronze_path = self.get_latest_bronze_partition(target_date)
            raw_df = self.load_bronze_data(bronze_path)
            
            missing_analysis = self.analyze_missing_values(raw_df)
            
            clean_df = self.handle_missing_values(raw_df)
            
            validated_df = self.validate_data_quality(clean_df)
            
            standardized_df = self.standardize_and_clean_data(validated_df)
            
            self.logger.info("Creating specialized Silver datasets")
            weather_clean = self.create_weather_dataset(standardized_df)
            location_clean = self.create_location_dataset(standardized_df)
            accidents_clean = self.create_accidents_clean_dataset(standardized_df)
            
            self.logger.info("Saving datasets to Silver Layer")
            self.save_to_silver(accidents_clean, "accidents_clean", target_date)
            self.save_to_silver(weather_clean, "weather_clean", target_date)
            self.save_to_silver(location_clean, "location_clean", target_date)
            
            end_time = datetime.now()
            execution_time_str = format_execution_time(start_time, end_time)
            self.logger.info("=" * 65)
            self.logger.info(f"END - Preprocessing completed in {execution_time_str}")
            self.logger.info(f"END - 3 Silver datasets created for {target_date}")
            
        except Exception as e:
            self.logger.error(f"FATAL ERROR - Preprocessing failed: {str(e)}")
            raise
        finally:
            self.logger.info("Stopping Spark session")
            self.spark_manager.stop_session()

def main() -> None:
    """Main entry point for the US Accidents Preprocessor application."""
    from src.utils.helpers import get_incremental_dates
    
    logger = get_logger(__name__)
    logger.info("Starting US Accidents Preprocessor application")
    
    parser = argparse.ArgumentParser(description='US Accidents Silver Preprocessor Enhanced')
    parser.add_argument('--date', required=True,
                       choices=get_incremental_dates(),
                       help='Target date for preprocessing')
    
    args = parser.parse_args()
    logger.info(f"Target date: {args.date}")
    
    try:

        preprocessor = USAccidentsPreprocessor()
        preprocessor.run_preprocessing(args.date)
        
        logger.info("Preprocessor execution completed successfully")
        
    except Exception as e:
        logger.error(f"Preprocessor execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()