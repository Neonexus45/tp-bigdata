import argparse
import sys
from datetime import datetime
from typing import Optional

from src.core.config import Config
from src.core.logger import get_logger
from src.core.spark_manager import SparkManager
from src.utils.helpers import get_partition_path, format_execution_time, validate_file_path
from src.datamart.analyzers import WeatherFrequencyAnalyzer, WeatherSeverityAnalyzer, WeatherTrendsAnalyzer
from src.datamart.database_writer import AnalyticsWriter

class WeatherAnalysisApp:
    
    def __init__(self):
        self.config = Config()
        self.logger = get_logger(__name__)
        self.spark_manager = SparkManager()
        self.analytics_writer = AnalyticsWriter()
        
        self.frequency_analyzer = WeatherFrequencyAnalyzer(self.spark_manager)
        self.severity_analyzer = WeatherSeverityAnalyzer(self.spark_manager)
        self.trends_analyzer = WeatherTrendsAnalyzer(self.spark_manager)
        
        self.logger.info("Weather Analysis App initialized")
    
    def get_silver_data_paths(self, target_date: str) -> tuple:
        weather_path = get_partition_path(f"{self.config.silver_output_path}/weather_clean", target_date)
        accidents_path = get_partition_path(f"{self.config.silver_output_path}/accidents_clean", target_date)
        
        if not validate_file_path(weather_path):
            raise FileNotFoundError(f"Weather data not found: {weather_path}")
        if not validate_file_path(accidents_path):
            raise FileNotFoundError(f"Accidents data not found: {accidents_path}")
        
        return weather_path, accidents_path
    
    def load_silver_datasets(self, target_date: str):
        self.logger.info("Loading Silver layer datasets")
        
        weather_path, accidents_path = self.get_silver_data_paths(target_date)
        
        weather_df = self.spark_manager.read_parquet(weather_path)
        accidents_df = self.spark_manager.read_parquet(accidents_path)
        
        weather_count = weather_df.count()
        accidents_count = accidents_df.count()
        
        self.logger.info(f"Loaded weather data: {weather_count} records")
        self.logger.info(f"Loaded accidents data: {accidents_count} records")
        
        return weather_df, accidents_df
    
    def run_frequency_analysis(self, weather_df, accidents_df, target_date: str):
        self.logger.info("Running weather frequency analysis")
        
        frequency_data = self.frequency_analyzer.analyze_weather_accident_frequency(weather_df, accidents_df)
        self.analytics_writer.write_weather_frequency_data(frequency_data, target_date)
        
        self.logger.info("Weather frequency analysis completed")
    
    def run_severity_analysis(self, weather_df, accidents_df, target_date: str):
        self.logger.info("Running weather severity correlation analysis")
        
        severity_data = self.severity_analyzer.analyze_severity_correlation(weather_df, accidents_df)
        self.analytics_writer.write_severity_correlation_data(severity_data, target_date)
        
        self.logger.info("Weather severity analysis completed")
    
    def run_trends_analysis(self, weather_df, accidents_df, target_date: str):
        self.logger.info("Running weather trends analysis")
        
        trends_data = self.trends_analyzer.analyze_weather_trends(weather_df, accidents_df)
        self.analytics_writer.write_weather_trends_data(trends_data, target_date)
        
        self.logger.info("Weather trends analysis completed")
    
    def run_analysis(self, target_date: str):
        start_time = datetime.now()
        self.logger.info(f"Starting weather impact analysis for {target_date}")
        
        try:
            self.logger.info("Step 1 - Creating database tables")
            self.analytics_writer.create_tables()
            
            self.logger.info("Step 2 - Loading Silver layer data")
            weather_df, accidents_df = self.load_silver_datasets(target_date)
            
            self.logger.info("Step 3 - Running frequency analysis")
            self.run_frequency_analysis(weather_df, accidents_df, target_date)
            
            self.logger.info("Step 4 - Running severity correlation analysis")
            self.run_severity_analysis(weather_df, accidents_df, target_date)
            
            self.logger.info("Step 5 - Running trends analysis")
            self.run_trends_analysis(weather_df, accidents_df, target_date)
            
            end_time = datetime.now()
            execution_time_str = format_execution_time(start_time, end_time)
            self.logger.info(f"Weather analysis completed in {execution_time_str}")
            
        except Exception as e:
            self.logger.error(f"Weather analysis failed: {str(e)}")
            raise
        finally:
            self.analytics_writer.close_connection()
            self.spark_manager.stop_session()

def main():
    from src.utils.helpers import get_incremental_dates
    
    logger = get_logger(__name__)
    logger.info("Starting Weather Analysis Application")
    
    parser = argparse.ArgumentParser(description='Weather Impact Analysis for Accidents')
    parser.add_argument('--date', required=True,
                       choices=get_incremental_dates(),
                       help='Target date for weather analysis')
    
    args = parser.parse_args()
    logger.info(f"Target date: {args.date}")
    
    try:
        app = WeatherAnalysisApp()
        app.run_analysis(args.date)
        
        logger.info("Weather analysis execution completed successfully")
        
    except Exception as e:
        logger.error(f"Weather analysis execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()