"""Spark session management for US Accidents Lakehouse project."""

import os
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from .config import Config
from .logger import get_logger


class SparkManager:
    """Singleton Spark session manager."""
    
    _instance: Optional['SparkManager'] = None
    _spark_session: Optional[SparkSession] = None
    
    def __new__(cls) -> 'SparkManager':
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """Initialize Spark manager with configuration."""
        if not hasattr(self, '_initialized'):
            self.config = Config()
            self.logger = get_logger(__name__)
            self._initialized = True
    
    def get_spark_session(self) -> SparkSession:
        """Get or create Spark session with optimized configuration.
        
        Returns:
            Configured Spark session
        """
        if self._spark_session is None:
            self.logger.info("Initializing Spark session")
            
            builder = SparkSession.builder.appName("USAccidents-Lakehouse")
            
            # Use minimal configuration for Windows compatibility
            builder = builder.master("local[*]")
            builder = builder.config("spark.ui.enabled", "false")
            builder = builder.config("spark.driver.memory", "2g")
            builder = builder.config("spark.executor.memory", "2g")
            builder = builder.config("spark.sql.adaptive.enabled", "true")
            builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
            # Set temp directory to avoid path issues
            import tempfile
            temp_dir = tempfile.gettempdir().replace("\\", "/")
            builder = builder.config("spark.local.dir", temp_dir)
            
            self._spark_session = builder.getOrCreate()
            self._spark_session.sparkContext.setLogLevel("WARN")
            
            self.logger.info("Spark session initialized successfully")
            self._log_spark_config()
        
        return self._spark_session
    
    def _log_spark_config(self) -> None:
        """Log current Spark configuration."""
        if self._spark_session:
            try:
                # Try the newer method first
                conf = dict(self._spark_session.conf.getAll())
                self.logger.debug("Spark configuration:")
                for key, value in conf.items():
                    if "password" not in key.lower():
                        self.logger.debug(f"  {key}: {value}")
            except AttributeError:
                # Fallback for older Spark versions
                self.logger.debug("Spark configuration logging not available in this version")
    
    def read_csv(self, path: str, **options) -> DataFrame:
        """Read CSV file with default options optimized for US Accidents data.
        
        Args:
            path: Path to CSV file
            **options: Additional Spark read options
            
        Returns:
            Spark DataFrame
        """
        spark = self.get_spark_session()
        self.logger.info(f"Reading CSV from: {path}")
        
        default_options = {
            "header": "true",
            "inferSchema": "true",
            "multiline": "true",
            "escape": '"'
        }
        default_options.update(options)
        
        try:
            df = spark.read.options(**default_options).csv(path)
            record_count = df.count()
            self.logger.info(f"Successfully loaded {record_count} records from CSV")
            return df
        except Exception as e:
            self.logger.error(f"Error reading CSV file {path}: {str(e)}")
            raise
    
    def read_parquet(self, path: str) -> DataFrame:
        """Read Parquet file.
        
        Args:
            path: Path to Parquet file or directory
            
        Returns:
            Spark DataFrame
        """
        spark = self.get_spark_session()
        self.logger.info(f"Reading Parquet from: {path}")
        
        try:
            df = spark.read.parquet(path)
            record_count = df.count()
            self.logger.info(f"Successfully loaded {record_count} records from Parquet")
            return df
        except Exception as e:
            self.logger.error(f"Error reading Parquet file {path}: {str(e)}")
            raise
    
    def write_parquet(self, df: DataFrame, path: str, mode: str = 'overwrite', 
                     partition_by: Optional[List[str]] = None) -> None:
        """Write DataFrame to Parquet format with optimization.
        
        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode (overwrite, append, etc.)
            partition_by: List of columns to partition by
        """
        self.logger.info(f"Writing Parquet to: {path}")
        
        try:
            writer = df.coalesce(2).write.mode(mode).option("compression", "snappy")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.parquet(path)
            
            saved_count = self.read_parquet(path).count()
            self.logger.info(f"Successfully wrote {saved_count} records to Parquet")
            
        except Exception as e:
            self.logger.error(f"Error writing Parquet file {path}: {str(e)}")
            raise
    
    def write_to_mysql(self, df: DataFrame, table: str, mode: str = 'overwrite') -> None:
        """Write DataFrame to MySQL table (backward compatibility - uses analytics DB).
        
        Args:
            df: DataFrame to write
            table: Target table name
            mode: Write mode (overwrite, append, etc.)
        """
        self.write_to_analytics_db(df, table, mode)
    
    def write_to_analytics_db(self, df: DataFrame, table: str, mode: str = 'overwrite') -> None:
        """Write DataFrame to MySQL Analytics database for weather impact analysis.
        
        Args:
            df: DataFrame to write
            table: Target table name
            mode: Write mode (overwrite, append, etc.)
        """
        self.logger.info(f"Writing to Analytics MySQL table: {table}")
        
        try:
            connection_string = self.config.mysql_analytics_connection_string
            
            df.write \
              .format("jdbc") \
              .option("url", connection_string) \
              .option("dbtable", table) \
              .mode(mode) \
              .save()
            
            self.logger.info(f"Successfully wrote data to Analytics MySQL table: {table}")
            
        except Exception as e:
            self.logger.error(f"Error writing to Analytics MySQL table {table}: {str(e)}")
            raise
    
    def write_to_ml_db(self, df: DataFrame, table: str, mode: str = 'overwrite') -> None:
        """Write DataFrame to MySQL ML database for machine learning models.
        
        Args:
            df: DataFrame to write
            table: Target table name
            mode: Write mode (overwrite, append, etc.)
        """
        self.logger.info(f"Writing to ML MySQL table: {table}")
        
        try:
            connection_string = self.config.mysql_ml_connection_string
            
            df.write \
              .format("jdbc") \
              .option("url", connection_string) \
              .option("dbtable", table) \
              .mode(mode) \
              .save()
            
            self.logger.info(f"Successfully wrote data to ML MySQL table: {table}")
            
        except Exception as e:
            self.logger.error(f"Error writing to ML MySQL table {table}: {str(e)}")
            raise
    
    def read_from_analytics_db(self, table: str) -> DataFrame:
        """Read DataFrame from MySQL Analytics database.
        
        Args:
            table: Source table name
            
        Returns:
            Spark DataFrame
        """
        self.logger.info(f"Reading from Analytics MySQL table: {table}")
        
        try:
            connection_string = self.config.mysql_analytics_connection_string
            spark = self.get_spark_session()
            
            df = spark.read \
                .format("jdbc") \
                .option("url", connection_string) \
                .option("dbtable", table) \
                .load()
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} records from Analytics MySQL table: {table}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading from Analytics MySQL table {table}: {str(e)}")
            raise
    
    def read_from_ml_db(self, table: str) -> DataFrame:
        """Read DataFrame from MySQL ML database.
        
        Args:
            table: Source table name
            
        Returns:
            Spark DataFrame
        """
        self.logger.info(f"Reading from ML MySQL table: {table}")
        
        try:
            connection_string = self.config.mysql_ml_connection_string
            spark = self.get_spark_session()
            
            df = spark.read \
                .format("jdbc") \
                .option("url", connection_string) \
                .option("dbtable", table) \
                .load()
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} records from ML MySQL table: {table}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading from ML MySQL table {table}: {str(e)}")
            raise
    
    def write_datamart_analysis(self, df: DataFrame, analysis_type: str,
                               mode: str = 'overwrite', partition_by: Optional[List[str]] = None) -> None:
        """Write DataFrame to datamart analysis paths.
        
        Args:
            df: DataFrame to write
            analysis_type: Type of analysis ('weather', 'frequency', 'severity')
            mode: Write mode (overwrite, append, etc.)
            partition_by: List of columns to partition by
        """
        path_mapping = {
            'weather': self.config.weather_analysis_path,
            'frequency': self.config.accident_frequency_path,
            'severity': self.config.severity_correlation_path
        }
        
        if analysis_type not in path_mapping:
            raise ValueError(f"Invalid analysis type: {analysis_type}. Must be one of: {list(path_mapping.keys())}")
        
        output_path = path_mapping[analysis_type]
        self.logger.info(f"Writing {analysis_type} analysis to: {output_path}")
        
        try:
            writer = df.coalesce(2).write.mode(mode).option("compression", "snappy")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.parquet(output_path)
            
            saved_count = self.read_parquet(output_path).count()
            self.logger.info(f"Successfully wrote {saved_count} records for {analysis_type} analysis")
            
        except Exception as e:
            self.logger.error(f"Error writing {analysis_type} analysis to {output_path}: {str(e)}")
            raise
    
    def write_ml_output(self, df: DataFrame, output_type: str,
                       mode: str = 'overwrite', partition_by: Optional[List[str]] = None) -> None:
        """Write DataFrame to ML output paths.
        
        Args:
            df: DataFrame to write
            output_type: Type of ML output ('models', 'predictions', 'features')
            mode: Write mode (overwrite, append, etc.)
            partition_by: List of columns to partition by
        """
        path_mapping = {
            'models': self.config.ml_models_path,
            'predictions': self.config.ml_predictions_path,
            'features': self.config.ml_features_path
        }
        
        if output_type not in path_mapping:
            raise ValueError(f"Invalid ML output type: {output_type}. Must be one of: {list(path_mapping.keys())}")
        
        output_path = path_mapping[output_type]
        self.logger.info(f"Writing ML {output_type} to: {output_path}")
        
        try:
            writer = df.coalesce(2).write.mode(mode).option("compression", "snappy")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.parquet(output_path)
            
            saved_count = self.read_parquet(output_path).count()
            self.logger.info(f"Successfully wrote {saved_count} records for ML {output_type}")
            
        except Exception as e:
            self.logger.error(f"Error writing ML {output_type} to {output_path}: {str(e)}")
            raise
    
    def get_database_connection_info(self) -> Dict[str, str]:
        """Get database connection information for debugging/monitoring.
        
        Returns:
            Dictionary with connection information (passwords masked)
        """
        return {
            "analytics_db": {
                "host": os.getenv("MYSQL_ANALYTICS_HOST"),
                "port": os.getenv("MYSQL_ANALYTICS_PORT"),
                "database": os.getenv("MYSQL_ANALYTICS_DATABASE"),
                "user": os.getenv("MYSQL_ANALYTICS_USER")
            },
            "ml_db": {
                "host": os.getenv("MYSQL_ML_HOST"),
                "port": os.getenv("MYSQL_ML_PORT"),
                "database": os.getenv("MYSQL_ML_DATABASE"),
                "user": os.getenv("MYSQL_ML_USER")
            }
        }
    
    def stop_session(self) -> None:
        """Stop the Spark session."""
        if self._spark_session:
            self.logger.info("Stopping Spark session")
            self._spark_session.stop()
            self._spark_session = None
            self.logger.info("Spark session stopped")
    
    def __del__(self) -> None:
        """Cleanup Spark session on object destruction."""
        self.stop_session()