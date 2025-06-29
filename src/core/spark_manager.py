"""Spark session management for US Accidents Lakehouse project."""

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
            
            spark_config = self.config.spark_config
            for key, value in spark_config.items():
                if key in ["spark.ui.enabled", "spark.ui.port"]:
                    continue
                builder = builder.config(key, value)
            
            if spark_config.get("spark.ui.enabled", True):
                builder = builder.config("spark.ui.enabled", "true")
                builder = builder.config("spark.ui.port", str(spark_config.get("spark.ui.port", 4040)))
            else:
                builder = builder.config("spark.ui.enabled", "false")
            
            self._spark_session = builder.getOrCreate()
            self._spark_session.sparkContext.setLogLevel("WARN")
            
            self.logger.info("Spark session initialized successfully")
            self._log_spark_config()
        
        return self._spark_session
    
    def _log_spark_config(self) -> None:
        """Log current Spark configuration."""
        if self._spark_session:
            conf = self._spark_session.conf.getAll()
            self.logger.debug("Spark configuration:")
            for key, value in conf:
                if "password" not in key.lower():
                    self.logger.debug(f"  {key}: {value}")
    
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
        """Write DataFrame to MySQL table.
        
        Args:
            df: DataFrame to write
            table: Target table name
            mode: Write mode (overwrite, append, etc.)
        """
        self.logger.info(f"Writing to MySQL table: {table}")
        
        try:
            connection_string = self.config.mysql_connection_string
            
            df.write \
              .format("jdbc") \
              .option("url", connection_string) \
              .option("dbtable", table) \
              .mode(mode) \
              .save()
            
            self.logger.info(f"Successfully wrote data to MySQL table: {table}")
            
        except Exception as e:
            self.logger.error(f"Error writing to MySQL table {table}: {str(e)}")
            raise
    
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