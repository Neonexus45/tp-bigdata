"""Configuration management for US Accidents Lakehouse project."""

import os
from typing import Dict, Any
from dotenv import load_dotenv


class Config:
    """Configuration class that loads and validates environment variables."""
    
    def __init__(self, env_file: str = ".env") -> None:
        """Initialize configuration by loading environment variables.
        
        Args:
            env_file: Path to the environment file
        """
        load_dotenv(env_file)
        self._validate_required_vars()
    
    def _validate_required_vars(self) -> None:
        """Validate that all required environment variables are set."""
        required_vars = [
            "MYSQL_HOST", "MYSQL_PORT", "MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD",
            "SPARK_DRIVER_MEMORY", "SPARK_EXECUTOR_MEMORY", "SPARK_EXECUTOR_CORES",
            "INPUT_PATH", "BRONZE_OUTPUT_PATH", "SILVER_OUTPUT_PATH"
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    @property
    def mysql_connection_string(self) -> str:
        """Get MySQL connection string."""
        host = os.getenv("MYSQL_HOST")
        port = os.getenv("MYSQL_PORT")
        database = os.getenv("MYSQL_DATABASE")
        user = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASSWORD")
        
        return f"jdbc:mysql://{host}:{port}/{database}?user={user}&password={password}"
    
    @property
    def spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration dictionary."""
        return {
            "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
            "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.ui.enabled": os.getenv("SPARK_UI_ENABLED", "true").lower() == "true",
            "spark.ui.port": int(os.getenv("SPARK_UI_PORT", "4040"))
        }
    
    @property
    def input_path(self) -> str:
        """Get input data path."""
        return os.getenv("INPUT_PATH")
    
    @property
    def bronze_output_path(self) -> str:
        """Get bronze layer output path."""
        return os.getenv("BRONZE_OUTPUT_PATH")
    
    @property
    def silver_output_path(self) -> str:
        """Get silver layer output path."""
        return os.getenv("SILVER_OUTPUT_PATH")
    
    @property
    def log_level(self) -> str:
        """Get logging level."""
        return os.getenv("LOG_LEVEL", "INFO").upper()
    
    @property
    def batch_size(self) -> int:
        """Get batch processing size."""
        return int(os.getenv("BATCH_SIZE", "1000"))
    
    @property
    def output_format(self) -> str:
        """Get output file format."""
        return os.getenv("OUTPUT_FORMAT", "parquet").lower()