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
            # Analytics Database
            "MYSQL_ANALYTICS_HOST", "MYSQL_ANALYTICS_PORT", "MYSQL_ANALYTICS_DATABASE",
            "MYSQL_ANALYTICS_USER", "MYSQL_ANALYTICS_PASSWORD",
            # ML Database
            "MYSQL_ML_HOST", "MYSQL_ML_PORT", "MYSQL_ML_DATABASE",
            "MYSQL_ML_USER", "MYSQL_ML_PASSWORD",
            # Spark Configuration
            "SPARK_DRIVER_MEMORY", "SPARK_EXECUTOR_MEMORY", "SPARK_EXECUTOR_CORES",
            # Core Paths
            "INPUT_PATH", "BRONZE_OUTPUT_PATH", "SILVER_OUTPUT_PATH",
            # Datamart Paths
            "DATAMART_OUTPUT_PATH", "WEATHER_ANALYSIS_PATH",
            "ACCIDENT_FREQUENCY_PATH", "SEVERITY_CORRELATION_PATH",
            # ML Paths
            "ML_OUTPUT_PATH", "ML_MODELS_PATH", "ML_PREDICTIONS_PATH", "ML_FEATURES_PATH"
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    @property
    def mysql_analytics_connection_string(self) -> str:
        """Get MySQL Analytics database connection string for weather impact analysis."""
        host = os.getenv("MYSQL_ANALYTICS_HOST")
        port = os.getenv("MYSQL_ANALYTICS_PORT")
        database = os.getenv("MYSQL_ANALYTICS_DATABASE")
        user = os.getenv("MYSQL_ANALYTICS_USER")
        password = os.getenv("MYSQL_ANALYTICS_PASSWORD")
        
        return f"jdbc:mysql://{host}:{port}/{database}?user={user}&password={password}"
    
    @property
    def mysql_ml_connection_string(self) -> str:
        """Get MySQL ML database connection string for machine learning models."""
        host = os.getenv("MYSQL_ML_HOST")
        port = os.getenv("MYSQL_ML_PORT")
        database = os.getenv("MYSQL_ML_DATABASE")
        user = os.getenv("MYSQL_ML_USER")
        password = os.getenv("MYSQL_ML_PASSWORD")
        
        return f"jdbc:mysql://{host}:{port}/{database}?user={user}&password={password}"
    
    @property
    def mysql_connection_string(self) -> str:
        """Get MySQL connection string (backward compatibility - defaults to analytics)."""
        return self.mysql_analytics_connection_string
    
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
    
    # Datamart Paths (Weather Impact Analysis)
    @property
    def datamart_output_path(self) -> str:
        """Get datamart output path for weather impact analysis."""
        return os.getenv("DATAMART_OUTPUT_PATH")
    
    @property
    def weather_analysis_path(self) -> str:
        """Get weather analysis output path."""
        return os.getenv("WEATHER_ANALYSIS_PATH")
    
    @property
    def accident_frequency_path(self) -> str:
        """Get accident frequency analysis output path."""
        return os.getenv("ACCIDENT_FREQUENCY_PATH")
    
    @property
    def severity_correlation_path(self) -> str:
        """Get severity correlation analysis output path."""
        return os.getenv("SEVERITY_CORRELATION_PATH")
    
    # ML Paths (Machine Learning Models)
    @property
    def ml_output_path(self) -> str:
        """Get ML output path for machine learning components."""
        return os.getenv("ML_OUTPUT_PATH")
    
    @property
    def ml_models_path(self) -> str:
        """Get ML models storage path."""
        return os.getenv("ML_MODELS_PATH")
    
    @property
    def ml_predictions_path(self) -> str:
        """Get ML predictions output path."""
        return os.getenv("ML_PREDICTIONS_PATH")
    
    @property
    def ml_features_path(self) -> str:
        """Get ML features output path."""
        return os.getenv("ML_FEATURES_PATH")
    
    # Streamlit Configuration
    @property
    def streamlit_host(self) -> str:
        """Get Streamlit host."""
        return os.getenv("STREAMLIT_HOST", "localhost")
    
    @property
    def streamlit_port(self) -> int:
        """Get Streamlit port."""
        return int(os.getenv("STREAMLIT_PORT", "8501"))
    
    @property
    def streamlit_title(self) -> str:
        """Get Streamlit application title."""
        return os.getenv("STREAMLIT_TITLE", "Accidents Weather Impact Dashboard")
    
    @property
    def streamlit_theme(self) -> str:
        """Get Streamlit theme."""
        return os.getenv("STREAMLIT_THEME", "light")
    
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