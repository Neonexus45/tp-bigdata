import pandas as pd
import mysql.connector
from typing import Dict, List, Optional, Tuple
import streamlit as st
from datetime import datetime, timedelta
import sys
import os

# Add the project root to Python path
current_file = os.path.abspath(__file__)
dashboard_dir = os.path.dirname(current_file)
src_dir = os.path.dirname(dashboard_dir)
project_root = os.path.dirname(src_dir)
sys.path.insert(0, project_root)

# Import Config with direct file loading
def load_config():
    config_path = os.path.join(project_root, 'src', 'core', 'config.py')
    
    if os.path.exists(config_path):
        # Add paths to sys.path
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        src_path = os.path.join(project_root, 'src')
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        
        # Import using importlib
        import importlib.util
        spec = importlib.util.spec_from_file_location("config", config_path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        
        return config_module.Config
    else:
        raise ImportError(f"Config file not found at {config_path}")

Config = load_config()

class AnalyticsDataLoader:
    def __init__(self):
        self.config = Config()
        self.connection_params = {
            'host': os.getenv("MYSQL_ANALYTICS_HOST"),
            'port': int(os.getenv("MYSQL_ANALYTICS_PORT")),
            'user': os.getenv("MYSQL_ANALYTICS_USER"),
            'password': os.getenv("MYSQL_ANALYTICS_PASSWORD"),
            'database': os.getenv("MYSQL_ANALYTICS_DATABASE")
        }
    
    @st.cache_data(ttl=300)
    def get_weather_impact_summary(_self) -> pd.DataFrame:
        query = """
        SELECT 
            weather_condition,
            COUNT(*) as incident_count,
            AVG(severity_score) as avg_severity,
            AVG(temperature) as avg_temperature,
            AVG(humidity) as avg_humidity,
            AVG(wind_speed) as avg_wind_speed
        FROM weather_impact_analysis 
        GROUP BY weather_condition
        ORDER BY incident_count DESC
        """
        return _self._execute_query(query)
    
    @st.cache_data(ttl=300)
    def get_temporal_analysis(_self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        base_query = """
        SELECT 
            DATE(analysis_date) as date,
            weather_condition,
            COUNT(*) as incident_count,
            AVG(severity_score) as avg_severity
        FROM weather_impact_analysis
        """
        
        conditions = []
        if start_date:
            conditions.append(f"analysis_date >= '{start_date}'")
        if end_date:
            conditions.append(f"analysis_date <= '{end_date}'")
        
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        
        base_query += " GROUP BY DATE(analysis_date), weather_condition ORDER BY date DESC"
        
        return _self._execute_query(base_query)
    
    @st.cache_data(ttl=300)
    def get_severity_distribution(_self) -> pd.DataFrame:
        query = """
        SELECT 
            CASE 
                WHEN severity_score < 3 THEN 'Low'
                WHEN severity_score < 7 THEN 'Medium'
                ELSE 'High'
            END as severity_category,
            weather_condition,
            COUNT(*) as count
        FROM weather_impact_analysis
        GROUP BY severity_category, weather_condition
        ORDER BY severity_category, count DESC
        """
        return _self._execute_query(query)
    
    @st.cache_data(ttl=300)
    def get_correlation_data(_self) -> pd.DataFrame:
        query = """
        SELECT 
            temperature,
            humidity,
            wind_speed,
            pressure,
            severity_score,
            weather_condition
        FROM weather_impact_analysis
        WHERE temperature IS NOT NULL 
        AND humidity IS NOT NULL 
        AND wind_speed IS NOT NULL
        """
        return _self._execute_query(query)
    
    def _execute_query(self, query: str) -> pd.DataFrame:
        try:
            connection = mysql.connector.connect(**self.connection_params)
            df = pd.read_sql(query, connection)
            connection.close()
            return df
        except Exception as e:
            st.error(f"Database connection error: {str(e)}")
            return pd.DataFrame()

class MLDataLoader:
    def __init__(self):
        self.config = Config()
        self.connection_params = {
            'host': os.getenv("MYSQL_ML_HOST"),
            'port': int(os.getenv("MYSQL_ML_PORT")),
            'user': os.getenv("MYSQL_ML_USER"),
            'password': os.getenv("MYSQL_ML_PASSWORD"),
            'database': os.getenv("MYSQL_ML_DATABASE")
        }
    
    @st.cache_data(ttl=300)
    def get_model_performance(_self) -> pd.DataFrame:
        query = """
        SELECT 
            model_name,
            model_version,
            accuracy,
            precision_score,
            recall,
            f1_score,
            training_date,
            is_active
        FROM model_performance
        ORDER BY training_date DESC
        """
        return _self._execute_query(query)
    
    @st.cache_data(ttl=300)
    def get_predictions_summary(_self, days: int = 30) -> pd.DataFrame:
        query = f"""
        SELECT 
            DATE(prediction_date) as date,
            predicted_severity,
            confidence_score,
            COUNT(*) as prediction_count,
            AVG(confidence_score) as avg_confidence
        FROM severity_predictions
        WHERE prediction_date >= DATE_SUB(NOW(), INTERVAL {days} DAY)
        GROUP BY DATE(prediction_date), predicted_severity
        ORDER BY date DESC
        """
        return _self._execute_query(query)
    
    @st.cache_data(ttl=300)
    def get_feature_importance(_self, model_name: str = None) -> pd.DataFrame:
        base_query = """
        SELECT 
            feature_name,
            importance_score,
            model_name,
            model_version
        FROM feature_importance
        """
        
        if model_name:
            base_query += f" WHERE model_name = '{model_name}'"
        
        base_query += " ORDER BY importance_score DESC"
        
        return _self._execute_query(base_query)
    
    @st.cache_data(ttl=300)
    def get_prediction_accuracy(_self) -> pd.DataFrame:
        query = """
        SELECT 
            DATE(prediction_date) as date,
            predicted_severity,
            actual_severity,
            ABS(predicted_severity - actual_severity) as error,
            confidence_score
        FROM severity_predictions p
        JOIN actual_outcomes a ON p.prediction_id = a.prediction_id
        WHERE actual_severity IS NOT NULL
        ORDER BY prediction_date DESC
        LIMIT 1000
        """
        return _self._execute_query(query)
    
    @st.cache_data(ttl=300)
    def get_model_comparison(_self) -> pd.DataFrame:
        query = """
        SELECT 
            model_name,
            AVG(accuracy) as avg_accuracy,
            AVG(precision_score) as avg_precision,
            AVG(recall) as avg_recall,
            AVG(f1_score) as avg_f1,
            COUNT(*) as version_count,
            MAX(training_date) as latest_training
        FROM model_performance
        GROUP BY model_name
        ORDER BY avg_f1 DESC
        """
        return _self._execute_query(query)
    
    def _execute_query(self, query: str) -> pd.DataFrame:
        try:
            connection = mysql.connector.connect(**self.connection_params)
            df = pd.read_sql(query, connection)
            connection.close()
            return df
        except Exception as e:
            st.error(f"ML Database connection error: {str(e)}")
            return pd.DataFrame()

class DataRefreshUtility:
    @staticmethod
    def clear_cache():
        st.cache_data.clear()
        st.success("Cache cleared successfully!")
    
    @staticmethod
    def get_last_refresh_time() -> str:
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        return st.session_state.last_refresh.strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def update_refresh_time():
        st.session_state.last_refresh = datetime.now()