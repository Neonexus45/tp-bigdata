import mysql.connector
import json
import pickle
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from src.core.config import Config
from src.core.logger import get_logger
from src.ml.ml_schema import MLSchema

class MLModelWriter:
    
    def __init__(self):
        self.config = Config()
        self.logger = get_logger(__name__)
        self._connection = None
        
    def _parse_connection_string(self, connection_string: str) -> dict:
        try:
            if connection_string.startswith('jdbc:mysql://'):
                parts = connection_string.replace('jdbc:mysql://', '').split('/')
                host_port = parts[0]
                db_and_params = '/'.join(parts[1:])
                
                host = host_port.split(':')[0]
                port = int(host_port.split(':')[1]) if ':' in host_port else 3306
                
                db_part = db_and_params.split('?')[0]
                params_part = db_and_params.split('?')[1] if '?' in db_and_params else ''
                
                params = {}
                if params_part:
                    for param in params_part.split('&'):
                        key, value = param.split('=')
                        params[key] = value
                
                return {
                    'host': host,
                    'port': port,
                    'database': db_part,
                    'user': params.get('user', 'root'),
                    'password': params.get('password', '')
                }
            
        except Exception as e:
            self.logger.error(f"Error parsing connection string: {e}")
            
        return {
            'host': 'localhost',
            'port': 3306,
            'database': 'accidents_ml_db',
            'user': 'root',
            'password': ''
        }
        
    def _create_database_if_not_exists(self, connection_params: dict):
        database_name = connection_params['database']
        temp_params = connection_params.copy()
        del temp_params['database']
        
        try:
            self.logger.info(f"Creating database '{database_name}' if not exists...")
            temp_connection = mysql.connector.connect(**temp_params)
            cursor = temp_connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
            temp_connection.commit()
            self.logger.info(f"Database '{database_name}' created or already exists")
            cursor.close()
            temp_connection.close()
        except Exception as e:
            self.logger.error(f"Error creating database '{database_name}': {e}")
            raise
        
    def _get_connection(self):
        if self._connection is None or not self._connection.is_connected():
            try:
                connection_params = self._parse_connection_string(
                    self.config.mysql_ml_connection_string
                )
                
                self.logger.info(f"Connecting to ML MySQL: {connection_params['host']}:{connection_params['port']}")
                
                connection_params['connection_timeout'] = 10
                connection_params['autocommit'] = True
                
                # Skip database creation since it's already created manually
                self.logger.info(f"Connecting directly to existing database: {connection_params['database']}")
                self._connection = mysql.connector.connect(**connection_params)
                
                self.logger.info(f"✓ Connected to ML MySQL database: {connection_params['database']}")
                
            except Exception as e:
                self.logger.error(f"Failed to connect to ML database: {e}")
                raise
                
        return self._connection
    
    def create_tables(self):
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            for ddl in MLSchema.get_all_table_ddls():
                cursor.execute(ddl)
                self.logger.info("ML table created successfully")
            connection.commit()
            self.logger.info("All ML tables created successfully")
        except Exception as e:
            self.logger.error(f"Error creating ML tables: {e}")
            raise
        finally:
            cursor.close()
    
    def save_model(self, model, model_metadata: Dict[str, Any]) -> int:
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            model_path = os.path.join(
                self.config.ml_models_path,
                f"{model_metadata['model_name']}_{model_metadata['model_version']}.pkl"
            )
            
            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            
            insert_query = """
            INSERT INTO ml_models 
            (model_name, model_version, model_type, algorithm, training_date,
             feature_count, training_samples, validation_samples, mae_score,
             mse_score, rmse_score, r2_score, model_path, feature_importance,
             hyperparameters)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                model_metadata['model_name'],
                model_metadata['model_version'],
                model_metadata['model_type'],
                model_metadata['algorithm'],
                model_metadata['training_date'],
                model_metadata['feature_count'],
                model_metadata['training_samples'],
                model_metadata['validation_samples'],
                model_metadata.get('mae_score'),
                model_metadata.get('mse_score'),
                model_metadata.get('rmse_score'),
                model_metadata.get('r2_score'),
                model_path,
                json.dumps(model_metadata.get('feature_importance', {})),
                json.dumps(model_metadata.get('hyperparameters', {}))
            )
            
            cursor.execute(insert_query, values)
            model_id = cursor.lastrowid
            connection.commit()
            
            self.logger.info(f"Model saved with ID: {model_id}")
            return model_id
            
        except Exception as e:
            self.logger.error(f"Error saving model: {e}")
            raise
        finally:
            cursor.close()
    
    def save_features(self, model_id: int, features_data: List[Dict[str, Any]]):
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            insert_query = """
            INSERT INTO ml_features 
            (model_id, feature_name, feature_type, importance_score, feature_category,
             description, min_value, max_value, mean_value, std_value, null_percentage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = [
                (
                    model_id,
                    feature['feature_name'],
                    feature['feature_type'],
                    feature.get('importance_score'),
                    feature.get('feature_category'),
                    feature.get('description'),
                    feature.get('min_value'),
                    feature.get('max_value'),
                    feature.get('mean_value'),
                    feature.get('std_value'),
                    feature.get('null_percentage')
                )
                for feature in features_data
            ]
            
            cursor.executemany(insert_query, values)
            connection.commit()
            self.logger.info(f"Saved {len(values)} feature records for model {model_id}")
            
        except Exception as e:
            self.logger.error(f"Error saving features: {e}")
            raise
        finally:
            cursor.close()
    
    def save_predictions(self, predictions_data: List[Dict[str, Any]]):
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            insert_query = """
            INSERT INTO ml_predictions 
            (model_id, prediction_batch, accident_id, predicted_severity, confidence_score,
             prediction_date, input_features, actual_severity, prediction_error)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = [
                (
                    pred['model_id'],
                    pred['prediction_batch'],
                    pred.get('accident_id'),
                    pred['predicted_severity'],
                    pred.get('confidence_score'),
                    pred['prediction_date'],
                    json.dumps(pred.get('input_features', {})),
                    pred.get('actual_severity'),
                    pred.get('prediction_error')
                )
                for pred in predictions_data
            ]
            
            cursor.executemany(insert_query, values)
            connection.commit()
            self.logger.info(f"Saved {len(values)} prediction records")
            
        except Exception as e:
            self.logger.error(f"Error saving predictions: {e}")
            raise
        finally:
            cursor.close()
    
    def load_model(self, model_name: str, model_version: str = None):
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            if model_version:
                query = "SELECT model_path FROM ml_models WHERE model_name = %s AND model_version = %s AND is_active = TRUE"
                cursor.execute(query, (model_name, model_version))
            else:
                query = """
                SELECT model_path FROM ml_models 
                WHERE model_name = %s AND is_active = TRUE 
                ORDER BY training_date DESC LIMIT 1
                """
                cursor.execute(query, (model_name,))
            
            result = cursor.fetchone()
            if result:
                model_path = result[0]
                with open(model_path, 'rb') as f:
                    model = pickle.load(f)
                self.logger.info(f"Model loaded from {model_path}")
                return model
            else:
                self.logger.warning(f"Model not found: {model_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            raise
        finally:
            cursor.close()
    
    def get_model_metadata(self, model_name: str, model_version: str = None) -> Optional[Dict[str, Any]]:
        connection = self._get_connection()
        cursor = connection.cursor(dictionary=True)
        
        try:
            if model_version:
                query = "SELECT * FROM ml_models WHERE model_name = %s AND model_version = %s AND is_active = TRUE"
                cursor.execute(query, (model_name, model_version))
            else:
                query = """
                SELECT * FROM ml_models 
                WHERE model_name = %s AND is_active = TRUE 
                ORDER BY training_date DESC LIMIT 1
                """
                cursor.execute(query, (model_name,))
            
            result = cursor.fetchone()
            if result:
                if result['feature_importance']:
                    result['feature_importance'] = json.loads(result['feature_importance'])
                if result['hyperparameters']:
                    result['hyperparameters'] = json.loads(result['hyperparameters'])
            
            return result
                
        except Exception as e:
            self.logger.error(f"Error getting model metadata: {e}")
            raise
        finally:
            cursor.close()
    
    def close_connection(self):
        if self._connection and self._connection.is_connected():
            self._connection.close()
            self.logger.info("ML database connection closed")