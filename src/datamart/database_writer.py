import mysql.connector
from typing import List, Dict, Any
from datetime import datetime
from src.core.config import Config
from src.core.logger import get_logger
from src.datamart.schema import DatamartSchema

class AnalyticsWriter:
    
    def __init__(self):
        self.config = Config()
        self.logger = get_logger(__name__)
        self._connection = None
        
    def _parse_connection_string(self, connection_string: str) -> dict:
        """Parse la chaîne de connexion MySQL pour extraire les paramètres."""
        try:
            # Format: mysql://user:password@host:port/database
            # ou jdbc:mysql://host:port/database?user=user&password=password
            
            if connection_string.startswith('jdbc:mysql://'):
                # Format JDBC
                parts = connection_string.replace('jdbc:mysql://', '').split('/')
                host_port = parts[0]
                db_and_params = '/'.join(parts[1:])
                
                host = host_port.split(':')[0]
                port = int(host_port.split(':')[1]) if ':' in host_port else 3306
                
                db_part = db_and_params.split('?')[0]
                params_part = db_and_params.split('?')[1] if '?' in db_and_params else ''
                
                # Parser les paramètres
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
            
            elif connection_string.startswith('mysql://'):
                # Format MySQL standard
                # mysql://user:password@host:port/database
                without_prefix = connection_string.replace('mysql://', '')
                
                if '@' in without_prefix:
                    user_pass, host_db = without_prefix.split('@')
                    user = user_pass.split(':')[0]
                    password = user_pass.split(':')[1] if ':' in user_pass else ''
                else:
                    host_db = without_prefix
                    user = 'root'
                    password = ''
                
                host_port = host_db.split('/')[0]
                database = host_db.split('/')[1] if '/' in host_db else ''
                
                host = host_port.split(':')[0]
                port = int(host_port.split(':')[1]) if ':' in host_port else 3306
                
                return {
                    'host': host,
                    'port': port,
                    'database': database,
                    'user': user,
                    'password': password
                }
            
            else:
                # Fallback - essayer de parser comme avant
                parts = connection_string.split('//')
                if len(parts) > 1:
                    connection_part = parts[1]
                    host = connection_part.split(':')[0]
                    return {
                        'host': host,
                        'port': 3306,
                        'database': 'accidents_analytics_db',
                        'user': 'root',
                        'password': ''
                    }
                
        except Exception as e:
            self.logger.error(f"Error parsing connection string: {e}")
            
        # Valeurs par défaut si le parsing échoue
        return {
            'host': 'localhost',
            'port': 3306,
            'database': 'accidents_analytics_db',
            'user': 'root',
            'password': ''
        }
        
    def _create_database_if_not_exists(self, connection_params: dict):
        """Crée la base de données si elle n'existe pas."""
        database_name = connection_params['database']
        
        # Connexion sans spécifier la base de données
        temp_params = connection_params.copy()
        del temp_params['database']
        
        try:
            temp_connection = mysql.connector.connect(**temp_params)
            cursor = temp_connection.cursor()
            
            # Créer la base de données
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
            self.logger.info(f"Database '{database_name}' created or already exists")
            
            cursor.close()
            temp_connection.close()
            
        except Exception as e:
            self.logger.error(f"Error creating database '{database_name}': {e}")
            raise
        
    def _get_connection(self):
        """Obtient une connexion à la base de données."""
        if self._connection is None or not self._connection.is_connected():
            try:
                connection_params = self._parse_connection_string(
                    self.config.mysql_analytics_connection_string
                )
                
                self.logger.info(f"Connecting to MySQL: {connection_params['host']}:{connection_params['port']}")
                
                # Créer la base de données si nécessaire
                self._create_database_if_not_exists(connection_params)
                
                # Connexion avec la base de données
                connection_params['autocommit'] = True
                self._connection = mysql.connector.connect(**connection_params)
                
                self.logger.info(f"Connected to MySQL database: {connection_params['database']}")
                
            except Exception as e:
                self.logger.error(f"Failed to connect to database: {e}")
                raise
                
        return self._connection
    
    def create_tables(self):
        """Crée toutes les tables nécessaires."""
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            for ddl in DatamartSchema.get_all_table_ddls():
                cursor.execute(ddl)
                self.logger.info("Table created successfully")
            connection.commit()
            self.logger.info("All tables created successfully")
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            raise
        finally:
            cursor.close()
    
    def clear_table(self, table_name: str, analysis_date: str):
        """Supprime les enregistrements existants pour la date d'analyse."""
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            delete_query = f"DELETE FROM {table_name} WHERE analysis_date = %s"
            cursor.execute(delete_query, (analysis_date,))
            deleted_rows = cursor.rowcount
            self.logger.info(f"Cleared {deleted_rows} existing records from {table_name}")
            connection.commit()
        except Exception as e:
            self.logger.error(f"Error clearing table {table_name}: {e}")
            raise
        finally:
            cursor.close()
    
    def write_weather_frequency_data(self, data: List[Dict[str, Any]], analysis_date: str):
        """Écrit les données de fréquence météorologique."""
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            self.clear_table("weather_accident_frequency", analysis_date)
            
            insert_query = """
            INSERT INTO weather_accident_frequency 
            (weather_category, temperature_category, accident_count, total_accidents, 
             frequency_percentage, analysis_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            values = [
                (
                    row['weather_category'],
                    row['temperature_category'], 
                    row['accident_count'],
                    row['total_accidents'],
                    row['frequency_percentage'],
                    analysis_date
                )
                for row in data
            ]
            
            cursor.executemany(insert_query, values)
            connection.commit()
            self.logger.info(f"Inserted {len(values)} weather frequency records")
            
        except Exception as e:
            self.logger.error(f"Error writing weather frequency data: {e}")
            raise
        finally:
            cursor.close()
    
    def write_severity_correlation_data(self, data: List[Dict[str, Any]], analysis_date: str):
        """Écrit les données de corrélation météo-sévérité."""
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            self.clear_table("weather_severity_correlation", analysis_date)
            
            insert_query = """
            INSERT INTO weather_severity_correlation 
            (weather_category, severity_level, accident_count, avg_duration_hours, 
             correlation_score, analysis_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            values = [
                (
                    row['weather_category'],
                    row['severity_level'],
                    row['accident_count'],
                    row['avg_duration_hours'],
                    row['correlation_score'],
                    analysis_date
                )
                for row in data
            ]
            
            cursor.executemany(insert_query, values)
            connection.commit()
            self.logger.info(f"Inserted {len(values)} severity correlation records")
            
        except Exception as e:
            self.logger.error(f"Error writing severity correlation data: {e}")
            raise
        finally:
            cursor.close()
    
    def write_weather_trends_data(self, data: List[Dict[str, Any]], analysis_date: str):
        """Écrit les données de tendances météorologiques."""
        connection = self._get_connection()
        cursor = connection.cursor()
        
        try:
            self.clear_table("weather_impact_trends", analysis_date)
            
            insert_query = """
            INSERT INTO weather_impact_trends 
            (period_month, weather_category, total_accidents, avg_severity, 
             trend_direction, seasonal_factor, analysis_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            values = [
                (
                    row['year_month'],  # Le code utilise toujours 'year_month' comme clé
                    row['weather_category'],
                    row['total_accidents'],
                    row['avg_severity'],
                    row['trend_direction'],
                    row['seasonal_factor'],
                    analysis_date
                )
                for row in data
            ]
            
            cursor.executemany(insert_query, values)
            connection.commit()
            self.logger.info(f"Inserted {len(values)} weather trends records")
            
        except Exception as e:
            self.logger.error(f"Error writing weather trends data: {e}")
            raise
        finally:
            cursor.close()
    
    def close_connection(self):
        """Ferme la connexion à la base de données."""
        if self._connection and self._connection.is_connected():
            self._connection.close()
            self.logger.info("Database connection closed")