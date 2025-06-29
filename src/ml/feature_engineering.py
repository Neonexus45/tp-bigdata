import numpy as np
import pandas as pd
from typing import Dict, List, Any, Tuple
from datetime import datetime
from sklearn.preprocessing import StandardScaler, LabelEncoder
from src.core.logger import get_logger

class GeographicFeatureExtractor:
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.state_encoders = {}
        self.city_encoders = {}
        
    def extract_features(self, df: pd.DataFrame) -> pd.DataFrame:
        features_df = df.copy()
        
        features_df['lat_rounded'] = np.round(features_df['Latitude'], 2)
        features_df['lng_rounded'] = np.round(features_df['Longitude'], 2)
        
        features_df['lat_lng_interaction'] = features_df['Latitude'] * features_df['Longitude']
        
        if 'State' in features_df.columns:
            if 'State' not in self.state_encoders:
                self.state_encoders['State'] = LabelEncoder()
                features_df['state_encoded'] = self.state_encoders['State'].fit_transform(features_df['State'].fillna('Unknown'))
            else:
                features_df['state_encoded'] = self.state_encoders['State'].transform(features_df['State'].fillna('Unknown'))
        
        if 'City' in features_df.columns:
            city_counts = features_df['City'].value_counts()
            features_df['city_frequency'] = features_df['City'].map(city_counts).fillna(0)
            
            top_cities = city_counts.head(50).index
            features_df['is_major_city'] = features_df['City'].isin(top_cities).astype(int)
        
        features_df['coord_cluster'] = self._create_coordinate_clusters(
            features_df['Latitude'], features_df['Longitude']
        )
        
        self.logger.info("Geographic features extracted successfully")
        return features_df
    
    def _calculate_distance(self, lat1, lng1, lat2, lng2):
        lat1, lng1, lat2, lng2 = map(np.radians, [lat1, lng1, lat2, lng2])
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        return 6371 * c
    
    def _create_coordinate_clusters(self, lat, lng, n_clusters=20):
        from sklearn.cluster import KMeans
        coords = np.column_stack([lat, lng])
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        return kmeans.fit_predict(coords)

class TemporalFeatureExtractor:
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
    def extract_features(self, df: pd.DataFrame) -> pd.DataFrame:
        features_df = df.copy()
        
        if 'Start_Time' in features_df.columns:
            features_df['Start_Time'] = pd.to_datetime(features_df['Start_Time'])
            
            features_df['hour'] = features_df['Start_Time'].dt.hour
            features_df['day_of_week'] = features_df['Start_Time'].dt.dayofweek
            features_df['month'] = features_df['Start_Time'].dt.month
            features_df['year'] = features_df['Start_Time'].dt.year
            features_df['day_of_year'] = features_df['Start_Time'].dt.dayofyear
            
            features_df['is_weekend'] = (features_df['day_of_week'] >= 5).astype(int)
            features_df['is_rush_hour'] = ((features_df['hour'].between(7, 9)) | 
                                         (features_df['hour'].between(17, 19))).astype(int)
            features_df['is_night'] = ((features_df['hour'] >= 22) | 
                                     (features_df['hour'] <= 6)).astype(int)
            
            features_df['hour_sin'] = np.sin(2 * np.pi * features_df['hour'] / 24)
            features_df['hour_cos'] = np.cos(2 * np.pi * features_df['hour'] / 24)
            features_df['month_sin'] = np.sin(2 * np.pi * features_df['month'] / 12)
            features_df['month_cos'] = np.cos(2 * np.pi * features_df['month'] / 12)
            features_df['day_sin'] = np.sin(2 * np.pi * features_df['day_of_week'] / 7)
            features_df['day_cos'] = np.cos(2 * np.pi * features_df['day_of_week'] / 7)
            
            features_df['season'] = self._get_season(features_df['month'])
        
        if 'Hour_of_Day' in features_df.columns:
            features_df['hour'] = features_df['Hour_of_Day']
            features_df['is_rush_hour'] = ((features_df['hour'].between(7, 9)) | 
                                         (features_df['hour'].between(17, 19))).astype(int)
            features_df['is_night'] = ((features_df['hour'] >= 22) | 
                                     (features_df['hour'] <= 6)).astype(int)
            
            features_df['hour_sin'] = np.sin(2 * np.pi * features_df['hour'] / 24)
            features_df['hour_cos'] = np.cos(2 * np.pi * features_df['hour'] / 24)
        
        if 'Day_of_Week' in features_df.columns:
            features_df['day_of_week'] = features_df['Day_of_Week']
            features_df['is_weekend'] = (features_df['day_of_week'].isin([1, 7])).astype(int)
            features_df['day_sin'] = np.sin(2 * np.pi * features_df['day_of_week'] / 7)
            features_df['day_cos'] = np.cos(2 * np.pi * features_df['day_of_week'] / 7)
        
        if 'Month' in features_df.columns:
            features_df['month'] = features_df['Month']
            features_df['month_sin'] = np.sin(2 * np.pi * features_df['month'] / 12)
            features_df['month_cos'] = np.cos(2 * np.pi * features_df['month'] / 12)
            features_df['season'] = self._get_season(features_df['month'])
        
        if 'Duration_Hours' in features_df.columns:
            features_df['duration_hours'] = features_df['Duration_Hours'].clip(0, 24)
        
        if 'Is_Weekend' in features_df.columns:
            features_df['is_weekend'] = features_df['Is_Weekend'].astype(int)
        
        if 'Time_Period' in features_df.columns:
            time_period_encoder = LabelEncoder()
            features_df['time_period_encoded'] = time_period_encoder.fit_transform(
                features_df['Time_Period'].fillna('Unknown')
            )
        
        self.logger.info("Temporal features extracted successfully")
        return features_df
    
    def _get_season(self, month):
        conditions = [
            month.isin([12, 1, 2]),
            month.isin([3, 4, 5]),
            month.isin([6, 7, 8]),
            month.isin([9, 10, 11])
        ]
        choices = [0, 1, 2, 3]  # Winter, Spring, Summer, Fall
        return np.select(conditions, choices, default=0)

class EnvironmentalFeatureExtractor:
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.encoders = {}
        
    def extract_features(self, df: pd.DataFrame) -> pd.DataFrame:
        features_df = df.copy()
        
        boolean_cols = ['Bump', 'Crossing', 'Give_Way', 'Junction', 
                       'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 
                       'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
        
        for col in boolean_cols:
            if col in features_df.columns:
                features_df[f'{col.lower()}_flag'] = features_df[col].astype(int)
        
        road_feature_cols = [col for col in boolean_cols if col in features_df.columns]
        if road_feature_cols:
            features_df['road_features_count'] = features_df[road_feature_cols].sum(axis=1)
        
        if 'Road_Complexity_Score' in features_df.columns:
            features_df['road_complexity_score'] = features_df['Road_Complexity_Score']
        
        if 'Road_Complexity_Category' in features_df.columns:
            if 'road_complexity_cat' not in self.encoders:
                self.encoders['road_complexity_cat'] = LabelEncoder()
                features_df['road_complexity_encoded'] = self.encoders['road_complexity_cat'].fit_transform(
                    features_df['Road_Complexity_Category'].fillna('Simple')
                )
            else:
                features_df['road_complexity_encoded'] = self.encoders['road_complexity_cat'].transform(
                    features_df['Road_Complexity_Category'].fillna('Simple')
                )
        
        if 'Severity_Category' in features_df.columns:
            if 'severity_cat' not in self.encoders:
                self.encoders['severity_cat'] = LabelEncoder()
                features_df['severity_category_encoded'] = self.encoders['severity_cat'].fit_transform(
                    features_df['Severity_Category'].fillna('Unknown')
                )
            else:
                features_df['severity_category_encoded'] = self.encoders['severity_cat'].transform(
                    features_df['Severity_Category'].fillna('Unknown')
                )
        
        if 'Source' in features_df.columns:
            if 'source' not in self.encoders:
                self.encoders['source'] = LabelEncoder()
                features_df['source_encoded'] = self.encoders['source'].fit_transform(
                    features_df['Source'].fillna('Unknown')
                )
            else:
                features_df['source_encoded'] = self.encoders['source'].transform(
                    features_df['Source'].fillna('Unknown')
                )
        
        self.logger.info("Environmental features extracted successfully")
        return features_df

class FeaturePipeline:
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.geographic_extractor = GeographicFeatureExtractor()
        self.temporal_extractor = TemporalFeatureExtractor()
        self.environmental_extractor = EnvironmentalFeatureExtractor()
        self.scaler = StandardScaler()
        self.feature_columns = None
        
    def fit_transform(self, df: pd.DataFrame, target_column: str = 'Severity') -> Tuple[pd.DataFrame, pd.Series]:
        self.logger.info("Starting feature pipeline fit_transform")
        
        features_df = df.copy()
        
        features_df = self.geographic_extractor.extract_features(features_df)
        features_df = self.temporal_extractor.extract_features(features_df)
        features_df = self.environmental_extractor.extract_features(features_df)
        
        target = features_df[target_column] if target_column in features_df.columns else None
        
        numeric_features = self._select_numeric_features(features_df)
        
        numeric_features_filled = numeric_features.fillna(numeric_features.median())
        
        scaled_features = self.scaler.fit_transform(numeric_features_filled)
        scaled_df = pd.DataFrame(scaled_features, columns=numeric_features.columns, index=features_df.index)
        
        self.feature_columns = scaled_df.columns.tolist()
        
        self.logger.info(f"Feature pipeline completed. Features shape: {scaled_df.shape}")
        return scaled_df, target
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Starting feature pipeline transform")
        
        features_df = df.copy()
        
        features_df = self.geographic_extractor.extract_features(features_df)
        features_df = self.temporal_extractor.extract_features(features_df)
        features_df = self.environmental_extractor.extract_features(features_df)
        
        numeric_features = self._select_numeric_features(features_df)
        
        if self.feature_columns:
            missing_cols = set(self.feature_columns) - set(numeric_features.columns)
            for col in missing_cols:
                numeric_features[col] = 0
            numeric_features = numeric_features[self.feature_columns]
        
        numeric_features_filled = numeric_features.fillna(numeric_features.median())
        
        scaled_features = self.scaler.transform(numeric_features_filled)
        scaled_df = pd.DataFrame(scaled_features, columns=numeric_features.columns, index=features_df.index)
        
        self.logger.info(f"Feature transform completed. Features shape: {scaled_df.shape}")
        return scaled_df
    
    def _select_numeric_features(self, df: pd.DataFrame) -> pd.DataFrame:
        exclude_cols = ['ID', 'Source', 'Start_Time', 'End_Time', 
                       'State', 'City', 'County', 'Severity',
                       'Severity_Category', 'Time_Period', 'Road_Complexity_Category',
                       'ingestion_date', 'Year']
        
        numeric_cols = []
        for col in df.columns:
            if col not in exclude_cols:
                if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                    numeric_cols.append(col)
                elif df[col].dtype == 'bool':
                    numeric_cols.append(col)
        
        return df[numeric_cols]
    
    def get_feature_importance_data(self, feature_importance: Dict[str, float]) -> List[Dict[str, Any]]:
        feature_data = []
        
        for i, (feature_name, importance) in enumerate(feature_importance.items()):
            feature_type = self._categorize_feature(feature_name)
            feature_category = self._get_feature_category(feature_name)
            
            feature_data.append({
                'feature_name': feature_name,
                'feature_type': feature_type,
                'importance_score': float(importance),
                'feature_category': feature_category,
                'description': self._get_feature_description(feature_name)
            })
        
        return feature_data
    
    def _categorize_feature(self, feature_name: str) -> str:
        if any(x in feature_name.lower() for x in ['lat', 'lng', 'coord', 'state', 'city']):
            return 'geographic'
        elif any(x in feature_name.lower() for x in ['hour', 'day', 'month', 'time', 'season', 'weekend']):
            return 'temporal'
        elif any(x in feature_name.lower() for x in ['bump', 'junction', 'signal', 'road', 'traffic']):
            return 'infrastructure'
        elif any(x in feature_name.lower() for x in ['source', 'severity', 'duration']):
            return 'accident_context'
        else:
            return 'other'
    
    def _get_feature_category(self, feature_name: str) -> str:
        categories = {
            'geographic': ['lat', 'lng', 'coord', 'state', 'city'],
            'temporal': ['hour', 'day', 'month', 'time', 'season', 'weekend'],
            'infrastructure': ['bump', 'junction', 'signal', 'road', 'traffic'],
            'accident': ['source', 'severity', 'duration']
        }
        
        for category, keywords in categories.items():
            if any(keyword in feature_name.lower() for keyword in keywords):
                return category
        
        return 'other'
    
    def _get_feature_description(self, feature_name: str) -> str:
        descriptions = {
            'lat_rounded': 'Rounded latitude coordinate',
            'lng_rounded': 'Rounded longitude coordinate',
            'hour': 'Hour of the day when accident occurred',
            'is_weekend': 'Whether accident occurred on weekend',
            'is_rush_hour': 'Whether accident occurred during rush hour',
            'road_features_count': 'Count of road infrastructure features present',
            'road_complexity_score': 'Complexity score based on road features'
        }
        
        return descriptions.get(feature_name, f'Feature: {feature_name}')