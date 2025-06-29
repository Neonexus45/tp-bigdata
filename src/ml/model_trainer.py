import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.svm import SVR
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from src.core.logger import get_logger
from src.ml.feature_engineering import FeaturePipeline

class SeverityRegressor:
    
    def __init__(self, model_type: str = 'random_forest'):
        self.logger = get_logger(__name__)
        self.model_type = model_type
        self.model = None
        self.feature_pipeline = FeaturePipeline()
        self.is_fitted = False
        self.feature_importance_ = None
        self.training_metrics = {}
        
        self.model_configs = {
            'random_forest': {
                'model': RandomForestRegressor,
                'params': {
                    'n_estimators': 100,
                    'max_depth': 10,
                    'min_samples_split': 5,
                    'min_samples_leaf': 2,
                    'random_state': 42,
                    'n_jobs': -1
                }
            },
            'gradient_boosting': {
                'model': GradientBoostingRegressor,
                'params': {
                    'n_estimators': 100,
                    'learning_rate': 0.1,
                    'max_depth': 6,
                    'min_samples_split': 5,
                    'random_state': 42
                }
            },
            'linear_regression': {
                'model': LinearRegression,
                'params': {}
            },
            'ridge': {
                'model': Ridge,
                'params': {
                    'alpha': 1.0,
                    'random_state': 42
                }
            },
            'lasso': {
                'model': Lasso,
                'params': {
                    'alpha': 1.0,
                    'random_state': 42,
                    'max_iter': 2000
                }
            },
            'svr': {
                'model': SVR,
                'params': {
                    'kernel': 'rbf',
                    'C': 1.0,
                    'gamma': 'scale'
                }
            }
        }
        
        if model_type not in self.model_configs:
            raise ValueError(f"Unsupported model type: {model_type}")
        
        config = self.model_configs[model_type]
        self.model = config['model'](**config['params'])
        
    def train(self, df: pd.DataFrame, target_column: str = 'Severity', 
              test_size: float = 0.2, validation_split: float = 0.2) -> Dict[str, Any]:
        
        self.logger.info(f"Starting training with {self.model_type} model")
        
        X, y = self.feature_pipeline.fit_transform(df, target_column)
        
        if y is None:
            raise ValueError(f"Target column '{target_column}' not found in dataset")
        
        y = pd.to_numeric(y, errors='coerce')
        valid_indices = ~(X.isnull().any(axis=1) | y.isnull())
        X = X[valid_indices]
        y = y[valid_indices]
        
        self.logger.info(f"Training data shape: {X.shape}, Target shape: {y.shape}")
        
        X_temp, X_test, y_temp, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=None
        )
        
        X_train, X_val, y_train, y_val = train_test_split(
            X_temp, y_temp, test_size=validation_split/(1-test_size), random_state=42
        )
        
        self.model.fit(X_train, y_train)
        self.is_fitted = True
        
        train_pred = self.model.predict(X_train)
        val_pred = self.model.predict(X_val)
        test_pred = self.model.predict(X_test)
        
        metrics = {
            'training_samples': len(X_train),
            'validation_samples': len(X_val),
            'test_samples': len(X_test),
            'train_mae': mean_absolute_error(y_train, train_pred),
            'train_mse': mean_squared_error(y_train, train_pred),
            'train_rmse': np.sqrt(mean_squared_error(y_train, train_pred)),
            'train_r2': r2_score(y_train, train_pred),
            'val_mae': mean_absolute_error(y_val, val_pred),
            'val_mse': mean_squared_error(y_val, val_pred),
            'val_rmse': np.sqrt(mean_squared_error(y_val, val_pred)),
            'val_r2': r2_score(y_val, val_pred),
            'test_mae': mean_absolute_error(y_test, test_pred),
            'test_mse': mean_squared_error(y_test, test_pred),
            'test_rmse': np.sqrt(mean_squared_error(y_test, test_pred)),
            'test_r2': r2_score(y_test, test_pred)
        }
        
        if hasattr(self.model, 'feature_importances_'):
            feature_names = X.columns.tolist()
            self.feature_importance_ = dict(zip(feature_names, self.model.feature_importances_))
        elif hasattr(self.model, 'coef_'):
            feature_names = X.columns.tolist()
            self.feature_importance_ = dict(zip(feature_names, np.abs(self.model.coef_)))
        
        cv_scores = cross_val_score(self.model, X_train, y_train, cv=5, 
                                   scoring='neg_mean_absolute_error', n_jobs=-1)
        metrics['cv_mae_mean'] = -cv_scores.mean()
        metrics['cv_mae_std'] = cv_scores.std()
        
        self.training_metrics = metrics
        
        self.logger.info(f"Training completed. Validation MAE: {metrics['val_mae']:.4f}, "
                        f"Test R²: {metrics['test_r2']:.4f}")
        
        return metrics
    
    def predict(self, df: pd.DataFrame) -> np.ndarray:
        if not self.is_fitted:
            raise ValueError("Model must be trained before making predictions")
        
        X = self.feature_pipeline.transform(df)
        predictions = self.model.predict(X)
        
        return predictions
    
    def predict_with_confidence(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        predictions = self.predict(df)
        
        if hasattr(self.model, 'predict') and self.model_type == 'random_forest':
            tree_predictions = np.array([tree.predict(self.feature_pipeline.transform(df)) 
                                       for tree in self.model.estimators_])
            confidence = 1 - np.std(tree_predictions, axis=0) / np.mean(np.abs(tree_predictions), axis=0)
            confidence = np.clip(confidence, 0, 1)
        else:
            residuals_std = np.sqrt(self.training_metrics.get('val_mse', 1.0))
            confidence = 1 / (1 + residuals_std)
            confidence = np.full(len(predictions), confidence)
        
        return predictions, confidence
    
    def hyperparameter_tuning(self, df: pd.DataFrame, target_column: str = 'Severity',
                             param_grid: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        
        self.logger.info(f"Starting hyperparameter tuning for {self.model_type}")
        
        X, y = self.feature_pipeline.fit_transform(df, target_column)
        
        if param_grid is None:
            param_grid = self._get_default_param_grid()
        
        grid_search = GridSearchCV(
            self.model, param_grid, cv=5, scoring='neg_mean_absolute_error',
            n_jobs=-1, verbose=1
        )
        
        grid_search.fit(X, y)
        
        self.model = grid_search.best_estimator_
        self.is_fitted = True
        
        tuning_results = {
            'best_params': grid_search.best_params_,
            'best_score': -grid_search.best_score_,
            'cv_results': grid_search.cv_results_
        }
        
        self.logger.info(f"Hyperparameter tuning completed. Best MAE: {tuning_results['best_score']:.4f}")
        
        return tuning_results
    
    def _get_default_param_grid(self) -> Dict[str, Any]:
        param_grids = {
            'random_forest': {
                'n_estimators': [50, 100, 200],
                'max_depth': [5, 10, 15, None],
                'min_samples_split': [2, 5, 10],
                'min_samples_leaf': [1, 2, 4]
            },
            'gradient_boosting': {
                'n_estimators': [50, 100, 200],
                'learning_rate': [0.05, 0.1, 0.2],
                'max_depth': [3, 5, 7],
                'min_samples_split': [2, 5, 10]
            },
            'ridge': {
                'alpha': [0.1, 1.0, 10.0, 100.0]
            },
            'lasso': {
                'alpha': [0.1, 1.0, 10.0, 100.0]
            },
            'svr': {
                'C': [0.1, 1, 10],
                'gamma': ['scale', 'auto', 0.001, 0.01],
                'kernel': ['rbf', 'linear']
            }
        }
        
        return param_grids.get(self.model_type, {})
    
    def evaluate_model(self, df: pd.DataFrame, target_column: str = 'Severity') -> Dict[str, Any]:
        if not self.is_fitted:
            raise ValueError("Model must be trained before evaluation")
        
        X, y = self.feature_pipeline.fit_transform(df, target_column)
        predictions = self.model.predict(X)
        
        evaluation_metrics = {
            'mae': mean_absolute_error(y, predictions),
            'mse': mean_squared_error(y, predictions),
            'rmse': np.sqrt(mean_squared_error(y, predictions)),
            'r2': r2_score(y, predictions),
            'samples_count': len(y)
        }
        
        return evaluation_metrics
    
    def get_model_metadata(self, model_name: str, model_version: str) -> Dict[str, Any]:
        if not self.is_fitted:
            raise ValueError("Model must be trained before getting metadata")
        
        metadata = {
            'model_name': model_name,
            'model_version': model_version,
            'model_type': 'regression',
            'algorithm': self.model_type,
            'training_date': datetime.now(),
            'feature_count': len(self.feature_pipeline.feature_columns) if self.feature_pipeline.feature_columns else 0,
            'training_samples': self.training_metrics.get('training_samples', 0),
            'validation_samples': self.training_metrics.get('validation_samples', 0),
            'mae_score': self.training_metrics.get('val_mae'),
            'mse_score': self.training_metrics.get('val_mse'),
            'rmse_score': self.training_metrics.get('val_rmse'),
            'r2_score': self.training_metrics.get('val_r2'),
            'feature_importance': self.feature_importance_ or {},
            'hyperparameters': self.model.get_params()
        }
        
        return metadata
    
    def get_feature_importance_data(self) -> list:
        if not self.feature_importance_:
            return []
        
        return self.feature_pipeline.get_feature_importance_data(self.feature_importance_)
    
    def generate_predictions_data(self, df: pd.DataFrame, model_id: int, 
                                 prediction_batch: str) -> list:
        predictions, confidence = self.predict_with_confidence(df)
        
        predictions_data = []
        for i, (pred, conf) in enumerate(zip(predictions, confidence)):
            pred_data = {
                'model_id': model_id,
                'prediction_batch': prediction_batch,
                'accident_id': df.iloc[i].get('ID', f'acc_{i}'),
                'predicted_severity': float(pred),
                'confidence_score': float(conf),
                'prediction_date': datetime.now(),
                'input_features': df.iloc[i].to_dict()
            }
            
            if 'Severity' in df.columns:
                actual_severity = df.iloc[i]['Severity']
                pred_data['actual_severity'] = float(actual_severity)
                pred_data['prediction_error'] = abs(float(pred) - float(actual_severity))
            
            predictions_data.append(pred_data)
        
        return predictions_data