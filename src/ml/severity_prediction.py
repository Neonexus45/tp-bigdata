import argparse
import pandas as pd
from datetime import datetime
from typing import Optional
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))


from src.core.config import Config
from src.core.logger import get_logger
from src.ml.model_trainer import SeverityRegressor
from src.ml.model_storage import MLModelWriter

class SeverityPredictionApp:
    
    def __init__(self):
        self.config = Config()
        self.logger = get_logger(__name__)
        self.model_writer = MLModelWriter()
        
    def train_model(self, input_path: str, model_name: str, model_version: str,
                   model_type: str = 'random_forest', tune_hyperparameters: bool = False):
        
        self.logger.info(f"Starting model training: {model_name} v{model_version}")
        
        try:
            df = pd.read_parquet(input_path)
            self.logger.info(f"Loaded dataset with {len(df)} records")
            
            regressor = SeverityRegressor(model_type=model_type)
            
            if tune_hyperparameters:
                self.logger.info("Performing hyperparameter tuning")
                tuning_results = regressor.hyperparameter_tuning(df)
                self.logger.info(f"Best parameters: {tuning_results['best_params']}")
            
            training_metrics = regressor.train(df)
            
            self.model_writer.create_tables()
            
            model_metadata = regressor.get_model_metadata(model_name, model_version)
            model_id = self.model_writer.save_model(regressor.model, model_metadata)
            
            feature_data = regressor.get_feature_importance_data()
            if feature_data:
                self.model_writer.save_features(model_id, feature_data)
            
            self.logger.info(f"Model training completed successfully. Model ID: {model_id}")
            self.logger.info(f"Training metrics: MAE={training_metrics['val_mae']:.4f}, "
                           f"R²={training_metrics['val_r2']:.4f}")
            
            return model_id, training_metrics
            
        except Exception as e:
            self.logger.error(f"Error during model training: {e}")
            raise
        finally:
            self.model_writer.close_connection()
    
    def predict(self, input_path: str, model_name: str, output_path: Optional[str] = None,
               model_version: Optional[str] = None, prediction_batch: Optional[str] = None):
        
        self.logger.info(f"Starting prediction with model: {model_name}")
        
        try:
            df = pd.read_parquet(input_path)
            self.logger.info(f"Loaded dataset with {len(df)} records for prediction")
            
            model = self.model_writer.load_model(model_name, model_version)
            if model is None:
                raise ValueError(f"Model {model_name} not found")
            
            model_metadata = self.model_writer.get_model_metadata(model_name, model_version)
            model_id = model_metadata['id']
            
            regressor = SeverityRegressor(model_type=model_metadata['algorithm'])
            regressor.model = model
            regressor.is_fitted = True
            
            predictions, confidence = regressor.predict_with_confidence(df)
            
            results_df = df.copy()
            results_df['predicted_severity'] = predictions
            results_df['confidence_score'] = confidence
            results_df['prediction_date'] = datetime.now()
            
            if output_path:
                results_df.to_parquet(output_path, index=False)
                self.logger.info(f"Predictions saved to: {output_path}")
            
            if prediction_batch:
                predictions_data = regressor.generate_predictions_data(df, model_id, prediction_batch)
                self.model_writer.save_predictions(predictions_data)
                self.logger.info(f"Predictions saved to database with batch: {prediction_batch}")
            
            self.logger.info(f"Prediction completed for {len(predictions)} records")
            
            return results_df
            
        except Exception as e:
            self.logger.error(f"Error during prediction: {e}")
            raise
        finally:
            self.model_writer.close_connection()
    
    def evaluate_model(self, test_data_path: str, model_name: str, 
                      model_version: Optional[str] = None):
        
        self.logger.info(f"Starting model evaluation: {model_name}")
        
        try:
            df = pd.read_parquet(test_data_path)
            self.logger.info(f"Loaded test dataset with {len(df)} records")
            
            model = self.model_writer.load_model(model_name, model_version)
            if model is None:
                raise ValueError(f"Model {model_name} not found")
            
            model_metadata = self.model_writer.get_model_metadata(model_name, model_version)
            
            regressor = SeverityRegressor(model_type=model_metadata['algorithm'])
            regressor.model = model
            regressor.is_fitted = True
            
            evaluation_metrics = regressor.evaluate_model(df)
            
            self.logger.info("Model evaluation completed:")
            for metric, value in evaluation_metrics.items():
                self.logger.info(f"  {metric}: {value:.4f}")
            
            return evaluation_metrics
            
        except Exception as e:
            self.logger.error(f"Error during model evaluation: {e}")
            raise
        finally:
            self.model_writer.close_connection()
    
    def list_models(self):
        self.logger.info("Listing available models")
        
        try:
            connection = self.model_writer._get_connection()
            cursor = connection.cursor(dictionary=True)
            
            query = """
            SELECT model_name, model_version, algorithm, training_date, 
                   mae_score, r2_score, is_active
            FROM ml_models 
            ORDER BY training_date DESC
            """
            
            cursor.execute(query)
            models = cursor.fetchall()
            
            if models:
                self.logger.info(f"Found {len(models)} models:")
                for model in models:
                    status = "ACTIVE" if model['is_active'] else "INACTIVE"
                    self.logger.info(f"  {model['model_name']} v{model['model_version']} "
                                   f"({model['algorithm']}) - {status}")
                    self.logger.info(f"    Trained: {model['training_date']}, "
                                   f"MAE: {model['mae_score']:.4f}, "
                                   f"R²: {model['r2_score']:.4f}")
            else:
                self.logger.info("No models found")
            
            return models
            
        except Exception as e:
            self.logger.error(f"Error listing models: {e}")
            raise
        finally:
            cursor.close()
            self.model_writer.close_connection()

def main():
    parser = argparse.ArgumentParser(description='Accident Severity Prediction ML Application')
    parser.add_argument('command', choices=['train', 'predict', 'evaluate', 'list'],
                       help='Command to execute')
    
    parser.add_argument('--input-path', type=str, help='Input data path')
    parser.add_argument('--output-path', type=str, help='Output path for predictions')
    parser.add_argument('--model-name', type=str, help='Model name')
    parser.add_argument('--model-version', type=str, help='Model version')
    parser.add_argument('--model-type', type=str, default='random_forest',
                       choices=['random_forest', 'gradient_boosting', 'linear_regression', 
                               'ridge', 'lasso', 'svr'],
                       help='Type of model to train')
    parser.add_argument('--tune-hyperparameters', action='store_true',
                       help='Perform hyperparameter tuning during training')
    parser.add_argument('--prediction-batch', type=str, help='Prediction batch identifier')
    
    args = parser.parse_args()
    
    app = SeverityPredictionApp()
    
    try:
        if args.command == 'train':
            if not args.input_path or not args.model_name or not args.model_version:
                raise ValueError("Training requires --input-path, --model-name, and --model-version")
            
            model_id, metrics = app.train_model(
                args.input_path, args.model_name, args.model_version,
                args.model_type, args.tune_hyperparameters
            )
            print(f"Model trained successfully. ID: {model_id}")
            
        elif args.command == 'predict':
            if not args.input_path or not args.model_name:
                raise ValueError("Prediction requires --input-path and --model-name")
            
            results = app.predict(
                args.input_path, args.model_name, args.output_path,
                args.model_version, args.prediction_batch
            )
            print(f"Predictions completed for {len(results)} records")
            
        elif args.command == 'evaluate':
            if not args.input_path or not args.model_name:
                raise ValueError("Evaluation requires --input-path and --model-name")
            
            metrics = app.evaluate_model(args.input_path, args.model_name, args.model_version)
            print("Evaluation metrics:")
            for metric, value in metrics.items():
                print(f"  {metric}: {value:.4f}")
                
        elif args.command == 'list':
            models = app.list_models()
            print(f"Found {len(models)} models")
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())