import argparse
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.config import Config
from src.core.logger import get_logger
from src.utils.helpers import get_partition_path, format_execution_time, validate_file_path
from src.ml.model_trainer import SeverityRegressor
from src.ml.model_storage import MLModelWriter

class MLAnalysisApp:
    
    def __init__(self):
        self.config = Config()
        self.logger = get_logger(__name__)
        self.ml_writer = MLModelWriter()
        
        self.logger.info("ML Analysis App initialized")
    
    def get_silver_data_paths(self, target_date: str) -> str:
        accidents_path = get_partition_path(f"{self.config.silver_output_path}/accidents_clean", target_date)
        
        if not validate_file_path(accidents_path):
            raise FileNotFoundError(f"Accidents data not found: {accidents_path}")
        
        return accidents_path
    
    def load_silver_datasets(self, target_date: str):
        self.logger.info("Loading Silver layer datasets")
        
        accidents_path = self.get_silver_data_paths(target_date)
        
        all_data = []
        for root, dirs, files in os.walk(accidents_path):
            for file in files:
                if file.endswith('.parquet') and not file.startswith('.') and not file.startswith('_'):
                    file_path = os.path.join(root, file)
                    try:
                        df = pd.read_parquet(file_path)
                        if len(df) > 0:
                            all_data.append(df)
                            self.logger.info(f"Loaded {len(df)} records from {file_path}")
                    except Exception as e:
                        self.logger.warning(f"Could not load {file_path}: {e}")
        
        if not all_data:
            raise ValueError("No parquet files found in silver accidents data")
        
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.dropna(subset=['Severity'])
        combined_df = combined_df[combined_df['Severity'].between(1, 4)]
        
        accidents_count = len(combined_df)
        self.logger.info(f"Loaded accidents data: {accidents_count} records")
        
        return combined_df
    
    def run_model_training(self, df: pd.DataFrame, model_name: str, model_version: str, model_type: str):
        self.logger.info(f"Training {model_type} model: {model_name} v{model_version}")
        
        regressor = SeverityRegressor(model_type=model_type)
        training_metrics = regressor.train(df)
        
        model_metadata = regressor.get_model_metadata(model_name, model_version)
        model_id = self.ml_writer.save_model(regressor.model, model_metadata)
        
        feature_data = regressor.get_feature_importance_data()
        if feature_data:
            self.ml_writer.save_features(model_id, feature_data)
        
        self.logger.info(f"Model {model_name} training completed. Model ID: {model_id}")
        self.logger.info(f"Training metrics: MAE={training_metrics['val_mae']:.4f}, R²={training_metrics['val_r2']:.4f}")
        
        return model_id, training_metrics, regressor
    
    def run_model_predictions(self, regressor: SeverityRegressor, test_df: pd.DataFrame, model_id: int, prediction_batch: str):
        self.logger.info("Running model predictions")
        
        predictions_data = regressor.generate_predictions_data(test_df, model_id, prediction_batch)
        self.ml_writer.save_predictions(predictions_data)
        
        self.logger.info(f"Saved {len(predictions_data)} predictions to database")
    
    def run_analysis(self, target_date: str):
        start_time = datetime.now()
        self.logger.info(f"Starting ML analysis for {target_date}")
        
        try:
            self.logger.info("Step 1 - Creating database tables")
            self.ml_writer.create_tables()
            
            self.logger.info("Step 2 - Loading Silver layer data")
            df = self.load_silver_datasets(target_date)
            
            if len(df) < 100:
                self.logger.warning(f"Limited training data: {len(df)} records. Results may not be optimal.")
            
            test_size = min(500, len(df) // 4)
            test_df = df.sample(n=test_size, random_state=42)
            
            self.logger.info("Step 3 - Training Random Forest model")
            model_id_rf, metrics_rf, regressor_rf = self.run_model_training(
                df, "accident_severity_rf", "1.0", "random_forest"
            )
            
            self.logger.info("Step 4 - Training Gradient Boosting model")
            model_id_gb, metrics_gb, regressor_gb = self.run_model_training(
                df, "accident_severity_gb", "1.0", "gradient_boosting"
            )
            
            self.logger.info("Step 5 - Training Ridge Regression model")
            model_id_ridge, metrics_ridge, regressor_ridge = self.run_model_training(
                df, "accident_severity_ridge", "1.0", "ridge"
            )
            
            self.logger.info("Step 6 - Running predictions with best model")
            prediction_batch = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.run_model_predictions(regressor_rf, test_df, model_id_rf, prediction_batch)
            
            end_time = datetime.now()
            execution_time_str = format_execution_time(start_time, end_time)
            
            self.logger.info("=" * 60)
            self.logger.info("ML ANALYSIS COMPLETED SUCCESSFULLY!")
            self.logger.info("=" * 60)
            self.logger.info(f"Training Data: {len(df)} records")
            self.logger.info(f"Test Data: {test_size} records")
            self.logger.info(f"Models Trained: 3 total")
            self.logger.info("\nModel Performance Summary:")
            self.logger.info(f"Random Forest    - MAE: {metrics_rf['val_mae']:.4f}, R²: {metrics_rf['val_r2']:.4f}")
            self.logger.info(f"Gradient Boost   - MAE: {metrics_gb['val_mae']:.4f}, R²: {metrics_gb['val_r2']:.4f}")
            self.logger.info(f"Ridge Regression - MAE: {metrics_ridge['val_mae']:.4f}, R²: {metrics_ridge['val_r2']:.4f}")
            
            best_model = min([
                ("Random Forest", metrics_rf['val_mae']),
                ("Gradient Boosting", metrics_gb['val_mae']),
                ("Ridge Regression", metrics_ridge['val_mae'])
            ], key=lambda x: x[1])
            
            self.logger.info(f"\nBest Model: {best_model[0]} (MAE: {best_model[1]:.4f})")
            self.logger.info(f"✓ All data stored in MySQL database")
            self.logger.info(f"Analysis completed in {execution_time_str}")
            
        except Exception as e:
            self.logger.error(f"ML analysis failed: {str(e)}")
            raise
        finally:
            self.ml_writer.close_connection()
    
    def run_quick_demo(self):
        self.logger.info("Running quick ML demo")
        
        try:
            self.logger.info("Step 1 - Creating database tables")
            self.ml_writer.create_tables()
            
            self.logger.info("Step 2 - Loading Silver layer data")
            # Use latest available date
            from src.utils.helpers import get_incremental_dates
            available_dates = get_incremental_dates()
            latest_date = available_dates[-1] if available_dates else "2025/01/03"
            
            df = self.load_silver_datasets(latest_date)
            
            sample_size = min(500, len(df))
            sample_df = df.sample(n=sample_size, random_state=42)
            
            self.logger.info(f"Step 3 - Training demo model with {sample_size} records")
            model_id, metrics, regressor = self.run_model_training(
                sample_df, "demo_severity_model", "1.0", "random_forest"
            )
            
            test_sample = sample_df.sample(n=min(50, sample_size//4), random_state=42)
            prediction_batch = f"demo_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            self.logger.info("Step 4 - Running demo predictions")
            self.run_model_predictions(regressor, test_sample, model_id, prediction_batch)
            
            self.logger.info("=" * 50)
            self.logger.info("QUICK DEMO COMPLETED!")
            self.logger.info("=" * 50)
            self.logger.info(f"Training Records: {sample_size}")
            self.logger.info(f"Test Records: {len(test_sample)}")
            self.logger.info(f"Model MAE: {metrics['val_mae']:.4f}")
            self.logger.info(f"Model R²: {metrics['val_r2']:.4f}")
            self.logger.info("✓ Demo data stored in MySQL database")
            
        except Exception as e:
            self.logger.error(f"Quick demo failed: {str(e)}")
            raise
        finally:
            self.ml_writer.close_connection()

def main():
    from src.utils.helpers import get_incremental_dates
    
    logger = get_logger(__name__)
    logger.info("Starting ML Analysis Application")
    
    parser = argparse.ArgumentParser(description='Machine Learning Analysis for Accident Severity Prediction')
    parser.add_argument('--mode', choices=['full', 'demo'], default='demo',
                       help='Run mode: full analysis or quick demo')
    parser.add_argument('--date', 
                       choices=get_incremental_dates(),
                       help='Target date for ML analysis (required for full mode)')
    
    args = parser.parse_args()
    
    if args.mode == 'full' and not args.date:
        logger.error("--date is required for full mode")
        sys.exit(1)
    
    logger.info(f"Mode: {args.mode}")
    if args.date:
        logger.info(f"Target date: {args.date}")
    
    try:
        app = MLAnalysisApp()
        
        if args.mode == 'full':
            app.run_analysis(args.date)
        else:
            app.run_quick_demo()
        
        logger.info("ML analysis execution completed successfully")
        
    except Exception as e:
        logger.error(f"ML analysis execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
