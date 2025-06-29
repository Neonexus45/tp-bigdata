from typing import List

class MLSchema:
    
    @staticmethod
    def get_ml_models_ddl() -> str:
        return """
        CREATE TABLE IF NOT EXISTS ml_models (
            id INT AUTO_INCREMENT PRIMARY KEY,
            model_name VARCHAR(100) NOT NULL,
            model_version VARCHAR(20) NOT NULL,
            model_type VARCHAR(50) NOT NULL,
            algorithm VARCHAR(50) NOT NULL,
            training_date DATETIME NOT NULL,
            feature_count INT NOT NULL,
            training_samples INT NOT NULL,
            validation_samples INT NOT NULL,
            mae_score DECIMAL(10,6),
            mse_score DECIMAL(10,6),
            rmse_score DECIMAL(10,6),
            r2_score DECIMAL(10,6),
            model_path VARCHAR(500),
            feature_importance JSON,
            hyperparameters JSON,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_model_name (model_name),
            INDEX idx_model_version (model_version),
            INDEX idx_training_date (training_date),
            INDEX idx_is_active (is_active),
            UNIQUE KEY unique_model_version (model_name, model_version)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    
    @staticmethod
    def get_ml_features_ddl() -> str:
        return """
        CREATE TABLE IF NOT EXISTS ml_features (
            id INT AUTO_INCREMENT PRIMARY KEY,
            model_id INT NOT NULL,
            feature_name VARCHAR(100) NOT NULL,
            feature_type VARCHAR(50) NOT NULL,
            importance_score DECIMAL(10,6),
            feature_category VARCHAR(50),
            description TEXT,
            min_value DECIMAL(15,6),
            max_value DECIMAL(15,6),
            mean_value DECIMAL(15,6),
            std_value DECIMAL(15,6),
            null_percentage DECIMAL(5,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES ml_models(id) ON DELETE CASCADE,
            INDEX idx_model_id (model_id),
            INDEX idx_feature_name (feature_name),
            INDEX idx_feature_type (feature_type),
            INDEX idx_importance_score (importance_score)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    
    @staticmethod
    def get_ml_predictions_ddl() -> str:
        return """
        CREATE TABLE IF NOT EXISTS ml_predictions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            model_id INT NOT NULL,
            prediction_batch VARCHAR(100) NOT NULL,
            accident_id VARCHAR(50),
            predicted_severity DECIMAL(8,4) NOT NULL,
            confidence_score DECIMAL(5,4),
            prediction_date DATETIME NOT NULL,
            input_features JSON,
            actual_severity DECIMAL(8,4),
            prediction_error DECIMAL(8,4),
            is_validated BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES ml_models(id) ON DELETE CASCADE,
            INDEX idx_model_id (model_id),
            INDEX idx_prediction_batch (prediction_batch),
            INDEX idx_accident_id (accident_id),
            INDEX idx_prediction_date (prediction_date),
            INDEX idx_is_validated (is_validated)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    
    @staticmethod
    def get_all_table_ddls() -> List[str]:
        return [
            MLSchema.get_ml_models_ddl(),
            MLSchema.get_ml_features_ddl(),
            MLSchema.get_ml_predictions_ddl()
        ]
    
    @staticmethod
    def get_table_names() -> List[str]:
        return [
            "ml_models",
            "ml_features",
            "ml_predictions"
        ]