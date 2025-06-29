from typing import List

class DatamartSchema:
    
    @staticmethod
    def get_weather_accident_frequency_ddl() -> str:
        return """
        CREATE TABLE IF NOT EXISTS weather_accident_frequency (
            id INT AUTO_INCREMENT PRIMARY KEY,
            weather_category VARCHAR(50) NOT NULL,
            temperature_category VARCHAR(20) NOT NULL,
            accident_count INT NOT NULL,
            total_accidents INT NOT NULL,
            frequency_percentage DECIMAL(5,2) NOT NULL,
            analysis_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_weather_category (weather_category),
            INDEX idx_temperature_category (temperature_category),
            INDEX idx_analysis_date (analysis_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    
    @staticmethod
    def get_weather_severity_correlation_ddl() -> str:
        return """
        CREATE TABLE IF NOT EXISTS weather_severity_correlation (
            id INT AUTO_INCREMENT PRIMARY KEY,
            weather_category VARCHAR(50) NOT NULL,
            severity_level INT NOT NULL,
            accident_count INT NOT NULL,
            avg_duration_hours DECIMAL(8,2),
            correlation_score DECIMAL(5,3),
            analysis_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_weather_severity (weather_category, severity_level),
            INDEX idx_analysis_date (analysis_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    
    @staticmethod
    def get_weather_impact_trends_ddl() -> str:
        return """
        CREATE TABLE IF NOT EXISTS weather_impact_trends (
            id INT AUTO_INCREMENT PRIMARY KEY,
            period_month VARCHAR(7) NOT NULL,
            weather_category VARCHAR(50) NOT NULL,
            total_accidents INT NOT NULL,
            avg_severity DECIMAL(3,2) NOT NULL,
            trend_direction VARCHAR(20),
            seasonal_factor DECIMAL(5,3),
            analysis_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_period_month (period_month),
            INDEX idx_weather_category (weather_category),
            INDEX idx_analysis_date (analysis_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    
    @staticmethod
    def get_all_table_ddls() -> List[str]:
        return [
            DatamartSchema.get_weather_accident_frequency_ddl(),
            DatamartSchema.get_weather_severity_correlation_ddl(),
            DatamartSchema.get_weather_impact_trends_ddl()
        ]
    
    @staticmethod
    def get_table_names() -> List[str]:
        return [
            "weather_accident_frequency",
            "weather_severity_correlation", 
            "weather_impact_trends"
        ]