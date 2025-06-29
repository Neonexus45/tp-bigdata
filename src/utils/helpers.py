"""Common utility functions for US Accidents Lakehouse project."""

import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull


def validate_file_path(path: str) -> bool:
    """Validate if a file path exists.
    
    Args:
        path: File path to validate
        
    Returns:
        True if path exists, False otherwise
    """
    return os.path.exists(path)


def create_directory_if_not_exists(path: str) -> None:
    """Create directory if it doesn't exist.
    
    Args:
        path: Directory path to create
    """
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def get_partition_path(base_path: str, date: str) -> str:
    """Generate partition path based on date.
    
    Args:
        base_path: Base output path
        date: Date in YYYY-MM-DD format
        
    Returns:
        Partitioned path in format base_path/YYYY/MM/DD
    """
    year, month, day = date.split("-")
    return f"{base_path}/{year}/{month}/{day}"


def analyze_dataframe_quality(df: DataFrame) -> Dict[str, Any]:
    """Analyze DataFrame data quality metrics.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary containing quality metrics
    """
    total_records = df.count()
    total_columns = len(df.columns)
    
    quality_metrics = {
        "total_records": total_records,
        "total_columns": total_columns,
        "column_analysis": {}
    }
    
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
        
        quality_metrics["column_analysis"][column] = {
            "null_count": null_count,
            "null_percentage": round(null_percentage, 2),
            "data_type": str(df.schema[column].dataType)
        }
    
    return quality_metrics


def get_selected_columns() -> List[str]:
    """Get the standard list of columns for US Accidents data.
    
    Returns:
        List of column names to select from source data
    """
    return [
        "ID", "Source", "Severity", "Start_Time", "End_Time",
        "Start_Lat", "Start_Lng", "City", "County", "State", "Zipcode",
        "Timezone", "Temperature(F)", "Humidity(%)", "Pressure(in)",
        "Visibility(mi)", "Wind_Direction", "Wind_Speed(mph)", 
        "Precipitation(in)", "Weather_Condition", "Bump", "Crossing",
        "Give_Way", "Junction", "No_Exit", "Railway", "Roundabout",
        "Station", "Stop", "Traffic_Calming", "Traffic_Signal", "Turning_Loop"
    ]


def filter_available_columns(df: DataFrame, desired_columns: List[str]) -> List[str]:
    """Filter desired columns to only include those available in DataFrame.
    
    Args:
        df: Source DataFrame
        desired_columns: List of desired column names
        
    Returns:
        List of available columns from desired list
    """
    available_columns = df.columns
    return [col for col in desired_columns if col in available_columns]


def log_missing_columns(desired_columns: List[str], available_columns: List[str]) -> List[str]:
    """Log missing columns and return the list.
    
    Args:
        desired_columns: List of desired column names
        available_columns: List of available column names
        
    Returns:
        List of missing column names
    """
    missing_columns = [col for col in desired_columns if col not in available_columns]
    return missing_columns


def format_execution_time(start_time: datetime, end_time: datetime) -> str:
    """Format execution time in a readable format.
    
    Args:
        start_time: Process start time
        end_time: Process end time
        
    Returns:
        Formatted execution time string
    """
    duration = end_time - start_time
    total_seconds = duration.total_seconds()
    
    if total_seconds < 60:
        return f"{total_seconds:.2f} seconds"
    elif total_seconds < 3600:
        minutes = int(total_seconds // 60)
        seconds = total_seconds % 60
        return f"{minutes}m {seconds:.2f}s"
    else:
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = total_seconds % 60
        return f"{hours}h {minutes}m {seconds:.2f}s"


def get_file_size_mb(file_path: str) -> float:
    """Get file size in megabytes.
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in MB, 0.0 if file doesn't exist
    """
    try:
        if os.path.exists(file_path):
            size_bytes = os.path.getsize(file_path)
            return size_bytes / (1024 * 1024)
        return 0.0
    except OSError:
        return 0.0


def validate_date_format(date_str: str) -> bool:
    """Validate date string format (YYYY-MM-DD).
    
    Args:
        date_str: Date string to validate
        
    Returns:
        True if valid format, False otherwise
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_incremental_dates() -> List[str]:
    """Get list of valid incremental processing dates.
    
    Returns:
        List of valid date strings
    """
    return ["2025-01-01", "2025-01-02", "2025-01-03"]