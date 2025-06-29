import argparse
import sys
from pyspark.sql.functions import lit, col
from datetime import datetime
import os
from typing import List, Optional

from src.core.config import Config
from src.core.logger import get_logger
from src.core.spark_manager import SparkManager
from src.utils.helpers import (
    get_selected_columns,
    filter_available_columns,
    log_missing_columns,
    get_partition_path,
    format_execution_time
)

class USAccidentsFeeder:
    """US Accidents Bronze Layer Feeder with incremental data simulation."""
    
    def __init__(self) -> None:
        """Initialize the feeder with core classes."""
        self.config = Config()
        self.logger = get_logger(__name__)
        self.spark_manager = SparkManager()
        
        self.logger.info("Initializing US Accidents Feeder")
        
        self.selected_columns = get_selected_columns()
        
        self._partitions_cache: Optional[List] = None
        
        self.logger.info("US Accidents Feeder initialized successfully")

    def load_source_data(self):
        """Load source data from CSV file with column validation.
        
        Returns:
            DataFrame: Loaded and filtered DataFrame
        """
        self.logger.info(f"Loading source data from {self.config.input_path}")
        
        try:
            
            df_full = self.spark_manager.read_csv(self.config.input_path)
            
            available_columns = df_full.columns
            self.logger.info(f"Available columns: {len(available_columns)}")
            self.logger.debug(f"Column list: {available_columns}")
            
            missing_columns = log_missing_columns(self.selected_columns, available_columns)
            if missing_columns:
                self.logger.warning(f"Missing columns: {missing_columns}")
                
            self.selected_columns = filter_available_columns(df_full, self.selected_columns)
            
            self.logger.info("Selecting desired columns")
            df = df_full.select(*self.selected_columns)
            
            total_records = df.count()
            self.logger.info(f"Loaded {total_records} records with {len(self.selected_columns)} columns")
            
            for i, col_name in enumerate(self.selected_columns):
                self.logger.debug(f"  {i:2d}. {col_name}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading source data: {str(e)}")
            raise

    def split_dataset_incrementally(self, df):
        """Split dataset into 3 incremental partitions for simulation.
        
        Args:
            df: Source DataFrame to split
            
        Returns:
            List of DataFrames representing the 3 partitions
        """
        self.logger.info("Splitting dataset into 3 incremental partitions")
        
        if self._partitions_cache is None:
            self.logger.info("Creating partitions with randomSplit")
            
            self._partitions_cache = df.randomSplit([0.33, 0.33, 0.34], seed=42)
            
            for i, partition in enumerate(self._partitions_cache, 1):
                count = partition.count()
                self.logger.info(f"Partition {i}: {count} records")
        
        return self._partitions_cache

    def create_incremental_data(self, partitions, target_date: str):
        """Create incremental data based on target date and partitions.
        
        Args:
            partitions: List of DataFrame partitions
            target_date: Target date for incremental processing
            
        Returns:
            DataFrame: Incremental data with ingestion_date column
        """
        self.logger.info(f"Creating incremental data for {target_date}")
        
        part1, part2, part3 = partitions
        
        if target_date == "2025-01-01":
            result_df = part1
            self.logger.info("Day 1: Partition 1 only")
            
        elif target_date == "2025-01-02":
            result_df = part1.union(part2)
            self.logger.info("Day 2: Partition 1 + Partition 2 (cumulative)")
            
        elif target_date == "2025-01-03":
            result_df = part1.union(part2).union(part3)
            self.logger.info("Day 3: Partition 1 + Partition 2 + Partition 3 (complete historical)")
            
        else:
            self.logger.error(f"Invalid target date: {target_date}")
            raise ValueError(f"Invalid target_date: {target_date}")
        
        result_df = result_df.withColumn("ingestion_date", lit(target_date))
        
        record_count = result_df.count()
        self.logger.info(f"Total records for {target_date}: {record_count}")
        
        return result_df

    def save_to_bronze(self, df, target_date: str) -> None:
        """Save DataFrame to Bronze layer with date partitioning.
        
        Args:
            df: DataFrame to save
            target_date: Target date for partitioning
        """
        self.logger.info(f"Saving to Bronze Layer for {target_date}")
        
        output_path = get_partition_path(self.config.bronze_output_path, target_date)
        
        self.logger.info(f"Output path: {output_path}")
        
        try:
            self.logger.info("Starting write in overwrite mode")

            self.spark_manager.write_parquet(df, output_path, mode="overwrite")
            
            self.logger.info(f"Successfully saved to {output_path}")
            
            self._log_output_metadata(output_path)
            
        except Exception as e:
            self.logger.error(f"Error saving to Bronze layer: {str(e)}")
            raise

    def _log_output_metadata(self, output_path: str) -> None:
        """Log output metadata for verification.
        
        Args:
            output_path: Path to output directory
        """
        self.logger.info("Verifying output metadata")
        try:

            if os.path.exists(output_path):
                parquet_files = [f for f in os.listdir(output_path) if f.endswith('.parquet')]
                self.logger.info(f"Created {len(parquet_files)} parquet files")
                
                total_size = sum(os.path.getsize(os.path.join(output_path, f))
                               for f in parquet_files)
                self.logger.info(f"Total size: {total_size / (1024*1024):.2f} MB")
                
        except Exception as e:
            self.logger.warning(f"Unable to read metadata: {str(e)}")

    def run_feeder(self, target_date: str) -> None:
        """Run the complete feeder process for the target date.
        
        Args:
            target_date: Target date for incremental feeding
        """
        start_time = datetime.now()
        self.logger.info(f"Starting Bronze Feeder for {target_date}")
        
        try:
            self.logger.info("Step 1 - Loading source data")
            source_df = self.load_source_data()
            
            self.logger.info("Step 2 - Splitting dataset into partitions")
            partitions = self.split_dataset_incrementally(source_df)
            
            self.logger.info("Step 3 - Creating incremental data")
            incremental_df = self.create_incremental_data(partitions, target_date)
            
            self.logger.info("Step 4 - Saving to Bronze layer")
            self.save_to_bronze(incremental_df, target_date)
            
            end_time = datetime.now()
            execution_time_str = format_execution_time(start_time, end_time)
            self.logger.info(f"Feeder completed in {execution_time_str}")
            
        except Exception as e:
            self.logger.error(f"Feeder failed: {str(e)}")
            raise
        finally:
            self.logger.info("Stopping Spark session")
            self.spark_manager.stop_session()

def main() -> None:
    """Main entry point for the US Accidents Feeder application."""
    from src.utils.helpers import get_incremental_dates
    
    logger = get_logger(__name__)
    logger.info("Starting US Accidents Feeder application")
    
    parser = argparse.ArgumentParser(description='US Accidents Bronze Feeder')
    parser.add_argument('--date', required=True,
                       choices=get_incremental_dates(),
                       help='Target date for incremental feeding')
    
    args = parser.parse_args()
    logger.info(f"Target date: {args.date}")
    
    try:

        feeder = USAccidentsFeeder()
        feeder.run_feeder(args.date)
        
        logger.info("Feeder execution completed successfully")
        
    except Exception as e:
        logger.error(f"Feeder execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()