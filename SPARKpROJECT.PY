import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, desc, lower, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging
from typing import List, Optional

class OpenFoodFactsProcessor:
    """Process OpenFoodFacts dataset using PySpark."""

    def __init__(self, app_name: str = "OpenFoodFactsProcessor"):
        """Initialize the processor with SparkSession and logger."""
        self.logger = self._setup_logger()
        self.spark = self._create_spark_session(app_name)

    def _setup_logger(self) -> logging.Logger:
        """Configure logging for the application."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def _create_spark_session(self, app_name: str) -> SparkSession:
        """Create and configure SparkSession."""
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()

    def load_data(self, file_path: str) -> Optional[SparkSession]:
        """
        Load OpenFoodFacts dataset with proper column handling.

        Args:
            file_path: Path to the CSV file

        Returns:
            Spark DataFrame if successful, None if failed
        """
        try:
            # First, read the CSV with all columns
            df = self.spark.read \
                .option("header", "true") \
                .option("delimiter", "\t") \
                .option("quote", '"') \
                .option("escape", '"') \
                .option("multiLine", "true") \
                .csv(file_path)

            # Select and rename only the columns we need
            df_selected = df.select(
                col("code"),
                col("product_name"),
                col("brands"),
                col("categories"),
                col("nutriscore_score").cast("integer"),
                col("countries")
            )

            self.logger.info(f"Successfully loaded data with {df_selected.count()} rows")
            return df_selected

        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            return None

    def clean_data(self, df) -> SparkSession:
        """Clean the dataset by handling missing values and invalid entries."""
        self.logger.info("Starting data cleaning process...")

        # Handle missing values and strip whitespace
        df_cleaned = df.select([
            col("code"),
            when(col("product_name").isNull() | (col("product_name") == ""), "Unknown")
                .otherwise(trim(col("product_name"))).alias("product_name"),
            when(col("brands").isNull() | (col("brands") == ""), "Unknown")
                .otherwise(trim(col("brands"))).alias("brands"),
            when(col("categories").isNull() | (col("categories") == ""), "Uncategorized")
                .otherwise(trim(col("categories"))).alias("categories"),
            when(col("nutriscore_score").isNull(), -1)
                .otherwise(col("nutriscore_score")).alias("nutriscore_score"),
            when(col("countries").isNull() | (col("countries") == ""), "Unknown")
                .otherwise(trim(col("countries"))).alias("countries")
        ])

        # Remove duplicates
        df_cleaned = df_cleaned.dropDuplicates(["code"])

        self.logger.info("Data cleaning completed")
        return df_cleaned

    def analyze_french_products(self, df) -> SparkSession:
        """Analyze products from France and their nutriscores."""
        # Filter for French products
        french_products = df.filter(
            lower(col("countries")).contains("france")
        )

        # Group by brand and calculate statistics
        brand_analysis = french_products.groupBy("brands") \
            .agg(
                avg("nutriscore_score").alias("avg_nutriscore"),
                count("*").alias("product_count")
            ) \
            .filter(col("brands") != "Unknown") \
            .orderBy(desc("product_count"))

        return brand_analysis

    def save_results(self, df, output_path: str) -> None:
        """Save processed data to parquet format."""
        try:
            df.write.mode("overwrite").parquet(output_path)
            self.logger.info(f"Successfully saved results to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving results: {str(e)}")

    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
            self.logger.info("SparkSession stopped")

def main():
    processor = OpenFoodFactsProcessor()

    try:
        # Load and process data
        file_path = "/Users/mac/Downloads/en.openfoodfacts.org.products.csv"
        df = processor.load_data(file_path)

        if df is not None:
            # Clean data
            df_cleaned = processor.clean_data(df)

            # Analyze French products
            brand_analysis = processor.analyze_french_products(df_cleaned)

            # Display results
            print("\nBrand Analysis (Top 20 brands by product count in France):")
            brand_analysis.show(20, truncate=False)

            # Save results
            processor.save_results(brand_analysis, "./afterData.parquet")

    except Exception as e:
        processor.logger.error(f"Error in main process: {str(e)}")

    finally:
        processor.cleanup()

if __name__ == "__main__":
    main()
