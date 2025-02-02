import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class OpenFoodFactsProcessor {
    private static final Logger logger = Logger.getLogger(OpenFoodFactsProcessor.class);
    private SparkSession spark;
    
    private SparkSession createSparkSession(String appName) {
        return SparkSession.builder()
                .appName(appName)
                .master("local[*]") // Run Spark locally using all available cores
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate();
    }

    public OpenFoodFactsProcessor(String appName) {
        setupLogger();
        this.spark = createSparkSession(appName);
    }

    private void setupLogger() {
        Logger.getLogger("org").setLevel(Level.WARN);
    }


    public Dataset<Row> loadData(String filePath) {
        try {
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("delimiter", "\t")
                    .option("quote", "\"")
                    .option("escape", "\"")
                    .option("multiLine", "true")
                    .csv(filePath);

            Dataset<Row> dfSelected = df.select(
                    functions.col("code"),
                    functions.col("product_name"),
                    functions.col("brands"),
                    functions.col("categories"),
                    functions.col("nutriscore_score").cast(DataTypes.IntegerType),
                    functions.col("countries")
            );

            logger.info("Successfully loaded data with " + dfSelected.count() + " rows");
            return dfSelected;

        } catch (Exception e) {
            logger.error("Error loading data: " + e.getMessage());
            return null;
        }
    }

    public Dataset<Row> cleanData(Dataset<Row> df) {
        logger.info("Starting data cleaning process...");

        Dataset<Row> dfCleaned = df.select(
                functions.col("code"),
                functions.when(functions.col("product_name").isNull()
                        .or(functions.col("product_name").equalTo("")), "Unknown")
                        .otherwise(functions.trim(functions.col("product_name"))).alias("product_name"),
                functions.when(functions.col("brands").isNull()
                        .or(functions.col("brands").equalTo("")), "Unknown")
                        .otherwise(functions.trim(functions.col("brands"))).alias("brands"),
                functions.when(functions.col("categories").isNull()
                        .or(functions.col("categories").equalTo("")), "Uncategorized")
                        .otherwise(functions.trim(functions.col("categories"))).alias("categories"),
                functions.when(functions.col("nutriscore_score").isNull(), -1)
                        .otherwise(functions.col("nutriscore_score")).alias("nutriscore_score"),
                functions.when(functions.col("countries").isNull()
                        .or(functions.col("countries").equalTo("")), "Unknown")
                        .otherwise(functions.trim(functions.col("countries"))).alias("countries")
        ).dropDuplicates("code");

        logger.info("Data cleaning completed");
        return dfCleaned;
    }

    public Dataset<Row> analyzeFrenchProducts(Dataset<Row> df) {
        Dataset<Row> frenchProducts = df.filter(functions.lower(functions.col("countries")).contains("france"));

        Dataset<Row> brandAnalysis = frenchProducts.groupBy("brands")
                .agg(
                        functions.avg("nutriscore_score").alias("avg_nutriscore"),
                        functions.count("*").alias("product_count")
                )
                .filter(functions.col("brands").notEqual("Unknown"))
                .orderBy(functions.desc("product_count"));

        return brandAnalysis;
    }

    public void saveResults(Dataset<Row> df, String outputPath) {
        try {
            df.write().mode(SaveMode.Overwrite).parquet(outputPath);
            logger.info("Successfully saved results to " + outputPath);
        } catch (Exception e) {
            logger.error("Error saving results: " + e.getMessage());
        }
    }

    public void cleanup() {
        if (spark != null) {
            spark.stop();
            logger.info("SparkSession stopped");
        }
    }

    public static void main(String[] args) {
        OpenFoodFactsProcessor processor = new OpenFoodFactsProcessor("OpenFoodFactsProcessor");

        try {
            String filePath = "/Users/mac/Downloads/en.openfoodfacts.org.products.csv";
            Dataset<Row> df = processor.loadData(filePath);

            if (df != null) {
                Dataset<Row> dfCleaned = processor.cleanData(df);
                Dataset<Row> brandAnalysis = processor.analyzeFrenchProducts(dfCleaned);

                System.out.println("\nBrand Analysis (Top 20 brands by product count in France):");
                brandAnalysis.show(20, false);

                processor.saveResults(brandAnalysis, "./afterData.parquet");
            }

        } catch (Exception e) {
            logger.error("Error in main process: " + e.getMessage());

        } finally {
            processor.cleanup();
        }
    }
}
