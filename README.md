# OpenFoodFacts Data Processing with Apache Spark

## 📌 Objective

The goal of this project is to demonstrate the ability to manipulate, clean, transform, and integrate data from the OpenFoodFacts dataset using Apache Spark in Python. The project is part of the **"Data Integration Workshop"** module and focuses on applying key Spark concepts.

## 📂 Project Structure

```
📦 OpenFoodFacts-Spark
├── 📁 data                 # Raw and processed datasets
├── 📁 src                  # Source code
│   ├── openfoodfacts_processor.py  # Main processing script
│   ├── utils.py            # Helper functions
├── 📁 output               # Processed data and reports
├── README.md               # Project documentation
├── requirements.txt        # Dependencies
└── .gitignore              # Ignore unnecessary files
```

## 📝 Dataset

Download the full OpenFoodFacts dataset:
🔗 [OpenFoodFacts Data](https://fr.openfoodfacts.org/data)

### Key Fields Used:
- `product_name` - Name of the product
- `brands` - Brand name
- `countries` - Countries where the product is sold
- `ingredients_text` - List of ingredients
- `nutriments` - Nutritional values (energy_100g, sugars_100g, fat_100g, etc.)
- `labels` - Quality labels
- `packaging` - Packaging details

## 🚀 Implementation Steps

### 1️⃣ Data Loading & Exploration
- Load the CSV file into a **Spark DataFrame**.
- Display schema and sample records.
- Handle potential errors (corrupt files, incomplete rows).

### 2️⃣ Data Cleaning
- Remove unusable rows (e.g., missing `product_name` or essential nutrients).
- Fill missing values with default values (e.g., `0` for numerical data).
- Standardize brand and country names.
- Remove duplicate entries.

### 3️⃣ Data Transformation
- Create a **new column `is_healthy`** based on sugar and fat content.
- Add an **`ingredient_count`** column to count ingredients.
- Filter products available in a specific country (e.g., France).

### 4️⃣ Data Aggregation & Analysis
- Identify the **top 10 most represented brands**.
- Compute the **average sugar and energy content per country**.
- Analyze the **distribution of products by quality labels**.

### 5️⃣ Export Results
- Save the cleaned and transformed data as a **CSV file**.
- Export aggregated results into separate **CSV files**.

## 📊 Expected Outputs
- **Cleaned dataset**
- **Aggregated results:**
  - Top brands
  - Nutrient statistics per country
  - Label distribution

## 🛠️ Setup & Execution

### 1️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```

### 2️⃣ Run the Processing Script
```bash
python src/openfoodfacts_processor.py
```

### 3️⃣ Check Output
Processed data will be stored in the `output/` folder.

## 🎯 Evaluation Criteria

### ✅ Code Quality (40%)
- Proper structuring and readability
- Exception handling and robustness

### ✅ Data Processing Accuracy (30%)
- Correctness of transformations and aggregations

### ✅ Documentation (20%)
- Clear and concise explanation of the approach
- Insights from results

### ✅ Team Organization (10%)
- Project structuring
- Task distribution and collaboration

## 📌 Resources
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [OpenFoodFacts Documentation](https://fr.openfoodfacts.org/data)

## 📅 Deadline
The project must be submitted **within one month** from the assignment date via the **Edensia platform**.

## 🔥 Tips for Success
- **Read the documentation** before starting.
- **Iterate in small steps**—implement and test each feature incrementally.
- **Use sample data** to validate transformations before processing the entire dataset.

