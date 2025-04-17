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


