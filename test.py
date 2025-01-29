import pandas as pd

file_path = '../processed_data.parquet/part-00000-ccb19bc8-e101-40e3-a805-acf12497e926-c000.snappy.parquet'
data = pd.read_parquet(file_path)

# Display the first few rows to understand the structure of the file
print(data.head())
