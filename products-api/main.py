import pandas as pd
from fastapi import FastAPI, HTTPException
import os

# Create a FastAPI instance
app = FastAPI()

# Define the CSV File Path
CSV_FILE_PATH = "../SampleData/products_20250101.csv"



@app.get("/products")
def get_products_from_csv():
    # Checking if file exists
    if not os.path.exists(CSV_FILE_PATH):
        raise HTTPException(status_code=404, detail="CSV file not found")

    # Read CSV into a DataFrame
    df = pd.read_csv(CSV_FILE_PATH)

    # Check if the DataFrame is empty
    if df.empty:
        raise HTTPException(status_code=404, detail="No products found in the CSV file")

    # Convert DataFrame to a list of dictionaries
    products_list = df.to_dict(orient="records")

    return {
        "status": 200,
        "products": products_list  # Return the actual data
    }