import os
import glob
import pandas as pd
from fastapi import FastAPI, HTTPException, Query

app = FastAPI()

# Required columns for each file type
REQUIRED_COLUMNS = {
    "products": ["product_id", "product_name", "category", "price", "stock_quantity", "reorder_level", "supplier_id"],
    "customers": ["customer_id", "name", "city", "email", "phone_number", "loyalty_tier"],
    "suppliers": ["supplier_id", "supplier_name", "contact_details", "region"]
}

# Base directory for CSV files
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "SampleData"))

def get_latest_file(file_type: str, filename: str = None) :
    """Retrieve the latest available CSV file or a specific file if given."""
    files = glob.glob(os.path.join(BASE_DIR, f"{file_type}_*.csv"))
    if not files:
        raise HTTPException(status_code=404, detail=f"No {file_type} CSV files found.")

    if filename:
        file_path = os.path.join(BASE_DIR, filename)
        if file_path in files:
            return file_path
        raise HTTPException(status_code=404, detail=f"File {filename} not found.")

    # Get the latest file based on modification time
    return max(files, key=os.path.getmtime)

def read_csv(file_path: str, file_type: str) :
    """Reads a CSV file and validates its structure."""
    try:
        df = pd.read_csv(file_path, usecols=lambda col: col in REQUIRED_COLUMNS[file_type])
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")

    if df.isnull().any().any():
        raise HTTPException(status_code=500, detail="CSV contains missing values in required columns.")

    return df

@app.get("/products")
def get_products(filename: str = Query(None)):
    """API endpoint to retrieve product data."""
    file_path = get_latest_file("products", filename)
    df = read_csv(file_path, "products")
    return {"status": 200, "data": df.to_dict(orient="records")}

@app.get("/customers")
def get_customers(filename: str = Query(None)):
    """API endpoint to retrieve customer data."""
    file_path = get_latest_file("customers", filename)
    df = read_csv(file_path, "customers")

    # Remove 'loyalty_tier' column if it exists
    df.drop(columns=["loyalty_tier"], errors="ignore", inplace=True)
    
    return {"status": 200, "data": df.to_dict(orient="records")}

@app.get("/suppliers")
def get_suppliers(filename: str = Query(None)):
    """API endpoint to retrieve supplier data."""
    file_path = get_latest_file("suppliers", filename)
    df = read_csv(file_path, "suppliers")
    return {"status": 200, "data": df.to_dict(orient="records")}
