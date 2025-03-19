import os
import glob
from datetime import datetime
import pandas as pd
from fastapi import FastAPI, HTTPException, Query

apps = FastAPI()

# Define required columns for each file type
REQUIRED_COLUMNS = {
    "products": ["product_id", "product_name", "category", "price", "stock_quantity", "reorder_level", "supplier_id"],
    "customers": ["customer_id", "name", "city", "email", "phone_number", "loyalty_tier"],  
    "suppliers": ["supplier_id", "supplier_name", "contact_details", "region"]
}

# Base directory where CSV files are stored
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "SampleData"))

def get_file(file_type: str, filename: str = None) :
    """Retrieve the latest or specific file of the given type."""
    files = glob.glob(os.path.join(base_dir, f"{file_type}_*.csv"))
    
    if not files:
        raise HTTPException(status_code=404, detail=f"No valid {file_type} CSV file found.")

    today = datetime.today().strftime("%Y%m%d")
    latest_file, latest_date = None, "00000000"

    for file in files:
        base_filename = os.path.basename(file)
        file_date = base_filename[-12:-4]  # Extract date (YYYYMMDD)

        # Validate date format
        if not (file_date.isdigit() and len(file_date) == 8):
            continue  # Skip files with invalid date format

        # Skip future-dated files
        if file_date > today:
            continue

        # If a specific filename is provided, return it if it exists
        if filename and base_filename == filename:
            return file

        # Otherwise, track the latest file
        if file_date > latest_date:
            latest_file, latest_date = file, file_date

    if filename and latest_file is None:
        raise HTTPException(status_code=404, detail=f"File {filename} not found.")
    
    if latest_file is None:
        raise HTTPException(status_code=404, detail=f"No valid {file_type} CSV file found.")

    return latest_file

def read_csv_file(file_path: str, file_type: str) -> pd.DataFrame:
    """Read and validate a CSV file."""
    try:
        df = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=500, detail=f"CSV file {file_path} is empty.")
    except pd.errors.ParserError:
        raise HTTPException(status_code=500, detail=f"CSV file {file_path} is corrupted or improperly formatted.")
   
    missing_columns = set(REQUIRED_COLUMNS[file_type]) - set(df.columns)
    if missing_columns:
        raise HTTPException(status_code=500, detail=f"CSV file {file_path} is missing required columns: {missing_columns}.")

    if df[REQUIRED_COLUMNS[file_type]].isnull().any().any():
        raise HTTPException(status_code=500, detail=f"CSV file {file_path} contains missing values in required columns.")

    return df

# API endpoint to retrieve product data
@apps.get("/products")
def get_products(filename: str = Query(None)):
    file_path = get_file("products", filename)
    df = read_csv_file(file_path, "products")
    return {"status": 200, "data": df.to_dict(orient="records")}

# API endpoint to retrieve customers data
@apps.get("/customers")
def get_customers(filename: str = Query(None)):
    file_path = get_file("customers", filename)
    df = read_csv_file(file_path, "customers")

    # Remove 'loyalty_tier' column if it exists
    df = df.drop(columns=["loyalty_tier"], errors="ignore")

    return {"status": 200, "data": df.to_dict(orient="records")}

# API endpoint to retrieve suppliers data
@apps.get("/suppliers")
def get_suppliers(filename: str = Query(None)):
    file_path = get_file("suppliers", filename)
    df = read_csv_file(file_path, "suppliers")
    return {"status": 200, "data": df.to_dict(orient="records")}
