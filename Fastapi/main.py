import io
from datetime import  timedelta, datetime
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, status,Query
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from utils import (
    get_current_active_user,
    authenticate_user,
    create_access_token,
    User,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    fake_users_db
)
from dotenv import load_dotenv
from os import environ as env
load_dotenv()
# Initialize FastAPI app
app = FastAPI()

# Authentication models and utils
class Token(BaseModel):
    """Represents an access token returned after successful login."""

    access_token: str
    token_type: str



# GCS Configuration
GCS_BUCKET_NAME = "meta-morph-flow"
GCS_CREDENTIALS_PATH = env["GCS_CREDENTIALS_PATH"]

def get_gcs_client():
    """
    Returns an authenticated Google Cloud Storage client.
    Raises:
        HTTPException: If authentication fails.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GCS_CREDENTIALS_PATH
        )
        return storage.Client(credentials=credentials)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GCS client error: {str(e)}")

def get_latest_file_from_gcs(file_keyword: str, date_str: str):
    """
    Retrieves the latest blob from GCS for the given file keyword and date.

    Args:
        file_keyword (str): Prefix of the file name (e.g., 'product').
        date_str (str): Date in YYYYMMDD format.

    Returns:
        Blob: GCS blob object.

    Raises:
        HTTPException: If file is not found or GCS access fails.
    """
    try:
    
        client = get_gcs_client()
        bucket = client.get_bucket(GCS_BUCKET_NAME)
        blob_path = f"{date_str}/{file_keyword}_{date_str}.csv"
        blob = bucket.blob(blob_path)

        
        
        if not blob.exists():
            raise HTTPException(status_code=404, detail=f"No file found for date: {date_str}")
        return blob
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GCS access error: {str(e)}")

def read_csv_from_gcs(blob):
    """
    Downloads and reads CSV data from a GCS blob.

    Args:
        blob: The GCS blob object.

    Returns:
        pd.DataFrame: DataFrame containing CSV data.

    Raises:
        HTTPException: If CSV download or parsing fails.
    """
    try:
        content = blob.download_as_string()
        return pd.read_csv(io.StringIO(content.decode('utf-8')))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CSV read error: {str(e)}")


# API Endpoints for v1
@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Authenticates the user and returns an access token.

    Args:
        form_data: OAuth2 login form data with username and password.

    Returns:
        Token: JWT token and token type.

    Raises:
        HTTPException: If authentication fails.
    """
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(
        data={"sub": user.username},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return {"access_token": access_token, "token_type": "bearer"}

# --------------------------------------------------- --------------------------------------------------- ---------------------------------------------------
# ####################################/v1/ Endpoints - Decommissioned as part of MM-163####################################################
# --------------------------------------------------- --------------------------------------------------- ---------------------------------------------------

# today_date = datetime.now().strftime("%Y%m%d")

# @app.get("/v1/products")
# def get_products():
#  Returns product data from GCS for the given date and  dict: Product data as list of dictionaries.


#     blob = get_latest_file_from_gcs("product",today_date) 
#     df = read_csv_from_gcs(blob)
#     return {"status": 200, "data": df.to_dict(orient="records")}
  
# @app.get("/v1/customers")
# def get_customers(current_user: User = Depends(get_current_active_user)):
#  Returns customer data from GCS for the given date and  dict: Product data as list of dictionaries.

#     blob = get_latest_file_from_gcs("customer",today_date)
#     df = read_csv_from_gcs(blob)
#     return {"status": 200, "data": df.to_dict(orient="records")}

# @app.get("/v1/suppliers")
# def get_suppliers():
#  Returns suppliers data from GCS for the given date and dict: Product data as list of dictionaries.

#     blob = get_latest_file_from_gcs("supplier",today_date)
#     df = read_csv_from_gcs(blob)
#     return {"status": 200, "data": df.to_dict(orient="records")}


# API Endpoints for v2
@app.get("/v2/products")
def get_products(date: str = Query(default=None)):
    """
    Returns product data from GCS for the given date.

    Query Parameters:
        date (str): Date string in YYYYMMDD format (optional).

    Returns:
        dict: Product data as list of dictionaries.
    """

    if date is None:
            date = datetime.now().strftime("%Y%m%d")
        
    try:
        blob = get_latest_file_from_gcs("product", date)
        df = read_csv_from_gcs(blob)
        return {
            "status": 200,
            "file_date": date,
            "data": df.to_dict(orient="records")
        }
    except HTTPException as e:
        raise e
  
@app.get("/v2/customers")
def get_customers(current_user: User = Depends(get_current_active_user),date: str = Query(default=None)):
    """
    Returns customer data from GCS for the given date.
    Requires authentication.

    Query Parameters:
        date (str): Date string in YYYYMMDD format (optional).

    Returns:
        dict: Customer data as list of dictionaries.
    """
    if date is None:
        date = datetime.now().strftime("%Y%m%d")

    try:
        blob = get_latest_file_from_gcs("customer", date)
        df = read_csv_from_gcs(blob)
        return {
            "status": 200,
            "file_date": date,
            "data": df.to_dict(orient="records")
        }
    except HTTPException as e:
        raise e

@app.get("/v2/suppliers")
def get_suppliers(date: str = Query(default=None)):
    """
    Returns supplier data from GCS for the given date.

    Query Parameters:
        date (str): Date string in YYYYMMDD format (optional).

    Returns:
        dict: Supplier data as list of dictionaries.
    """
    if date is None:
        date = datetime.now().strftime("%Y%m%d")

    try:
        blob = get_latest_file_from_gcs("supplier", date)
        df = read_csv_from_gcs(blob)
        return {
            "status": 200,
            "file_date": date,
            "data": df.to_dict(orient="records")
        }
    except HTTPException as e:
        raise e