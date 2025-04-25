import os
import glob
import pandas as pd
from datetime import datetime, timedelta
from fastapi import FastAPI, Depends, HTTPException, status
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

apps = FastAPI()


class Token(BaseModel):
    access_token: str
    token_type: str

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","rest-api", "SampleData"))


def get_latest_file(file_type):
    files = glob.glob(os.path.join(base_dir, f"{file_type}_*.csv"))
    today = datetime.today().strftime("%Y%m%d")
    latest_file = None
    latest_date = "00000000"

    for file in files:
        filename = os.path.basename(file)
        file_date = filename[-12:-4]
        if file_date.isdigit() and len(file_date) == 8 and file_date <= today:
            if file_date > latest_date:
                latest_date, latest_file = file_date, file

    return latest_file

@apps.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
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

@apps.get("/products")
def get_products():
    file_path = get_latest_file("products")
    if not file_path:
        raise HTTPException(status_code=404, detail="No valid products CSV file found.")
    df = pd.read_csv(file_path)
    return {"status": 200, "data": df.to_dict(orient="records")}

@apps.get("/v1/customers")
def get_customers(current_user: User = Depends(get_current_active_user)):
    file_path = get_latest_file("customers")
    if not file_path:
        raise HTTPException(status_code=404, detail="No valid customers CSV file found.")
    df = pd.read_csv(file_path)
    df.drop(columns=["loyalty_tier"], errors="ignore", inplace=True)
    return {"status": 200, "data": df.to_dict(orient="records")}

@apps.get("/suppliers")
def get_suppliers():
    file_path = get_latest_file("suppliers")
    if not file_path:
        raise HTTPException(status_code=404, detail="No valid suppliers CSV file found.")
    df = pd.read_csv(file_path)
    return {"status": 200, "data": df.to_dict(orient="records")}