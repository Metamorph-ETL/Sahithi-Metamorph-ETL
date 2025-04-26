from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from typing import Optional
from jose import JWTError, jwt
from pydantic import BaseModel
from keys import KEY

# Secret key used to sign the JWT token (stored securely in 'keys.py')
SECRET_KEY = KEY
ALGORITHM = "HS256"  # Algorithm used for encoding the JWT
ACCESS_TOKEN_EXPIRE_MINUTES = 30  # Token expiration time in minutes

# OAuth2 scheme for token-based authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Fake database to simulate a user store
fake_users_db = {
    "admin": {
        "username": "admin",
        "password": "adminpassword",
        "disabled": False,
    }
}

# Pydantic model for the token payload (only contains username for simplicity)
class TokenData(BaseModel):
    username: Optional[str] = None

# Base user model
class User(BaseModel):
    username: str
    disabled: Optional[bool] = None

# User model with password (used internally)
class UserInDB(User):
    password: str

# Utility to get a user from the database
def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

# Authenticates a user using username and password
def authenticate_user(db, username: str, password: str):
    user = get_user(db, username)
    if not user or user.password != password:
        return False
    return user

# Creates a JWT access token with expiration time
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Dependency that retrieves the current user from the JWT token
async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # Decode the JWT
