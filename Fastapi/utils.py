from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from typing import Optional
from jose import JWTError, jwt
from pydantic import BaseModel
from keys import KEY

# Secret key used to sign and verify JWT tokens 
SECRET_KEY = KEY
# Algorithm used for encoding/decoding JWT
ALGORITHM = "HS256"
# Token expiration time in minutes
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# OAuth2PasswordBearer is a class that extracts token from request headers
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Simulated database with a single user
fake_users_db = {
    "admin": {
        "username": "admin",
        "password": "adminpassword",
        "disabled": False,
    }
}

# Pydantic model for token data
class TokenData(BaseModel):
    """
    Model representing the data stored inside the JWT token.
    """
    username: Optional[str] = None

# Pydantic model representing a basic user
class User(BaseModel):
    """
    Public-facing user model (without password).
    """
    username: str
    disabled: Optional[bool] = None
# Pydantic model representing a user in the database (includes password)
class UserInDB(User):
    """
    Internal model used for authentication (includes password).
    """
    password: str

# Utility function to get a user object from the database by username
def get_user(db, username: str):
    """
    Retrieve a user from the database by username.

    Args:
        db (dict): Database dictionary.
        username (str): The username to search.

    Returns:
        UserInDB or None: The user object if found.
    """
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

# Function to authenticate the user by verifying username and password
def authenticate_user(db, username: str, password: str):
    """
    Verify the username and password against the database.

    Args:
        db (dict): Database dictionary.
        username (str): Username to check.
        password (str): Password to verify.

    Returns:
        UserInDB or False: The authenticated user or False if invalid.
    """
    user = get_user(db, username)
    if not user or user.password != password:
        return False
    return user

# Function to create a JWT access token with optional expiration time
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """
    Create a JWT access token.

    Args:
        data (dict): Data to encode in the token.
        expires_delta (timedelta, optional): Token validity duration.

    Returns:
        str: Encoded JWT token.
    """
    to_encode = data.copy()  # Create a copy of the data to encode
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=30))  # Set expiration
    to_encode.update({"exp": expire})  # Add expiration time to payload
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)  # Encode and return token

# Dependency function that retrieves the current user based on the token
async def get_current_user(token: str = Depends(oauth2_scheme)):
    """
    Extract and verify the user from a JWT token.

    Args:
        token (str): JWT token from Authorization header.

    Raises:
        HTTPException: If token is invalid or user is not found.

    Returns:
        User: The authenticated user.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # Decode the token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        # Extract username (subject) from token payload
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        # Raise an error if token is invalid or expired
        raise credentials_exception

    # Fetch the user from the database
    user = get_user(fake_users_db, username)
    if user is None:
        raise credentials_exception
    return user

# Dependency function that ensures the user is active (not disabled)
async def get_current_active_user(current_user: User = Depends(get_current_user)):
    """
    Verify the user is not disabled.

    Args:
        current_user (User): The authenticated user.

    Raises:
        HTTPException: If user is disabled.

    Returns:
        User: The active user.
    """
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
