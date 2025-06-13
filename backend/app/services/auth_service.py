# app/services/auth_service.py

from sqlalchemy.orm import Session
from app.models.user import User
from app.utils.security import verify_password
from app.utils.jwt import create_access_token

def authenticate_user(username: str, password: str, session: Session) -> str:
    user = session.query(User).filter(User.username == username).first()
    if user and verify_password(password, user.hashed_password):
        return create_access_token({"sub": user.username})
    return None