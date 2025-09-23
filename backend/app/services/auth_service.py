# app/services/auth_service.py

from sqlalchemy.orm import Session
from app.models.user import User
from app.utils.security import verify_password
from app.utils.jwt import create_access_token

def authenticate_user(email: str, password: str, session: Session) -> str:
    user = session.query(User).filter(User.email == email).first()
    if user and verify_password(password, user.password):
        return create_access_token({"sub": str(user.id)})
    return None
