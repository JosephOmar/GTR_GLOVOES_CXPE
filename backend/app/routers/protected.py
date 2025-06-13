# app/routers/protected.py

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from app.database.database import get_session
from app.utils.jwt import verify_token
from app.models.user import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

router = APIRouter()

def get_current_user(token: str = Depends(oauth2_scheme), session: Session = Depends(get_session)):
    payload = verify_token(token)
    if payload is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    user = session.query(User).filter(User.username == payload["sub"]).first()
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user

@router.get("/protected-endpoint")
async def protected_endpoint(current_user: User = Depends(get_current_user)):
    return {"message": f"Hello {current_user.username}, you are authenticated!"}
