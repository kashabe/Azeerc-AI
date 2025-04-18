# oauth2_provider.py
import logging
import os
import secrets
import time
from datetime import datetime, timedelta
from typing import Annotated, Dict, List, Optional, Union

from fastapi import Depends, FastAPI, HTTPException, Request, Security, status
from fastapi.security import OAuth2AuthorizationCodeBearer, OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr, Field, ValidationError
from sqlalchemy import Column, String, create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger("OAuth2Provider")

#region Database Models
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(String(36), primary_key=True)
    email = Column(String(255), unique=True)
    hashed_password = Column(String(512))
    scopes = Column(String(1024))  # Space-separated scopes
    enabled = Column(String(1), default="1")

class Client(Base):
    __tablename__ = "clients"
    client_id = Column(String(48), primary_key=True)
    client_secret = Column(String(512))
    redirect_uris = Column(String(2048))  # Space-separated URIs
    grant_types = Column(String(1024))    # Space-separated grants
    scopes = Column(String(2048))
#endregion

#region Pydantic Models
class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: Optional[str] = None
    id_token: Optional[str] = None  # For OpenID Connect

class TokenData(BaseModel):
    sub: Optional[str] = None
    scopes: List[str] = []
    client_id: Optional[str] = None

class UserInfo(BaseModel):
    sub: str = Field(..., alias="id")
    email: EmailStr
    email_verified: bool = False
#endregion

# Configuration
class OAuth2Config:
    JWT_ALGORITHM = "RS256"
    JWT_EXPIRE_MINUTES = 30
    JWT_REFRESH_EXPIRE_DAYS = 7
    PKCE_CODE_VERIFIER_LENGTH = 128
    AUTH_CODE_EXPIRE_SECONDS = 300
    TOKEN_SECRET_ROTATION_HOURS = 24

    def __init__(self):
        self.private_key = self._load_private_key()
        self.public_key = self._load_public_key()

    def _load_private_key(self) -> str:
        # Load from HSM/Vault in production
        with open("private_key.pem") as f:
            return f.read()
    
    def _load_public_key(self) -> str:
        with open("public_key.pem") as f:
            return f.read()

config = OAuth2Config()
oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl="/authorize",
    tokenUrl="/token",
    scopes={
        "openid": "OpenID Connect",
        "profile": "User profile access",
        "email": "Email address"
    }
)

# Database Setup
engine = create_engine(os.getenv("DB_URI"))
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Azeerc AI OAuth2 Provider")

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def rate_limiter(request: Request, call_next):
    # Implement Redis-based rate limiting
    return await call_next(request)

#region Utility Functions
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_jwt(
    data: dict, 
    expires_delta: Optional[timedelta] = None,
    token_type: str = "access"
) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow(),
        "iss": os.getenv("ISSUER_URL"),
        "aud": os.getenv("AUDIENCE"),
        "typ": f"JWT/{token_type}"
    })
    return jwt.encode(to_encode, config.private_key, algorithm=config.JWT_ALGORITHM)

async def validate_token(
    token: Annotated[str, Depends(oauth2_scheme)],
    db: SessionLocal = Depends(get_db)
) -> TokenData:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, config.public_key, algorithms=[config.JWT_ALGORITHM])
        sub: str = payload.get("sub")
        if sub is None:
            raise credentials_exception
        token_scopes = payload.get("scopes", [])
        client_id = payload.get("cid")  # Client ID for token introspection
    except (JWTError, ValidationError):
        raise credentials_exception
    
    # Verify client exists
    client = db.execute(select(Client).where(Client.client_id == client_id)).scalar()
    if not client:
        raise credentials_exception
    
    return TokenData(sub=sub, scopes=token_scopes, client_id=client_id)
#endregion

#region OAuth2 Endpoints
@app.get("/authorize")
async def authorization_endpoint(
    response_type: str,
    client_id: str,
    redirect_uri: str,
    scope: Optional[str] = None,
    state: Optional[str] = None,
    code_challenge: Optional[str] = None,
    code_challenge_method: Optional[str] = "S256",
    db: SessionLocal = Depends(get_db)
):
    # Validate client
    client = db.execute(select(Client).where(Client.client_id == client_id)).scalar()
    if not client or redirect_uri not in client.redirect_uris.split():
        raise HTTPException(status_code=400, detail="Invalid client or redirect URI")

    # PKCE Validation
    if code_challenge and code_challenge_method not in ["S256", "plain"]:
        raise HTTPException(status_code=400, detail="Unsupported code challenge method")

    # Generate authorization code
    auth_code = secrets.token_urlsafe(64)
    code_data = {
        "sub": "user_id",  # Replace with actual user after login
        "client_id": client_id,
        "exp": datetime.utcnow() + timedelta(seconds=config.AUTH_CODE_EXPIRE_SECONDS),
        "code_challenge": code_challenge,
        "scopes": scope.split() if scope else []
    }
    signed_code = create_jwt(code_data, token_type="auth_code")

    # Return redirect with code
    return JSONResponse(
        status_code=302,
        headers={"Location": f"{redirect_uri}?code={signed_code}&state={state}"},
    )

@app.post("/token")
async def token_endpoint(
    grant_type: str,
    code: Optional[str] = None,
    redirect_uri: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    code_verifier: Optional[str] = None,
    db: SessionLocal = Depends(get_db)
):
    # Client authentication
    client = db.execute(select(Client).where(Client.client_id == client_id)).scalar()
    if not client or client.client_secret != client_secret:
        raise HTTPException(status_code=401, detail="Invalid client credentials")

    # Authorization Code Grant
    if grant_type == "authorization_code":
        if not code or not redirect_uri:
            raise HTTPException(status_code=400, detail="Missing parameters")
        
        try:
            payload = jwt.decode(code, config.public_key, algorithms=[config.JWT_ALGORITHM])
            # Verify code challenge (PKCE)
            if payload.get("code_challenge"):
                if not code_verifier:
                    raise HTTPException(status_code=400, detail="Code verifier required")
                # Validate code_verifier vs code_challenge
                # (Implement hashing logic here)
        except JWTError:
            raise HTTPException(status_code=400, detail="Invalid authorization code")

        # Issue tokens
        access_token = create_jwt(
            {"sub": payload["sub"], "scopes": payload["scopes"], "cid": client_id},
            expires_delta=timedelta(minutes=config.JWT_EXPIRE_MINUTES)
        )
        refresh_token = create_jwt(
            {"sub": payload["sub"], "cid": client_id},
            expires_delta=timedelta(days=config.JWT_REFRESH_EXPIRE_DAYS),
            token_type="refresh"
        )
        return Token(
            access_token=access_token,
            expires_in=config.JWT_EXPIRE_MINUTES*60,
            refresh_token=refresh_token
        )

    # Handle other grant types (client_credentials, refresh_token)
    raise HTTPException(status_code=400, detail="Unsupported grant type")

@app.get("/userinfo")
async def userinfo_endpoint(
    token_data: TokenData = Security(validate_token, scopes=["profile"])
):
    # Fetch user details from DB
    return UserInfo(id=token_data.sub, email="user@example.com")

@app.get("/.well-known/jwks.json")
async def jwks_endpoint():
    # Generate JWKS from public key
    return {
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": config.JWT_ALGORITHM,
            "kid": "current",
            "n": "...",  # Public key modulus
            "e": "AQAB"
        }]
    }
#endregion

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
