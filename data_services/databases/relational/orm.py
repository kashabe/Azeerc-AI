# orm.py
from datetime import datetime
from typing import Any, Dict, List, Optional
import uuid
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.dialects.postgresql import UUID, JSONB

Base = declarative_base()
Session = orm.scoped_session(orm.sessionmaker())

class AuditMixin:
    created_at = sa.Column(sa.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = sa.Column(sa.DateTime, default=datetime.utcnow, 
                         onupdate=datetime.utcnow, nullable=False)
    version_id = sa.Column(sa.Integer, default=1, nullable=False)
    __versioned__ = {}
    
class SoftDeleteMixin:
    is_deleted = sa.Column(sa.Boolean, default=False, nullable=False)
    
    @classmethod
    def default_query(cls, session):
        return session.query(cls).filter(cls.is_deleted == False)

class BaseModel(Base, AuditMixin, SoftDeleteMixin):
    __abstract__ = True
    id = sa.Column(UUID(as_uuid=True), primary_key=True, 
                  default=uuid.uuid4, unique=True)
    
    @hybrid_property
    def public_id(self) -> str:
        return str(self.id)
    
    @classmethod
    def get(cls, _id: uuid.UUID) -> Optional[Base]:
        return Session().query(cls).filter(cls.id == _id).first()
    
    @classmethod
    def find_by(cls, **kwargs) -> List[Base]:
        query = Session().query(cls)
        for key, value in kwargs.items():
            query = query.filter(getattr(cls, key) == value)
        return query.all()
    
    def save(self):
        session = Session()
        session.add(self)
        session.commit()
        return self
    
    def delete(self, permanent: bool = False):
        session = Session()
        if permanent:
            session.delete(self)
        else:
            self.is_deleted = True
            session.add(self)
        session.commit()

class AuditLog(BaseModel):
    __tablename__ = "audit_logs"
    action = sa.Column(sa.String(20), nullable=False)
    model_type = sa.Column(sa.String(50), nullable=False)
    model_id = sa.Column(UUID(as_uuid=True), nullable=False)
    user_id = sa.Column(UUID(as_uuid=True), sa.ForeignKey("users.id"))
    diff = sa.Column(JSONB)
    
class VersionedModel(BaseModel):
    __abstract__ = True
    __versioned__ = {
        'base_classes': (BaseModel,),
        'version_cls': AuditLog
    }
    
    def snapshot(self) -> Dict:
        return {
            'model_type': self.__class__.__name__,
            'model_id': self.id,
            'diff': self.get_diff()
        }
    
    def get_diff(self) -> Dict:
        state = orm.object_session(self).query(self.__class__).filter_by(id=self.id).first()
        return {col.name: getattr(state, col.name) for col in self.__table__.columns}

class CachedQuery(orm.Query):
    _cache = {}
    
    def __init__(self, entities, **kwargs):
        super().__init__(entities, **kwargs)
        self._cache_key = None
        
    def cache(self, key: str, ttl: int = 300):
        self._cache_key = f"query:{key}"
        return self
    
    def __iter__(self):
        if self._cache_key and self._cache_key in CachedQuery._cache:
            return iter(CachedQuery._cache[self._cache_key])
        result = super().__iter__()
        if self._cache_key:
            CachedQuery._cache[self._cache_key] = list(result)
        return result

class TenantMixin:
    tenant_id = sa.Column(UUID(as_uuid=True), nullable=False)
    
class FilterMixin:
    FILTER_PARAMS = {}
    
    @classmethod
    def apply_filters(cls, query, params: Dict):
        for key, value in params.items():
            if key in cls.FILTER_PARAMS:
                field, operator = cls.FILTER_PARAMS[key]
                query = query.filter(getattr(field, operator)(value))
        return query

class PaginatedQuery:
    def __init__(self, query, page: int, per_page: int):
        self.query = query
        self.page = page
        self.per_page = per_page
        
    @property
    def items(self):
        return self.query.offset((self.page - 1) * self.per_page).limit(self.per_page).all()
    
    @property
    def total(self):
        return self.query.order_by(None).count()
    
    @property
    def pages(self):
        return (self.total + self.per_page - 1) // self.per_page

# Example Entity Implementation
class Agent(VersionedModel, TenantMixin):
    __tablename__ = "agents"
    name = sa.Column(sa.String(100), index=True, nullable=False)
    status = sa.Column(sa.String(20), default="idle", nullable=False)
    config = sa.Column(JSONB, default={})
    last_heartbeat = sa.Column(sa.DateTime)
    
    tasks = orm.relationship("Task", backref="agent", lazy="dynamic")
    
    __mapper_args__ = {
        'version_id_col': version_id,
        'version_id_generator': lambda version: version + 1
    }
    
    @hybrid_property
    def active(self) -> bool:
        return self.status != "terminated"
    
    @active.expression
    def active(cls):
        return cls.status != "terminated"
    
    __table_args__ = (
        sa.Index('ix_agent_tenant_status', 'tenant_id', 'status'),
        sa.CheckConstraint("status IN ('idle', 'busy', 'terminated')", 
                          name="ck_agent_status")
    )

# Event Listeners
@sa.event.listens_for(Agent, 'after_update')
def log_agent_change(mapper, connection, target):
    diff = target.get_diff()
    audit = AuditLog(
        action="UPDATE",
        model_type=target.__class__.__name__,
        model_id=target.id,
        diff=diff
    )
    Session().add(audit)

# Security Mixin
class EncryptedFieldMixin:
    _encrypt_key = None
    
    @hybrid_property
    def secret_field(self):
        return decrypt(self._secret_field, self._encrypt_key)
    
    @secret_field.setter
    def secret_field(self, value):
        self._secret_field = encrypt(value, self._encrypt_key)
