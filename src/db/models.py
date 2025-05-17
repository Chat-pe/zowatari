"""SQLAlchemy models for Zowatari."""

from datetime import datetime
from typing import Dict, List, Optional, Any

from sqlalchemy import (
    Column, 
    String, 
    Integer, 
    DateTime, 
    Text, 
    JSON, 
    ForeignKey,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class Pebble(Base):
    """Database model for a pebble function."""
    
    __tablename__ = "pebbles"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    tags = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Relationships
    cement_instructions = relationship("CementInstruction", back_populates="pebble")
    execution_logs = relationship("ExecutionLog", back_populates="pebble")


class Cement(Base):
    """Database model for a cement function."""
    
    __tablename__ = "cements"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Relationships
    instructions = relationship("CementInstruction", back_populates="cement")
    constructs = relationship(
        "Construct", 
        secondary="construct_cements", 
        back_populates="cements"
    )


class CementInstruction(Base):
    """Database model for a cement instruction."""
    
    __tablename__ = "cement_instructions"
    
    id = Column(Integer, primary_key=True)
    pebble_id = Column(Integer, ForeignKey("pebbles.id"), nullable=False)
    cement_id = Column(Integer, ForeignKey("cements.id"), nullable=False)
    parameters = Column(JSON, nullable=False, default=dict)
    order = Column(Integer, nullable=False)
    depends_on = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Relationships
    pebble = relationship("Pebble", back_populates="cement_instructions")
    cement = relationship("Cement", back_populates="instructions")


class Construct(Base):
    """Database model for a construct function."""
    
    __tablename__ = "constructs"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    tags = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Relationships
    cements = relationship(
        "Cement", 
        secondary="construct_cements", 
        back_populates="constructs"
    )
    passes = relationship("Pass", back_populates="construct")
    execution_logs = relationship("ExecutionLog", back_populates="construct")


class ConstructCement(Base):
    """Many-to-many relationship between constructs and cements."""
    
    __tablename__ = "construct_cements"
    
    construct_id = Column(
        Integer, ForeignKey("constructs.id"), primary_key=True
    )
    cement_id = Column(Integer, ForeignKey("cements.id"), primary_key=True)
    order = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Pass(Base):
    """Database model for a pass."""
    
    __tablename__ = "passes"
    
    id = Column(Integer, primary_key=True)
    construct_id = Column(Integer, ForeignKey("constructs.id"), nullable=False)
    pass_type = Column(String(50), nullable=False)  # first_pass or scheduled_pass
    schedule = Column(String(100), nullable=True)  # cron expression for scheduled_pass
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Relationships
    construct = relationship("Construct", back_populates="passes")
    execution_logs = relationship("ExecutionLog", back_populates="pass_")


class ExecutionLog(Base):
    """Database model for an execution log."""
    
    __tablename__ = "execution_logs"
    
    id = Column(Integer, primary_key=True)
    pebble_id = Column(Integer, ForeignKey("pebbles.id"), nullable=False)
    construct_id = Column(Integer, ForeignKey("constructs.id"), nullable=False)
    pass_id = Column(Integer, ForeignKey("passes.id"), nullable=False)
    start_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    status = Column(
        String(50), nullable=False, default="pending"
    )  # pending, running, completed, failed
    result = Column(JSON, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Relationships
    pebble = relationship("Pebble", back_populates="execution_logs")
    construct = relationship("Construct", back_populates="execution_logs")
    pass_ = relationship("Pass", back_populates="execution_logs")


def get_engine(connection_string):
    """Create a SQLAlchemy engine."""
    return create_engine(connection_string)


def get_session(engine):
    """Create a SQLAlchemy session."""
    Session = sessionmaker(bind=engine)
    return Session()


def init_db(connection_string):
    """Initialize the database."""
    engine = get_engine(connection_string)
    Base.metadata.create_all(engine)
    return engine