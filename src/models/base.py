"""Base models for Zowatari."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class PebbleMetadata(BaseModel):
    """Metadata for a pebble function."""
    
    name: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class CementInstruction(BaseModel):
    """Instructions for a cement function."""
    
    pebble_name: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    order: int
    depends_on: List[str] = Field(default_factory=list)


class ConstructConfig(BaseModel):
    """Configuration for a construct function."""
    
    name: str
    description: Optional[str] = None
    cement_instructions: List[CementInstruction]
    tags: List[str] = Field(default_factory=list)


class ExecutionLog(BaseModel):
    """Log for the execution of a pebble function."""
    
    pebble_name: str
    construct_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "pending"  # pending, running, completed, failed
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class PassConfig(BaseModel):
    """Configuration for a pass."""
    
    construct_name: str
    pass_type: str  # first_pass or scheduled_pass
    schedule: Optional[str] = None  # cron expression for scheduled_pass