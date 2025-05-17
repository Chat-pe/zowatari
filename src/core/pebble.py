"""Pebble decorator for Zowatari."""

import functools
import inspect
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, cast

from pydantic import BaseModel

from zowatari.db.models import Pebble as PebbleDB
from zowatari.models.base import PebbleMetadata
from zowatari.utils.logging import get_logger

logger = get_logger()

# Registry to store all pebble functions
PEBBLE_REGISTRY: Dict[str, Dict[str, Any]] = {}

# Type variable for the function return type
T = TypeVar("T")


def pebble(
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to register a function as a pebble.
    
    Args:
        name: Name of the pebble. Defaults to the function name.
        description: Description of the pebble.
        tags: Tags for the pebble.
        
    Returns:
        The decorated function.
    """
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Get function signature
        sig = inspect.signature(func)
        
        # Get function name if not provided
        nonlocal name
        if name is None:
            name = func.__name__
            
        # Get function docstring if description not provided
        nonlocal description
        if description is None and func.__doc__:
            description = inspect.cleandoc(func.__doc__)
            
        # Set tags if not provided
        nonlocal tags
        if tags is None:
            tags = []
            
        # Create metadata
        metadata = PebbleMetadata(
            name=name,
            description=description,
            tags=tags,
        )
        
        # Register the pebble
        PEBBLE_REGISTRY[name] = {
            "function": func,
            "metadata": metadata,
            "signature": sig,
        }
        
        logger.info(f"Registered pebble: {name}")
        
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            start_time = time.time()
            logger.info(f"Executing pebble: {name}")
            
            try:
                # Execute the function
                result = func(*args, **kwargs)
                
                # Log execution time
                execution_time = time.time() - start_time
                logger.info(f"Pebble {name} executed in {execution_time:.4f}s")
                
                return result
            except Exception as e:
                # Log error
                logger.error(f"Error executing pebble {name}: {str(e)}")
                raise
                
        return wrapper
    
    return decorator


def register_pebble_in_db(
    session, name: str, description: Optional[str] = None, tags: Optional[List[str]] = None
) -> PebbleDB:
    """Register a pebble in the database.
    
    Args:
        session: SQLAlchemy session.
        name: Name of the pebble.
        description: Description of the pebble.
        tags: Tags for the pebble.
        
    Returns:
        The registered pebble.
    """
    # Check if pebble already exists
    pebble = session.query(PebbleDB).filter_by(name=name).first()
    
    if pebble:
        # Update existing pebble
        if description is not None:
            pebble.description = description
        if tags is not None:
            pebble.tags = tags
        pebble.updated_at = datetime.utcnow()
    else:
        # Create new pebble
        pebble = PebbleDB(
            name=name,
            description=description,
            tags=tags or [],
        )
        session.add(pebble)
        
    session.commit()
    return pebble


def get_pebble(name: str) -> Dict[str, Any]:
    """Get a pebble from the registry.
    
    Args:
        name: Name of the pebble.
        
    Returns:
        The pebble.
        
    Raises:
        ValueError: If the pebble does not exist.
    """
    if name not in PEBBLE_REGISTRY:
        raise ValueError(f"Pebble {name} does not exist")
        
    return PEBBLE_REGISTRY[name]


def list_pebbles() -> List[str]:
    """List all pebbles in the registry.
    
    Returns:
        List of pebble names.
    """
    return list(PEBBLE_REGISTRY.keys())