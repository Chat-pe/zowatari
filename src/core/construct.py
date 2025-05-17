"""Construct function for Zowatari."""

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from zowatari.core.cement import get_cement
from zowatari.db.models import Cement as CementDB
from zowatari.db.models import Construct as ConstructDB
from zowatari.db.models import ConstructCement
from zowatari.models.base import ConstructConfig
from zowatari.utils.logging import get_logger

logger = get_logger()

# Registry to store all construct functions
CONSTRUCT_REGISTRY: Dict[str, Dict[str, Any]] = {}


def construct(
    name: str,
    description: Optional[str] = None,
    cement_order: List[Tuple[str, int]] = None,
    tags: Optional[List[str]] = None,
) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Create a construct function.
    
    A construct function wraps multiple cement functions and executes them in order.
    
    Args:
        name: Name of the construct function.
        description: Description of the construct function.
        cement_order: List of tuples (cement_name, order) to execute.
        tags: Tags for the construct function.
        
    Returns:
        A callable construct function.
    """
    cement_order = cement_order or []
    tags = tags or []
    
    # Validate cement order
    for cement_name, _ in cement_order:
        try:
            get_cement(cement_name)
        except ValueError as e:
            raise ValueError(f"Error in construct {name}: {str(e)}")
    
    # Create construct function
    def construct_func(context: Dict[str, Any] = None) -> Dict[str, Any]:
        context = context or {}
        logger.info(f"Executing construct: {name}")
        
        # Sort cements by order
        sorted_cements = sorted(cement_order, key=lambda x: x[1])
        
        # Execute cements
        for cement_name, order in sorted_cements:
            cement_data = get_cement(cement_name)
            cement_func = cement_data["function"]
            
            # Execute cement
            logger.info(f"Executing cement {cement_name} in construct {name}")
            context = cement_func(context)
        
        logger.info(f"Construct {name} executed successfully")
        return context
    
    # Register construct function
    CONSTRUCT_REGISTRY[name] = {
        "function": construct_func,
        "description": description,
        "cement_order": cement_order,
        "tags": tags,
    }
    
    logger.info(f"Registered construct: {name}")
    
    return construct_func


def register_construct_in_db(
    session,
    name: str,
    description: Optional[str] = None,
    cement_order: List[Tuple[str, int]] = None,
    tags: Optional[List[str]] = None,
) -> ConstructDB:
    """Register a construct function in the database.
    
    Args:
        session: SQLAlchemy session.
        name: Name of the construct function.
        description: Description of the construct function.
        cement_order: List of tuples (cement_name, order) to execute.
        tags: Tags for the construct function.
        
    Returns:
        The registered construct function.
    """
    cement_order = cement_order or []
    tags = tags or []
    
    # Check if construct already exists
    construct = session.query(ConstructDB).filter_by(name=name).first()
    
    if construct:
        # Update existing construct
        if description is not None:
            construct.description = description
        if tags is not None:
            construct.tags = tags
        construct.updated_at = datetime.utcnow()
        
        # Delete existing cement relations
        session.query(ConstructCement).filter_by(construct_id=construct.id).delete()
    else:
        # Create new construct
        construct = ConstructDB(
            name=name,
            description=description,
            tags=tags,
        )
        session.add(construct)
        session.flush()
    
    # Add cement relations
    for cement_name, order in cement_order:
        # Get cement
        cement = session.query(CementDB).filter_by(name=cement_name).first()
        if not cement:
            raise ValueError(f"Cement {cement_name} not found in database")
        
        # Create relation
        construct_cement = ConstructCement(
            construct_id=construct.id,
            cement_id=cement.id,
            order=order,
        )
        session.add(construct_cement)
    
    session.commit()
    return construct


def get_construct(name: str) -> Dict[str, Any]:
    """Get a construct function from the registry.
    
    Args:
        name: Name of the construct function.
        
    Returns:
        The construct function data.
        
    Raises:
        ValueError: If the construct function does not exist.
    """
    if name not in CONSTRUCT_REGISTRY:
        raise ValueError(f"Construct {name} does not exist")
        
    return CONSTRUCT_REGISTRY[name]


def list_constructs() -> List[str]:
    """List all construct functions in the registry.
    
    Returns:
        List of construct function names.
    """
    return list(CONSTRUCT_REGISTRY.keys())