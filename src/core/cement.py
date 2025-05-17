"""Cement function for Zowatari."""

import inspect
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

from pydantic import BaseModel

from zowatari.core.pebble import get_pebble
from zowatari.db.models import Cement as CementDB
from zowatari.db.models import CementInstruction as CementInstructionDB
from zowatari.db.models import Pebble as PebbleDB
from zowatari.models.base import CementInstruction
from zowatari.utils.logging import get_logger

logger = get_logger()

# Registry to store all cement functions
CEMENT_REGISTRY: Dict[str, Dict[str, Any]] = {}


def cement(
    name: str,
    description: Optional[str] = None,
    pebble_instructions: List[CementInstruction] = None,
) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Create a cement function.
    
    A cement function wraps multiple pebble functions and executes them in order.
    
    Args:
        name: Name of the cement function.
        description: Description of the cement function.
        pebble_instructions: List of pebble instructions.
        
    Returns:
        A callable cement function.
    """
    pebble_instructions = pebble_instructions or []
    
    # Validate pebble instructions
    for instruction in pebble_instructions:
        try:
            get_pebble(instruction.pebble_name)
        except ValueError as e:
            raise ValueError(f"Error in cement {name}: {str(e)}")
    
    # Create cement function
    def cement_func(context: Dict[str, Any] = None) -> Dict[str, Any]:
        context = context or {}
        logger.info(f"Executing cement: {name}")
        
        # Sort instructions by order
        sorted_instructions = sorted(
            pebble_instructions, key=lambda x: x.order
        )
        
        # Execute instructions
        for instruction in sorted_instructions:
            pebble_data = get_pebble(instruction.pebble_name)
            pebble_func = pebble_data["function"]
            
            # Prepare parameters
            parameters = {}
            for param_name, param_value in instruction.parameters.items():
                if isinstance(param_value, str) and param_value.startswith("$"):
                    # Get value from context
                    context_key = param_value[1:]
                    if context_key in context:
                        parameters[param_name] = context[context_key]
                    else:
                        raise ValueError(
                            f"Context key {context_key} not found for parameter {param_name}"
                        )
                else:
                    parameters[param_name] = param_value
            
            # Check dependencies
            for dependency in instruction.depends_on:
                if dependency not in context:
                    raise ValueError(
                        f"Dependency {dependency} not found in context for pebble {instruction.pebble_name}"
                    )
            
            # Execute pebble
            logger.info(f"Executing pebble {instruction.pebble_name} in cement {name}")
            result = pebble_func(**parameters)
            
            # Add result to context
            if isinstance(result, BaseModel):
                # Convert pydantic model to dict
                result_dict = result.model_dump()
                context.update(result_dict)
            elif isinstance(result, dict):
                context.update(result)
            else:
                # Add result with pebble name as key
                context[instruction.pebble_name] = result
        
        logger.info(f"Cement {name} executed successfully")
        return context
    
    # Register cement function
    CEMENT_REGISTRY[name] = {
        "function": cement_func,
        "description": description,
        "pebble_instructions": pebble_instructions,
    }
    
    logger.info(f"Registered cement: {name}")
    
    return cement_func


def register_cement_in_db(
    session,
    name: str,
    description: Optional[str] = None,
    pebble_instructions: List[CementInstruction] = None,
) -> CementDB:
    """Register a cement function in the database.
    
    Args:
        session: SQLAlchemy session.
        name: Name of the cement function.
        description: Description of the cement function.
        pebble_instructions: List of pebble instructions.
        
    Returns:
        The registered cement function.
    """
    pebble_instructions = pebble_instructions or []
    
    # Check if cement already exists
    cement = session.query(CementDB).filter_by(name=name).first()
    
    if cement:
        # Update existing cement
        if description is not None:
            cement.description = description
        cement.updated_at = datetime.utcnow()
        
        # Delete existing instructions
        session.query(CementInstructionDB).filter_by(cement_id=cement.id).delete()
    else:
        # Create new cement
        cement = CementDB(
            name=name,
            description=description,
        )
        session.add(cement)
        session.flush()
    
    # Add instructions
    for instruction in pebble_instructions:
        # Get pebble
        pebble = session.query(PebbleDB).filter_by(name=instruction.pebble_name).first()
        if not pebble:
            raise ValueError(f"Pebble {instruction.pebble_name} not found in database")
        
        # Create instruction
        cement_instruction = CementInstructionDB(
            pebble_id=pebble.id,
            cement_id=cement.id,
            parameters=instruction.parameters,
            order=instruction.order,
            depends_on=instruction.depends_on,
        )
        session.add(cement_instruction)
    
    session.commit()
    return cement


def get_cement(name: str) -> Dict[str, Any]:
    """Get a cement function from the registry.
    
    Args:
        name: Name of the cement function.
        
    Returns:
        The cement function data.
        
    Raises:
        ValueError: If the cement function does not exist.
    """
    if name not in CEMENT_REGISTRY:
        raise ValueError(f"Cement {name} does not exist")
        
    return CEMENT_REGISTRY[name]


def list_cements() -> List[str]:
    """List all cement functions in the registry.
    
    Returns:
        List of cement function names.
    """
    return list(CEMENT_REGISTRY.keys())