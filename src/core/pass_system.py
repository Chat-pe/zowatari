"""Pass system for Zowatari."""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from zowatari.core.construct import get_construct
from zowatari.db.models import Construct as ConstructDB
from zowatari.db.models import ExecutionLog as ExecutionLogDB
from zowatari.db.models import Pass as PassDB
from zowatari.db.models import Pebble as PebbleDB
from zowatari.models.base import PassConfig
from zowatari.utils.logging import get_logger

logger = get_logger()


def first_pass(
    construct_name: str, context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Run a first pass on a construct.
    
    A first pass is a one-time execution of a construct.
    
    Args:
        construct_name: Name of the construct to run.
        context: Initial context for the construct.
        
    Returns:
        The final context after execution.
    """
    context = context or {}
    logger.info(f"Starting first pass for construct: {construct_name}")
    
    try:
        # Get construct function
        construct_data = get_construct(construct_name)
        construct_func = construct_data["function"]
        
        # Execute construct
        result = construct_func(context)
        logger.info(f"First pass for construct {construct_name} completed successfully")
        return result
    except Exception as e:
        logger.error(f"Error in first pass for construct {construct_name}: {str(e)}")
        raise


def scheduled_pass(
    construct_name: str, schedule: str, context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Run a scheduled pass on a construct.
    
    A scheduled pass is a recurring execution of a construct based on a schedule.
    
    Args:
        construct_name: Name of the construct to run.
        schedule: Cron expression for the schedule.
        context: Initial context for the construct.
        
    Returns:
        The final context after execution.
        
    Notes:
        This function does not actually schedule the execution. It just runs the
        construct and records the schedule in the database. The scheduling should
        be handled by an external scheduler like Airflow or Prefect.
    """
    context = context or {}
    logger.info(
        f"Starting scheduled pass for construct: {construct_name} with schedule: {schedule}"
    )
    
    try:
        # Get construct function
        construct_data = get_construct(construct_name)
        construct_func = construct_data["function"]
        
        # Execute construct
        result = construct_func(context)
        logger.info(
            f"Scheduled pass for construct {construct_name} completed successfully"
        )
        return result
    except Exception as e:
        logger.error(
            f"Error in scheduled pass for construct {construct_name}: {str(e)}"
        )
        raise


def register_pass_in_db(
    session: Session, construct_name: str, pass_type: str, schedule: Optional[str] = None
) -> PassDB:
    """Register a pass in the database.
    
    Args:
        session: SQLAlchemy session.
        construct_name: Name of the construct to run.
        pass_type: Type of pass (first_pass or scheduled_pass).
        schedule: Cron expression for the schedule (only for scheduled_pass).
        
    Returns:
        The registered pass.
        
    Raises:
        ValueError: If the construct does not exist or the pass type is invalid.
    """
    # Validate pass type
    if pass_type not in ["first_pass", "scheduled_pass"]:
        raise ValueError(f"Invalid pass type: {pass_type}")
    
    # Validate schedule
    if pass_type == "scheduled_pass" and not schedule:
        raise ValueError("Schedule is required for scheduled_pass")
    
    # Get construct
    construct = session.query(ConstructDB).filter_by(name=construct_name).first()
    if not construct:
        raise ValueError(f"Construct {construct_name} not found in database")
    
    # Create pass
    pass_ = PassDB(
        construct_id=construct.id,
        pass_type=pass_type,
        schedule=schedule,
    )
    session.add(pass_)
    session.commit()
    
    return pass_


def log_execution(
    session: Session,
    pebble_name: str,
    construct_name: str,
    pass_id: int,
    status: str,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> ExecutionLogDB:
    """Log the execution of a pebble in a construct.
    
    Args:
        session: SQLAlchemy session.
        pebble_name: Name of the pebble.
        construct_name: Name of the construct.
        pass_id: ID of the pass.
        status: Status of the execution (pending, running, completed, failed).
        result: Result of the execution.
        error: Error message if the execution failed.
        
    Returns:
        The execution log.
    """
    # Get pebble
    pebble = session.query(PebbleDB).filter_by(name=pebble_name).first()
    if not pebble:
        raise ValueError(f"Pebble {pebble_name} not found in database")
    
    # Get construct
    construct = session.query(ConstructDB).filter_by(name=construct_name).first()
    if not construct:
        raise ValueError(f"Construct {construct_name} not found in database")
    
    # Create execution log
    execution_log = ExecutionLogDB(
        pebble_id=pebble.id,
        construct_id=construct.id,
        pass_id=pass_id,
        status=status,
        result=result,
        error=error,
        start_time=datetime.utcnow(),
    )
    
    if status in ["completed", "failed"]:
        execution_log.end_time = datetime.utcnow()
    
    session.add(execution_log)
    session.commit()
    
    return execution_log


def update_execution_log(
    session: Session,
    log_id: int,
    status: Optional[str] = None,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> ExecutionLogDB:
    """Update an execution log.
    
    Args:
        session: SQLAlchemy session.
        log_id: ID of the execution log.
        status: Status of the execution (pending, running, completed, failed).
        result: Result of the execution.
        error: Error message if the execution failed.
        
    Returns:
        The updated execution log.
    """
    # Get execution log
    execution_log = session.query(ExecutionLogDB).filter_by(id=log_id).first()
    if not execution_log:
        raise ValueError(f"Execution log with ID {log_id} not found in database")
    
    # Update execution log
    if status:
        execution_log.status = status
    if result:
        execution_log.result = result
    if error:
        execution_log.error = error
    
    if status in ["completed", "failed"]:
        execution_log.end_time = datetime.utcnow()
    
    execution_log.updated_at = datetime.utcnow()
    
    session.commit()
    
    return execution_log