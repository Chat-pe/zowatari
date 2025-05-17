"""Example ETL process using Zowatari."""

import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

# Add parent directory to path to import zowatari
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from zowatari import pebble, cement, construct, first_pass, scheduled_pass
from zowatari.db import init_db, get_session
from zowatari.models.base import CementInstruction


# Define data models
class Customer(BaseModel):
    """Customer data model."""
    
    id: int
    name: str
    email: str
    created_at: datetime


class Order(BaseModel):
    """Order data model."""
    
    id: int
    customer_id: int
    total_amount: float
    status: str
    created_at: datetime


class OrderSummary(BaseModel):
    """Order summary data model."""
    
    customer_id: int
    customer_name: str
    total_orders: int
    total_spent: float
    average_order_value: float


# Define pebble functions
@pebble(
    name="extract_customers",
    description="Extract customer data from source system",
    tags=["extract", "customer"],
)
def extract_customers() -> List[Customer]:
    """Extract customer data from source system."""
    # In a real implementation, this would fetch data from a database or API
    print("Extracting customer data...")
    
    customers = [
        Customer(
            id=1,
            name="John Doe",
            email="john.doe@example.com",
            created_at=datetime.now(),
        ),
        Customer(
            id=2,
            name="Jane Smith",
            email="jane.smith@example.com",
            created_at=datetime.now(),
        ),
    ]
    
    return customers


@pebble(
    name="extract_orders",
    description="Extract order data from source system",
    tags=["extract", "order"],
)
def extract_orders() -> List[Order]:
    """Extract order data from source system."""
    # In a real implementation, this would fetch data from a database or API
    print("Extracting order data...")
    
    orders = [
        Order(
            id=1,
            customer_id=1,
            total_amount=100.0,
            status="completed",
            created_at=datetime.now(),
        ),
        Order(
            id=2,
            customer_id=1,
            total_amount=50.0,
            status="completed",
            created_at=datetime.now(),
        ),
        Order(
            id=3,
            customer_id=2,
            total_amount=75.0,
            status="completed",
            created_at=datetime.now(),
        ),
    ]
    
    return orders


@pebble(
    name="transform_order_summary",
    description="Transform order data into order summary",
    tags=["transform", "order", "summary"],
)
def transform_order_summary(
    customers: List[Customer], orders: List[Order]
) -> List[OrderSummary]:
    """Transform order data into order summary."""
    print("Transforming order data into summary...")
    
    # Create dictionary of customers by ID for easy lookup
    customer_dict = {customer.id: customer for customer in customers}
    
    # Group orders by customer ID
    customer_orders: Dict[int, List[Order]] = {}
    for order in orders:
        if order.customer_id not in customer_orders:
            customer_orders[order.customer_id] = []
        customer_orders[order.customer_id].append(order)
    
    # Create order summaries
    order_summaries = []
    for customer_id, orders in customer_orders.items():
        if customer_id not in customer_dict:
            continue
            
        customer = customer_dict[customer_id]
        total_orders = len(orders)
        total_spent = sum(order.total_amount for order in orders)
        average_order_value = total_spent / total_orders if total_orders > 0 else 0
        
        order_summary = OrderSummary(
            customer_id=customer_id,
            customer_name=customer.name,
            total_orders=total_orders,
            total_spent=total_spent,
            average_order_value=average_order_value,
        )
        
        order_summaries.append(order_summary)
    
    return order_summaries


@pebble(
    name="load_order_summary",
    description="Load order summary data into target system",
    tags=["load", "order", "summary"],
)
def load_order_summary(order_summaries: List[OrderSummary]) -> None:
    """Load order summary data into target system."""
    # In a real implementation, this would load data into a database or API
    print("Loading order summary data...")
    
    for summary in order_summaries:
        print(f"Loaded order summary for customer {summary.customer_name}:")
        print(f"  Total orders: {summary.total_orders}")
        print(f"  Total spent: ${summary.total_spent:.2f}")
        print(f"  Average order value: ${summary.average_order_value:.2f}")
        print()


def main():
    """Run the example ETL process."""
    # Initialize database
    db_url = "postgresql://postgres:postgres@localhost:5432/zowatari"
    engine = init_db(db_url)
    session = get_session(engine)
    
    try:
        # Define cement function
        extract_transform_load = cement(
            name="extract_transform_load",
            description="Extract, transform, and load order summary data",
            pebble_instructions=[
                CementInstruction(
                    pebble_name="extract_customers",
                    parameters={},
                    order=1,
                    depends_on=[],
                ),
                CementInstruction(
                    pebble_name="extract_orders",
                    parameters={},
                    order=2,
                    depends_on=[],
                ),
                CementInstruction(
                    pebble_name="transform_order_summary",
                    parameters={
                        "customers": "$extract_customers",
                        "orders": "$extract_orders",
                    },
                    order=3,
                    depends_on=["extract_customers", "extract_orders"],
                ),
                CementInstruction(
                    pebble_name="load_order_summary",
                    parameters={
                        "order_summaries": "$transform_order_summary",
                    },
                    order=4,
                    depends_on=["transform_order_summary"],
                ),
            ],
        )
        
        # Define construct function
        order_summary_etl = construct(
            name="order_summary_etl",
            description="ETL process for order summary data",
            cement_order=[
                ("extract_transform_load", 1),
            ],
            tags=["order", "summary", "etl"],
        )
        
        # Run first pass
        first_pass("order_summary_etl")
        
        # Register in database
        # In a real implementation, you would register all functions in the database
        # and use the database to track execution
        
        print("ETL process completed successfully!")
    finally:
        session.close()


if __name__ == "__main__":
    main()