from pydantic import BaseModel, Field
from typing import List


class Product(BaseModel):
    id: str
    category_id: str
    category_name: str
    sku: str
    name: str
    description: str
    price: float

class Customer(BaseModel):
    id: str
    name: str
    email: str

class SalesOrder(BaseModel):
    id: str
    customer_id: str
    order_date: str
    total_amount: float

class ProductList(BaseModel):
    items: List[Product]

class CustomerList(BaseModel):
    items: List[Customer]

class SalesOrderList(BaseModel):
    items: List[SalesOrder]
