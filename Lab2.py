import os
import pymongo
import requests
from pymongo import UpdateOne, DeleteMany
from pymongo.errors import InvalidOperation  # Import the specific error
from dotenv import load_dotenv
from models import Customer, CustomerList, SalesOrder, SalesOrderList

load_dotenv()
CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")

client = pymongo.MongoClient(CONNECTION_STRING)
db = client.cosmic_works

try:
    # Clear existing data
    db.customers.bulk_write([DeleteMany({})])
    db.sales.bulk_write([DeleteMany({})])

    # Fetch customer and sales raw data
    customer_sales_raw_data = "https://cosmosdbcosmicworks.blob.core.windows.net/cosmic-works-small/customer.json"
    response = requests.get(customer_sales_raw_data)
    response.encoding = 'utf-8-sig'  # Ensure correct encoding
    response_json = response.json()

    # Filter out customers and sales orders from the JSON data
    customers = [cust for cust in response_json if cust.get("type") == "customer"]
    sales_orders = [sales for sales in response_json if sales.get("type") == "salesOrder"]

    # Ensure each customer data has all required fields before processing
    valid_customers = []
    for cust_data in customers:
        if all(field in cust_data for field in ['id', 'name', 'email']):
            valid_customers.append(Customer(**cust_data))
        else:
            print(f"Ignoring customer data with missing fields: {cust_data}")

    # Create CustomerList instance and bulk write to MongoDB
    customer_data = CustomerList(items=valid_customers)
    db.customers.bulk_write([UpdateOne({"_id": cust.id}, {"$set": cust.model_dump(by_alias=True)}, upsert=True) for cust in customer_data.items])

    # Ensure each sales order data has all required fields before processing
    valid_sales_orders = []
    for sale_data in sales_orders:
        if all(field in sale_data for field in ['id', 'customer_id', 'order_date', 'total_amount']):
            valid_sales_orders.append(SalesOrder(**sale_data))
        else:
            print(f"Ignoring sales order data with missing fields: {sale_data}")

    # Create SalesOrderList instance and bulk write to MongoDB
    sales_data = SalesOrderList(items=valid_sales_orders)
    db.sales.bulk_write([UpdateOne({"_id": sale.id}, {"$set": sale.model_dump(by_alias=True)}, upsert=True) for sale in sales_data.items])

    # Optional: Print success messages or other notifications
    print("Bulk write executed successfully for customers and sales orders.")

except InvalidOperation as e:
    print(f"InvalidOperation Exception: {e}")
    # Handle the exception here as needed, such as logging or notifying the user
except Exception as e:
    print(f"Unexpected error: {e}")
    # Handle other unexpected exceptions here

finally:
    client.close()  # Close the MongoDB client connection
