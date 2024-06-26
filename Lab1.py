from dotenv import load_dotenv
import os
import pymongo
from models import Product
import uuid  # For generating unique IDs
from pymongo import UpdateOne

# Load environment variables from .env file
load_dotenv()

# Fetch the MongoDB connection string from the environment variable
CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

if not CONNECTION_STRING:
    raise ValueError("Missing DB_CONNECTION_STRING environment variable.")

# Connect to MongoDB (Azure Cosmos DB)
client = pymongo.MongoClient(CONNECTION_STRING)

# Create or connect to the database 'cosmic_works'
db = client.cosmic_works

# Optionally, create a collection within the database
collection = db.products  # Assuming the collection name is 'products'

# Generate unique IDs for the new products
products = [
    Product(
        id=str(uuid.uuid4()),
        category_id="56400CF3-446D-4C3F-B9B2-68286DA3BB99",
        category_name="Bikes, Mountain Bikes",
        sku="BK-M18S-42",
        name="Mountain-100 Silver, 42",
        description='The product called "Mountain-500 Silver, 42"',
        price=742.42
    ),
    Product(
        id=str(uuid.uuid4()),
        category_id="26C74104-40BC-4541-8EF5-9892F7F03D72",
        category_name="Components, Saddles",
        sku="SE-R581",
        name="LL Road Seat/Saddle",
        description='The product called "LL Road Seat/Saddle"',
        price=27.12
    ),
    Product(
        id=str(uuid.uuid4()),
        category_id="75BF1ACB-168D-469C-9AA3-1FD26BB4EA4C",
        category_name="Bikes, Touring Bikes",
        sku="BK-T44U-60",
        name="Touring-2000 Blue, 60",
        description='The product called Touring-2000 Blue, 60"',
        price=1214.85
    ),
    Product(
        id=str(uuid.uuid4()),
        category_id="75BF1ACB-168D-469C-9AA3-1FD26BB4EA4C",
        category_name="Bikes, Touring Bikes",
        sku="BK-T79Y-60",
        name="Touring-1000 Yellow, 60",
        description='The product called Touring-1000 Yellow, 60"',
        price=2384.07
    ),
    Product(
        id=str(uuid.uuid4()),
        category_id="26C74104-40BC-4541-8EF5-9892F7F03D72",
        category_name="Components, Saddles",
        sku="SE-R995",
        name="HL Road Seat/Saddle",
        description='The product called "HL Road Seat/Saddle"',
        price=52.64,
    )
]

# Generate JSON using alias names defined on the model
products_json = [product.dict(by_alias=True) for product in products]

# Insert multiple documents
insert_result = collection.insert_many(products_json)
print(f"Inserted product IDs: {insert_result.inserted_ids}")

# Query for multiple documents
retrieved_documents = collection.find({})
print("All documents in the collection:")
for doc in retrieved_documents:
    print(doc)

# Cast JSON documents into Product models
retrieved_products = [Product(**doc) for doc in retrieved_documents]

# Print the retrieved products
print("\nRetrieved Products:")
for product in retrieved_products:
    print(product)

# Update a product's name
product_to_update_id = products[0].id
retrieved_product = collection.find_one({"id": product_to_update_id})

if retrieved_product:
    retrieved_product["name"] = "Mountain-100 Silver, 48\""

    # Update the document in the database
    update_result = collection.find_one_and_update(
        {"id": product_to_update_id},
        {"$set": {"name": retrieved_product["name"]}},
        return_document=pymongo.ReturnDocument.AFTER
    )

    # Print the updated JSON document
    print("Updated JSON document:")
    print(update_result)

    # Cast the updated JSON document into the Product model
    updated_product = Product(**update_result)

    # Print the updated product name
    print(f"\nUpdated Product name: {updated_product.name}")
else:
    print(f"No product found with id: {product_to_update_id}")

# Delete the document from the database
delete_result = collection.delete_one({"id": product_to_update_id})
print(f"Deleted documents count: {delete_result.deleted_count}")

# Print the number of documents remaining in the collection
print(f"Number of documents in the collection: {collection.count_documents({})}")

# Bulk write example
bulk_operations = [
    UpdateOne(
        {"id": products[1].id},
        {"$set": {"price": products[1].price + 10}},
        upsert=True
    ),
    UpdateOne(
        {"id": products[2].id},
        {"$set": {"price": products[2].price + 20}},
        upsert=True
    )
]

bulk_write_result = collection.bulk_write(bulk_operations)
print("Bulk write operation completed.")
print(f"Matched count: {bulk_write_result.matched_count}")
print(f"Modified count: {bulk_write_result.modified_count}")
print(f"Upserted count: {bulk_write_result.upserted_count}")

# db.drop_collection("products")
client.drop_database("cosmic_works")
client.close()