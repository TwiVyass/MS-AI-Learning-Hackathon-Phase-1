import json
import os

import openai
import pymongo
import time
import tenacity
from openai import AzureOpenAI, NotFoundError
from dotenv import load_dotenv
from tenacity import retry, wait_random_exponential, stop_after_attempt

load_dotenv()

# Load environment variables
CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")
EMBEDDINGS_DEPLOYMENT_NAME = "embeddings"
COMPLETIONS_DEPLOYMENT_NAME = "completions"
AOAI_ENDPOINT = os.environ.get("AOAI_ENDPOINT")
AOAI_KEY = os.environ.get("AOAI_KEY")
AOAI_API_VERSION = "2023-05-15"

# Initialize MongoDB client and database
db_client = pymongo.MongoClient(CONNECTION_STRING)
db = db_client.cosmic_works

# Initialize Azure OpenAI client
ai_client = AzureOpenAI(
    azure_endpoint=AOAI_ENDPOINT,
    api_version=AOAI_API_VERSION,
    api_key=AOAI_KEY
)


@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3),
       retry_error_callback=lambda _: print("Retrying due to DeploymentNotFound..."))
def generate_embeddings(text: str):
    '''
    Generate embeddings from string of text using the deployed Azure OpenAI API embeddings model.
    This will be used to vectorize document data and incoming user messages for a similarity search with
    the vector index.
    '''
    try:
        response = ai_client.embeddings.create(input=text, model=EMBEDDINGS_DEPLOYMENT_NAME)
        embeddings = response.data[0].embedding
        time.sleep(0.5)  # rest period to avoid rate limiting on AOAI
        return embeddings
    except openai.NotFoundError as e:
        raise tenacity.RetryError() from e


def add_collection_content_vector_field(collection_name: str):
    '''
    Add a new field to the collection to hold the vectorized content of each document.
    '''
    collection = db[collection_name]
    bulk_operations = []
    for doc in collection.find():
        # remove any previous contentVector embeddings
        if "contentVector" in doc:
            del doc["contentVector"]

        # generate embeddings for the document string representation
        content = json.dumps(doc, default=str)
        try:
            content_vector = generate_embeddings(content)
            bulk_operations.append(pymongo.UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"contentVector": content_vector}},
                upsert=True
            ))
        except tenacity.RetryError as e:
            print(f"Failed to generate embeddings for document {_id}: {e}")

    if bulk_operations:
        # execute bulk operations only if there are valid operations to execute
        collection.bulk_write(bulk_operations)
    else:
        print(f"No valid operations to execute for collection {collection_name}.")


# Main execution
if __name__ == "__main__":
    # Add vector field to products documents
    add_collection_content_vector_field("products")
    # Add vector field to customers documents
    add_collection_content_vector_field("customers")
    # Add vector field to sales documents
    add_collection_content_vector_field("sales")

    # Create vector indexes for products, customers, and sales collections
    for collection_name in ["products", "customers", "sales"]:
        db[collection_name].create_index([("contentVector", pymongo.HASHED)])


    # Example usage: Vector search on products collection
    def vector_search(collection_name, query, num_results=3):
        """
        Perform a vector search on the specified collection by vectorizing
        the query and searching the vector index for the most similar documents.

        returns a list of the top num_results most similar documents
        """
        collection = db[collection_name]
        query_embedding = generate_embeddings(query)
        pipeline = [
            {
                '$search': {
                    "vector": {
                        "query": query_embedding,
                        "path": "contentVector",
                        "score": {
                            "type": "cosine"
                        }
                    },
                    "limit": num_results
                }
            },
            {'$project': {'similarityScore': {'$meta': 'searchScore'}, 'document': '$$ROOT'}}
        ]
        results = collection.aggregate(pipeline)
        return results


    # Example usage: Print product search results
    def print_product_search_result(result):
        '''
        Print the search result document in a readable format
        '''
        print(f"Name: {result['name']}")
        print(f"Category: {result['categoryName']}")
        print(f"SKU: {result['categoryName']}")
        print(f"_id: {result['_id']}\n")


    # Example queries using vector search
    query1 = "What bikes do you have?"
    results1 = vector_search("products", query1, num_results=4)
    for result in results1:
        print_product_search_result(result)

    query2 = "What do you have that is yellow?"
    results2 = vector_search("products", query2, num_results=4)
    for result in results2:
        print_product_search_result(result)
