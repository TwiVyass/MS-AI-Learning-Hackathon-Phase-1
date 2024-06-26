import os
import json
import pymongo
from dotenv import load_dotenv
from langchain.chat_models import AzureChatOpenAI
from langchain.embeddings import AzureOpenAIEmbeddings
from langchain.vectorstores import AzureCosmosDBVectorSearch
from langchain.schema.document import Document
from langchain.prompts import PromptTemplate
from langchain.schema import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough
from langchain.agents import Tool
from langchain.agents.agent_toolkits import create_conversational_retrieval_agent
from langchain_core.messages import SystemMessage

# Load environment variables
load_dotenv()
CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")
AOAI_ENDPOINT = os.environ.get("AOAI_ENDPOINT")
AOAI_KEY = os.environ.get("AOAI_KEY")
AOAI_API_VERSION = "2023-09-01-preview"

# Establish Azure OpenAI connectivity for LLM and embeddings
llm = AzureChatOpenAI(
    temperature=0,
    openai_api_version=AOAI_API_VERSION,
    azure_endpoint=AOAI_ENDPOINT,
    openai_api_key=AOAI_KEY,
    azure_deployment="completions"
)

embedding_model = AzureOpenAIEmbeddings(
    openai_api_version=AOAI_API_VERSION,
    azure_endpoint=AOAI_ENDPOINT,
    openai_api_key=AOAI_KEY,
    azure_deployment="embeddings",
    chunk_size=10
)

# Define the vector store for product searches
vector_store_products = AzureCosmosDBVectorSearch.from_connection_string(
    connection_string=CONNECTION_STRING,
    namespace="cosmic_works.products",
    embedding=embedding_model,
    index_name="VectorSearchIndex",
    embedding_key="contentVector",
    text_key="_id"
)

# Define the system prompt template for RAG
system_prompt = """
You are a helpful, fun, and friendly sales assistant for Cosmic Works, a bicycle and bicycle accessories store.
Your name is Cosmo.
You are designed to answer questions about the products that Cosmic Works sells.

Only answer questions related to the information provided in the list of products below that are represented
in JSON format.

If you are asked a question that is not in the list, respond with "I don't know."

Only answer questions related to Cosmic Works products, customers, and sales orders.

If a question is not related to Cosmic Works products, customers, or sales orders,
respond with "I only answer questions about Cosmic Works"

List of products:
{products}

Question:
{question}
"""


# Function to format documents for the system prompt
def format_docs(docs: list) -> str:
    """
    Prepares the product list for the system prompt.
    """
    str_docs = []
    for doc in docs:
        doc_dict = {"_id": doc.page_content}
        doc_dict.update(doc.metadata)
        if "contentVector" in doc_dict:
            del doc_dict["contentVector"]
        str_docs.append(json.dumps(doc_dict, default=str))
    return "\n\n".join(str_docs)


# Create retriever from the vector store for product searches
retriever_products = vector_store_products.as_retriever()

# Create the prompt template from the system prompt text
llm_prompt = PromptTemplate.from_template(system_prompt)

# Define the RAG chain for integrating the retriever and LLM
rag_chain = (
        {"products": retriever_products | format_docs, "question": RunnablePassthrough()}
        | llm_prompt
        | llm
        | StrOutputParser()
)

# Example queries using the RAG chain
questions = [
    "What products do you have that are yellow?",
    "What products were purchased for sales order '06FE91D2-B350-471A-AD29-906BF4EB97C4' ?",
    "What was the sales order total for sales order '93436616-4C8A-407D-9FDA-908707EFA2C5' ?",
    "What was the price of the product with sku 'FR-R92B-58' ?"
]

for question in questions:
    response = rag_chain.invoke(question)
    print("***********************************************************")
    print(response)
