# Import the needed packages to use the configuration file '.env'
import os
# Import the library for pretty print.
import pprint

# Install python-dotenv to manage .env configutation files
from dotenv import load_dotenv
# Install pymongo[srv] (including dnspython) Many other depenencies may be installed with the same method.
# Import MongoClient to stablish connection.
from pymongo import MongoClient
from bson.objectid import ObjectId

""" CLIENT SETUP """

# Load the config from .env file
load_dotenv()
# Asign the conection string to a constant.
MONGODB_URI = os.environ["MONGODB_URI"]
# Create a MongoClient instance with the conection string.
mongo_client = MongoClient(MONGODB_URI)

for db_name in mongo_client.list_database_names():
    print(db_name)

""" INSERT DOCUMENTS """

# PyMongo allow to use native datatypes from python.
# The main way to work with documents in python is with dictionaries.
# Use bson package to wotk with other datatypes as ObjectId
# If "_id" is nos included, Mongo assign it automatically
new_document = {
    "_id": ObjectId('5ca4bbc7a2dd94ee5816238f'),
    "account_id": 674364,
    "limit": 10000,
    "products": [
        {   
            0: "InvestmentStock"
        }
    ]
}
# Get the reference to a database.
db = mongo_client.sample_analytics
# Get the reference to a collection
accounts_collection = db.accounts
# Create a dictionary with the information to insert.
new_account = {
    "account_id": 000001,
    "limit": 1500,
    "products": [
        {   
            0: "InvestmentStock"
        }
    ]
}
# Write an expresion that inserts the "new_account" document in the "accounts" collection.
# insertOne returns a result.
insert_result = accounts_collection.insert_one(new_account)
print(f"_id of the inserted document is {insert_result.inserted_id}")

# insertMany takes an iterable of documents to insert.
new_accounts = [
    {   "account_id": 000002,
        "limit": 2500,
        "products": [
            {
                0: "CurrencyService",
                1: "InvestmentStock"
            }
        ]
    },
    {
        "account_id": 000003,
        "limit": 2500,
        "products": [
            {
                0: "InvestmentFund",
                1: "InvestmentStock"
            }
        ]
    }
]
# insertMany returns a result.
insert_many_result = accounts_collection.insert_many(new_accounts)
document_ids = insert_many_result.inserted_ids
print(f"Number of inserted ids: {len(document_ids)}")
print(f"_ids of inserted documents: {document_ids}")

""" QUERY DOCUMENTS """

# findOne returns a single document that matches a query, or None if there are no matches.
# Its useful if we know that there's only 1 document that matches of we're interested only in the first match.
# To make a query we need a filter in a dictionary format.
query = {"_id": ObjectId("5ca4bbc7a2dd94ee5816238f")}
# Write an expression that retrieves the document matching the query.
find_result = accounts_collection.find_one(query)
pprint.pprint(find_result)

# findMany returns a cursor, an iterable with the documents.
# Make a query in dictionary format
query_many = {"limit":{"$lt": 3000}}
# Write an expression that retrieves the documents matching the query.
find_many_cursor = accounts_collection.find(query_many)
# Print total documents found and the documents.
total_docs = 0
for document in find_many_cursor:
    total_docs += 1
    pprint.pprint(document)
    print()
print(f"Total of documents found: {total_docs}")

""" UPDATE DOCUMENTS"""

# updateOne is used to update a single document that matches specified criteria.
# updateOne have 2 required arguments, a filter documents (selection criteria) and an update document (specify the modifications)
update_filter = {"_id": insert_result.inserted_id}
update_modification = {"$inc": {"limit": 1000}}
# Print the original document
pprint.pprint(accounts_collection.find_one(update_filter))
# Write an expression that update the document that matches the filter.
# updateOne returns a result.
update_result = accounts_collection.update_one(update_filter, update_modification)
print(f"Updated documents: {update_result.modified_count}")
# Print updated document
pprint.pprint(accounts_collection.find_one(update_filter))

# updateMany is used to update multiple documents in a single operation
update_many_filter = {"limit": {"$lt": 3000}}
update_many_modification = {"$set": {"min_limit": 1000}}
# Write an expression that update the documents that matches the filter.
# updateMany returns a result
update_many_result = accounts_collection.update_many(update_many_filter, update_many_modification)
# Print updates documents.
print(f"Documents matched: {update_many_result.matched_count}")
print(f"Documents updated: {update_many_result.modified_count}")
print("Sample updated document")
pprint.pprint(accounts_collection.find_one(update_many_filter))
print("All updated documents")
pprint.pprint(accounts_collection.find(update_many_filter))

""" DELETE DOCUMENTS """

# deleteOne is used to delete a single document from a collection
# It takes a query filter as argument to match the document to be deleted.
# If the argument is empty ({}) it matches and delete the first document in the collection.
delete_filter = {"_id": insert_result.inserted_id}
# Print the original document
print("Searching for target document before delete:")
pprint.pprint(accounts_collection.find_one(delete_filter))
# Write an expression that delete the target document.
# deleteOne returns a result.
delete_result = accounts_collection.delete_one(delete_filter)
# Search the document after delete.
print("Searching for target document after delete:")
pprint.pprint(accounts_collection.find_one(delete_filter))
# Print documents deleted
print(f"Documents deleted: {delete_result.deleted_count}")

# deleteMany is used to delete multiple documents in a single operation.
# It takes a query filter as an argument to match the documents to be deleted.
# If the argument is empty ({}) it delete all documents in the collection.
delete_many_filter = {"limit": {"$lt": 3000}}
# Search a sample target document before delete:
print("Search a sample target document before delete:")
pprint.pprint(accounts_collection.find_one(delete_many_filter))
# Write an expression that delete the target documents.
# deleteMany returns a result.
delete_many_result = accounts_collection.delete_many(delete_many_filter)
# Search a sample target document afet delete:
print("Search a sample target document after delete:")
pprint.pprint(accounts_collection.find_one(delete_many_filter))
# Print documents deleted.
print(f"Documents deleted: {delete_many_result.deleted_count}")

""" Transactions """

# A transaction is a group of multiple database operations that are completed together as unit or not at all.
# Are used when a group of related operations must eighter all succced or all fail together (This is the principle of Atomicity).
# Steps to complete a transaction:
# 1. Stablish a db conection.
# 2. Define a callback function that specifies the sequesn of operations to perform inside the transaction.
def callback(session, transfer_id=None, account_id_receiver=None, account_id_sender=None, transfer_amount=None):
    # Get reference to the collections
    accounts_collection = session.client.bank.accounts
    transfers_collection = session.client.banck.transfers

    transfer = {
        "transfer_id": transfer_id,
        "to_account": account_id_receiver,
        "from_account": account_id_sender,
        "amount": {"$numberDecimal": transfer_amount}
    }

    # Transaction operations
    # Important: You must pass the session to each operation
    # Update sender account: subtract transfer amount from balance and add transfer ID
    accounts_collection.update_one(
        {"account_id": account_id_sender},
        {
            "$inc": {"balance": -transfer_amount},
            "$push": {"transfers_complete": transfer_id}
        },
        session = session
    )
     # Update receiver account: add transfer amount to balance and add transfer ID
    accounts_collection.update_one(
        {"account_id": account_id_receiver},
        {
            "$inc": {"balance": transfer_amount},
            "$push": {"transfers_complete": transfer_id},
        },
        session=session,
    )

    # Add new transfer to "transfers" Collection
    transfers_collection.insert_one(transfer, session = session)
    # Print a success message
    print("Transaction successful")

# To pass the arguments needed for the callback function we can use a lambda that emulates the follow wrapper.
def callback_wrapper(session):
    callback(
        session,
        transfer_id="TR218721873",
        account_id_receiver="MDB343652528",
        account_id_sender="MDB574189300",
        transfer_amount=100,
    )

# 3. Start a client session.
with mongo_client.start_session() as session:
# 4. Start the transaction by calling the "with_transaction()" method on the session object.
    session.with_transaction(callback_wrapper)
# MongoDB will automatically cancel any multi-document transaction that runs for more than 60 seconds.

""" AGGREGATION """

# Aggregation framework is used to process documents and return computed results.
# An aggregation pipeline consists of one or more stages that proces documents.
# Each stage perdorms an operation on the input documents.
# The $match stage, filters documents to pass only the documents that match the specified contidion to the next stage.
# The $group stage separates documents into groups according to a group key.
# The output of the group stage is one document for each unique group key.
# Steps for aggregation pipeline.
# 1. Define the stages.
# Select accounts with balances less than $1000
select_by_balance = {"$match": {"balance": {"$lt": 1000}}}
# Separate documents by account type and calculate the average balance for each account type.
separate_by_account_calculate_avg_balance = {
    # The _id indicates wich field to group by.
    # The key is preceded by $ to indicate that the value of the field should be used.
    "$group": {"_id": "$account_type", "avg_balance": {"avg": "$balance"}}
}
# 2. Create an aggregation pipeline storing in a List the stages.
pipeline = [
    # Order is important in the aggregation pipelines.
    select_by_balance,
    separate_by_account_calculate_avg_balance
]
# 3. Perform an aggregation on "pipeline"
# Aggregation returns a cursor as result.
aggregation_result = accounts_collection.aggregate(pipeline)

print("AGGREGATION MATCH AND GROUP")
print("Average balance of checking and savings accounts with balances of less than $1000:\n")

for document in aggregation_result:
    pprint.pprint(document)

# The $sort stage sorts all input documents and passes the documents to the next stage in the sorted order.
# To sort in ascending order use the value 1, for descending order use the value -1
# The $project stage specifies the documents shape, the fields returned by the aggregation.
# Documents that contain only the specified fields will be passed on the next stage of the pipeline.
# The specified fields can be existing fields from the input documents or newly computed fields.
# The project stage should be the last stage because it specifies the exact fields to be returned to the client.
# 1. Define the stages.
# Conversion rate for calculate balance BGP
conversion_rate_usd_to_gbp = 1.3
# Select checking accounts with balances of more than $1500
select_accounts = {"$match": {"account_type": "checking", "balance": {"$gt": 1500}}}
# Sort the document from highest balance to lowest.
organize_by_original_balance = {"$sort": {"balance": -1}}
# Return only the account type & and balance fields, plus a new field containing balance in Great British Pounds (GBP)
return_specified_fields = {
    "$project": {
        # To include fields set the value to 1.
        "account_type": 1,
        "balance": 1,
        "gbp_balance": {"$divide": ["$balance", conversion_rate_usd_to_gbp]},
        # _id field is always returned, to exclude fields set the value to 0
        "_id": 0
    }
}
# 2. Create an aggregation pipeline storing in a List the stages.
pipeline_project = [
    select_accounts,
    organize_by_original_balance,
    return_specified_fields
]
# 3. Perform an aggregation on "pipeline"
# Aggregation returns a cursor as result.
aggregation_project_result = accounts_collection.aggregate(pipeline_project)

print("AGGREGATION SORT AND PROJECT")
print("""
    Account type, original balance and balance in BP of checking accounts with original balance greater then $1500
    in order from highest original balance to lowest:\n"""
)

for document in aggregation_project_result:
    pprint.pprint(document)

# We should close the conection to the database at the end of the execution.
mongo_client.close()
