"""
CREATE INDEX idx_book_title ON book(title);
CREATE INDEX idx_customer_email ON customer(email);
CREATE INDEX idx_order_date ON cust_order(order_date);
CREATE INDEX idx_address_country ON address(country_id);
COMMIT;

"""
from pymongo import MongoClient
import pymongo

database = "bookstore"
uri = "mongodb://localhost:27017/" 
client = MongoClient(uri)
db = client[database]


# CREATE INDEX idx_book_title ON book(title);
collection = db["books"]
collection.create_index([("title", pymongo.ASCENDING)])

# CREATE INDEX idx_customer_email ON customer(email);
collection = db["customers"]
collection.create_index([("email", pymongo.ASCENDING)])

# CREATE INDEX idx_order_date ON cust_order(order_date);
collection = db["orders"]
collection.create_index([("order_date", pymongo.ASCENDING)])

# CREATE INDEX idx_address_country ON address(country_id);

collection = db["customers"]
collection.create_index([("addresses.country._id", pymongo.ASCENDING)])

collection = db["orders"]
collection.create_index([("address.country._id", pymongo.ASCENDING)])

collections = ["books", "customers", "orders"]

for col_name in collections:
    col = db[col_name]
    print(f"Indexes for '{col_name}':")
    for name, index in col.index_information().items():
        print(f"  {name}: {index}")
    print()
    
# Close the connection
client.close()