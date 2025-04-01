from pymongo import MongoClient

def test_mongo_connection(uri: str, database: str):
    try:
        client = MongoClient(uri)
        db = client[database]
        # Test the connection by listing collection names
        collections = db.list_collection_names()
        print(f"Connected to MongoDB! Collections in '{database}': {collections}")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    MONGO_URI = "mongodb://localhost:27017/"  # Change this to your MongoDB URI
    DATABASE_NAME = "bookstore"  # Change this to your database name
    
    test_mongo_connection(MONGO_URI, DATABASE_NAME)