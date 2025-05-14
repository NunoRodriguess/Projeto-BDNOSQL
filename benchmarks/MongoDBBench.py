from pymongo import MongoClient
import time

# MongoDB connection
CLIENT = MongoClient("mongodb://localhost:27017/")
DB = CLIENT["bookstore"] 
COLLECTION = DB["customers"]

# Number of repetitions for each query
REPETITIONS = 10

# Dictionary of MongoDB queries
QUERIES = {
    "Query 1: Bestselling Books in Portugal": [
        { "$match": { "address.country.name": "Portugal" } },
        { "$unwind": { "path": "$lines" } },
        { "$lookup": {
            "from": "books",
            "localField": "lines.book_id",
            "foreignField": "_id",
            "as": "TITLE"
        }},
        { "$addFields": { "title": { "$arrayElemAt": ["$TITLE.title", 0] } }},
        { "$group": {
            "_id": { "country": "$address.country", "book_id": "$lines.book_id", "title": "$title" },
            "Sales": { "$count": {} }
        }},
        { "$project": { "_id": 0, "BOOK_ID": "$_id.book_id", "COUNTRY_NAME": "$_id.country.name", "TITLE": "$_id.title", "Sales": "$Sales" }},
        { "$sort": { "Sales": -1 }}
    ],
    
    "Query 2: Authors Who Earned the Most": [
        { "$unwind": { "path": "$lines" } },
        { "$lookup": {
            "from": "books",
            "localField": "lines.book_id",
            "foreignField": "_id",
            "as": "book"
        }},
        { "$addFields": { "oneBook": { "$arrayElemAt": ["$book", 0] } }},
        { "$addFields": { "num_authors": { "$size": "$oneBook.authors" } }},
        { "$group": {
            "_id": "$lines.book_id",
            "totalSales": { "$sum": "$lines.price" },
            "num_authors": { "$first": "$num_authors" },
            "authors": { "$first": "$oneBook.authors" }
        }},
        { "$unwind": { "path": "$authors" } },
        { "$group": {
            "_id": "$authors",
            "EARNINGS": { "$sum": { "$divide": [ "$totalSales", "$num_authors" ] } }
        }},
        { "$project": { "AUTHOR_ID": "$_id._id", "AUTHOR_NAME": "$_id.name", "_id": 0, "EARNINGS": { "$round": ["$EARNINGS", 2] } }},
        { "$sort": { "EARNINGS": -1 }}
    ],
    
    "Query 3: Country Where the 'Harper' Group Sold the Most": [
        { "$lookup": {
            "from": "books",
            "localField": "lines.book_id",
            "foreignField": "_id",
            "as": "book"
        }},
        { "$unwind": { "path": "$book" } },
        { "$match": { "book.publisher.name": { "$regex": "Harper" } } },
        { "$group": {
            "_id": "$address.country.name",
            "Sales": { "$count": {} }
        }},
        { "$sort": { "Sales": -1 } },
        { "$project": { "COUNTRY_NAME": "$_id", "Sales": "$Sales", "_id": 0 } }
    ],
    
    "Query 4: Customers from China and Their Orders": [
        { "$match": { "addresses.country._id": 42 } },
        { "$unwind": { "path": "$orders", "preserveNullAndEmptyArrays": False } },
        { "$lookup": { "from": "orders", "localField": "orders", "foreignField": "_id", "as": "order" } },
        { "$unwind": "$order" },
        { "$addFields": {
            "order.lines": { "$ifNull": ["$order.lines", []] },
            "order.history": { "$ifNull": ["$order.history", []] },
            "order.total_value": { "$sum": "$order.lines.price" },
            "order.total_items": { "$size": "$order.lines" },
            "order.days_since_last_status": {
                "$cond": {
                    "if": { "$gt": [{ "$size": "$order.history" }, 0] },
                    "then": { "$dateDiff": {
                        "startDate": { "$toDate": { "$max": "$order.history.status.date" } },
                        "endDate": "$$NOW", "unit": "day" }
                    },
                    "else": None
                }
            }
        }},
        { "$project": {
            "_id": 1, "first_name": 1, "last_name": 1, "email": 1,
            "order": { "_id": 1, "total_value": 1, "total_items": 1, "days_since_last_status": 1 }
        }},
        { "$group": {
            "_id": "$_id", 
            "first_name": { "$first": "$first_name" },
            "last_name": { "$first": "$last_name" }, 
            "email": { "$first": "$email" },
            "orders": { 
                "$push": {
                    "$cond": { 
                        "if": { "$ne": ["$order", {}] }, 
                        "then": "$order", 
                        "else": "$$REMOVE" 
                    }
                }
            }
        }},
        { "$sort": { "_id": 1 }}
    ]
}

def run_benchmark(collection, query, query_name):
    times = []
    print(f"\nRunning benchmark: {query_name}")

    for i in range(REPETITIONS):
        start_time = time.time()
        list(collection.aggregate(query))  # Execute aggregation pipeline
        end_time = time.time()
        duration = end_time - start_time
        times.append(duration)
        print(f"  Execution {i+1:02d}: {duration:.4f} seconds")

    average_time = sum(times) / len(times)
    print(f"Average time ({query_name}): {average_time:.4f} seconds")

def main():
    for query_name, query in QUERIES.items():
        run_benchmark(COLLECTION, query, query_name)

if __name__ == "__main__":
    main()
