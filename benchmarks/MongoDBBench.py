from pymongo import MongoClient
import time
import psutil
from tabulate import tabulate
import os

# MongoDB connection
CLIENT = MongoClient("mongodb://localhost:27017/")
DB = CLIENT["bookstore"]
COLLECTION = DB["customers"]

REPETITIONS = 10
PROCESS = psutil.Process(os.getpid())

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
    cpu_usages = []
    mem_usages = []

    for _ in range(REPETITIONS):
        PROCESS.cpu_percent(interval=None)  # Reset CPU percent
        start_mem = PROCESS.memory_info().rss / (1024 ** 2)  # in MB
        start_time = time.time()

        list(collection.aggregate(query))  # Run MongoDB aggregation

        end_time = time.time()
        end_mem = PROCESS.memory_info().rss / (1024 ** 2)
        cpu = PROCESS.cpu_percent(interval=None)
        
        duration_ms = (end_time - start_time) * 1000
        mem_used = end_mem - start_mem

        times.append(duration_ms)
        cpu_usages.append(cpu)
        mem_usages.append(mem_used)

    avg_time = sum(times) / REPETITIONS
    std_dev = (sum((x - avg_time) ** 2 for x in times) / (REPETITIONS - 1)) ** 0.5 if REPETITIONS > 1 else 0.0
    avg_cpu = sum(cpu_usages) / REPETITIONS
    avg_mem = sum(mem_usages) / REPETITIONS

    return {
        "query_name": query_name,
        "avg_time": avg_time,
        "std_dev": std_dev,
        "avg_cpu": avg_cpu,
        "avg_mem": avg_mem
    }

def main():
    results = []
    for query_name, query in QUERIES.items():
        result = run_benchmark(COLLECTION, query, query_name)
        results.append(result)

    table_data = [
        [
            r['query_name'],
            f"{r['avg_time']:.2f}",
            f"{r['std_dev']:.2f}",
            f"{r['avg_cpu']:.2f}",
            f"{r['avg_mem']:.2f}"
        ]
        for r in results
    ]
    headers = ["Query", "Avg Time (ms)", "Std Dev (ms)", "Avg CPU (%)", "Avg Mem (MB)"]

    print("\n=== MongoDB Benchmark Summary ===")
    print(tabulate(table_data, headers=headers, tablefmt="github"))

if __name__ == "__main__":
    main()