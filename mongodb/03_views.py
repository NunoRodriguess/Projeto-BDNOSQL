from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["bookstore"]

# Define the pipeline
pipeline = [
    {
        "$unwind": {
            "path": "$authors"
        }
    },
    {
        "$addFields": {
            "BOOK_ID": "$_id",
            "TITLE": "$title",
            "AUTHOR_NAME": "$authors.name"
        }
    },
    {
        "$unset": [
            "isbn13", "language", "_id", "publisher", "authors", "title"
        ]
    }
]

# Create the view
db.command({
    "create": "book_with_authors",     # Name of the view
    "viewOn": "books",                # Source collection
    "pipeline": pipeline
})

print("View 'book_with_authors' created successfully.")

pipeline = [
    {
        '$addFields': {
            'STATUS_VALUE': {
                '$reduce': {
                    'input': '$history', 
                    'initialValue': {}, 
                    'in': {
                        '$cond': [
                            {
                                '$or': [
                                    {
                                        '$gt': [
                                            '$$this.status.date', '$$value.date'
                                        ]
                                    }, {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$$this.status.date', '$$value.date'
                                                ]
                                            }, {
                                                '$gt': [
                                                    '$$this._id', '$$value._id'
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }, '$$this', '$$value'
                        ]
                    }
                }
            }
        }
    }, {
        '$project': {
            'ORDER_ID': '$_id', 
            'ORDER_DATE': '$order_date', 
            'STATUS_VALUE': '$STATUS_VALUE.status.status'
        }
    }
]

# Create the view
db.command({
    "create": "orders_with_status",     # Name of the view
    "viewOn": "orders",                # Source collection
    "pipeline": pipeline
})

print("View 'orders_with_status' created successfully.")


