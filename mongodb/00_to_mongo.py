import oracledb
from pymongo import MongoClient


# Transform data from Oracle to MongoDB format
def transform_data_book(data):
    transformed_data = []
    for record in data:
        transformed_record = {
            "_id": int(record[0]),
            "title": record[1],
            "isbn13": int(record[2]),
            "language": {
                "_id": record[3],
                "code": record[4],
                "name": record[5]
            },
            "publisher": {
                "_id": record[6],
                "name": record[7]
            },
            "authors":[],

        }
        transformed_data.append(transformed_record)
    return transformed_data


def transform_data_customers(data):
    transformed_data = []
    for record in data:
        transformed_record = {
            "_id": int(record[0]),
            "first_name": record[1],
            "last_name": record[2],
            "email": record[3],
            "addresses":[],
            "orders":[]
        }
        transformed_data.append(transformed_record)
    return transformed_data


def transform_data_orders(data):
    transformed_data = []
    for record in data:
        transformed_record = {
            "_id": int(record[0]),
            "order_date": record[1].strftime("%Y-%m-%d"),
            "method": {
                "_id": int(record[2]),
                "name": record[3],
                "cost": float(record[4]),
            },
            "history":[],
            "lines":[]
        }
        transformed_data.append(transformed_record)
    return transformed_data


try:
    # config
    connection = oracledb.connect(
        user="bookstore",
        password="bookstore",
        dsn="localhost/orclcdb"
    )
    database = "bookstore"
    uri = "mongodb://localhost:27017/" 
    
    # ligar me às dbs
    cursor = connection.cursor()
    client = MongoClient(uri)
    db = client[database]
    # tratar com a oracle
    # fazer querys
    book_query = """
    SELECT BOOK.BOOK_ID, 
        BOOK.TITLE, 
        BOOK.ISBN13, 
        BOOK.LANGUAGE_ID,
        BOOK_LANGUAGE.LANGUAGE_CODE, 
        BOOK_LANGUAGE.LANGUAGE_NAME,
        BOOK.PUBLISHER_ID,
        PUBLISHER.PUBLISHER_NAME
    FROM BOOK
    INNER JOIN BOOK_LANGUAGE 
        ON BOOK.LANGUAGE_ID = BOOK_LANGUAGE.LANGUAGE_ID
    INNER JOIN PUBLISHER
        ON BOOK.PUBLISHER_ID = PUBLISHER.PUBLISHER_ID
    """
    print("Fetching books ...")
    cursor.execute(book_query)
    rows = cursor.fetchall()
    #transformar os dados
    print("Processing books ...")
    data = transform_data_book(rows)
    print("Processing Authors ...")
    for t in data:
        authors_query = f"""
        SELECT AUTHOR.AUTHOR_ID,AUTHOR.AUTHOR_NAME FROM BOOK_AUTHOR
        INNER JOIN AUTHOR 
            ON BOOK_AUTHOR.AUTHOR_ID = AUTHOR.AUTHOR_ID
        WHERE BOOK_AUTHOR.BOOK_ID = {str(t['_id'])}
        """
        cursor.execute(authors_query)
        authors = cursor.fetchall()
        for record in authors:
            transformed_record = {
                "_id":int(record[0]),
                "name":record[1]
            }
            t['authors'].append(transformed_record)
    
    collection = db["books"]
    collection.insert_many(data)

    # CUSTOMERS

    customer_query = """
    SELECT 
    CUSTOMER.CUSTOMER_ID,
    CUSTOMER.FIRST_NAME,
    CUSTOMER.LAST_NAME,
    CUSTOMER.EMAIL
    FROM CUSTOMER
    """
    print("Fetching customers ...")
    cursor.execute(customer_query)
    rows = cursor.fetchall()
    print("Processing customers ...")
    data = transform_data_customers(rows)

    print("Processing Addresses ...")
    for t in data:
        add_query = f"""
        SELECT 
            CUSTOMER_ADDRESS.ADDRESS_ID,
            ADDRESS_STATUS.ADDRESS_STATUS,
            CUSTOMER_ADDRESS.STATUS_ID,
            ADDRESS.STREET_NUMBER,
            ADDRESS.STREET_NAME,
            ADDRESS.CITY,
            ADDRESS.COUNTRY_ID,
            COUNTRY.COUNTRY_NAME
        FROM CUSTOMER_ADDRESS
            INNER JOIN ADDRESS_STATUS
                ON CUSTOMER_ADDRESS.STATUS_ID = ADDRESS_STATUS.STATUS_ID
            INNER JOIN ADDRESS
                ON CUSTOMER_ADDRESS.ADDRESS_ID = ADDRESS.ADDRESS_ID
            INNER JOIN COUNTRY
                ON ADDRESS.COUNTRY_ID = COUNTRY.COUNTRY_ID
            WHERE CUSTOMER_ADDRESS.CUSTOMER_ID = {str(t['_id'])}
        """
        cursor.execute(add_query)
        adds = cursor.fetchall()
        for record in adds:
            transformed_record = {
                "_id":int(record[0]),
                "status":{
                    "_id":int(record[2]),
                    "status":record[1]
                },
                "street_number":int(record[3]),
                "street_name":record[4],
                "city":record[5],
                "country":{
                    "_id":int(record[6]),
                    "name":record[7]
                }
            }
            t['addresses'].append(transformed_record)

    print("Processing Orders on Customers ...")
    for t in data:
        ord_query = f"""
        SELECT
            CUST_ORDER.ORDER_ID
        FROM CUST_ORDER
        WHERE CUST_ORDER.CUSTOMER_ID = {str(t['_id'])}
        """
        cursor.execute(ord_query)
        orders = cursor.fetchall()
        for record in orders:
            transformed_record = int(record[0])
            t['orders'].append(transformed_record)

    collection = db["customers"]
    collection.insert_many(data)

    # Orders
    orders_query = """
    SELECT 
        CUST_ORDER.ORDER_ID,
        CUST_ORDER.ORDER_DATE,
        SHIPPING_METHOD.METHOD_ID,
        SHIPPING_METHOD.METHOD_NAME,
        SHIPPING_METHOD.COST
    FROM CUST_ORDER
        INNER JOIN SHIPPING_METHOD
            ON CUST_ORDER.SHIPPING_METHOD_ID = SHIPPING_METHOD.METHOD_ID
    """
    print("Fetching orders ...")
    cursor.execute(orders_query)
    rows = cursor.fetchall()
    print("Processing Orders ...")
    data = transform_data_orders(rows) 
    print("Processing Orders History ...")
    for t in data:
        ord_query = f"""
        SELECT
            ORDER_HISTORY.HISTORY_ID,
            ORDER_HISTORY.ORDER_ID,
            ORDER_HISTORY.STATUS_ID,
            ORDER_HISTORY.STATUS_DATE,
            ORDER_STATUS.STATUS_ID,
            ORDER_STATUS.STATUS_VALUE
        FROM ORDER_HISTORY 
            INNER JOIN ORDER_STATUS
                ON ORDER_HISTORY.STATUS_ID = ORDER_STATUS.STATUS_ID
            WHERE ORDER_HISTORY.ORDER_ID = {str(t['_id'])}
        """
        cursor.execute(ord_query)
        orders = cursor.fetchall()
        for record in orders:
            transformed_record = {
                "_id":int(record[0]),
                "status":{
                    "_id":int(record[2]),
                    "date":record[3].strftime("%Y-%m-%d"),
                    "status":record[5]
                }
            }
            t['history'].append(transformed_record)
    print("Processing Lines ...")
    for t in data:
        l_query = f"""
        SELECT
            ORDER_LINE.LINE_ID,
            ORDER_LINE.BOOK_ID,
            ORDER_LINE.PRICE
         FROM ORDER_LINE
            WHERE ORDER_LINE.ORDER_ID = {str(t['_id'])}
        """
        cursor.execute(l_query)
        orders = cursor.fetchall()
        for record in orders:
            transformed_record = {
                "_id":int(record[0]),
                "book_id":int(record[1]),
                "price":float(record[2])
                }
            t['lines'].append(transformed_record)

    collection = db["orders"]
    collection.insert_many(data)
    print("Done")
    
except Exception as e:
    print(e)