import oracledb
from neo4j import GraphDatabase



# Transform data from Oracle to MongoDB format
def transform_data_book(data):
    transformed_data = []
    for record in data:
        transformed_record = {
            "_id": int(record[0]),
            "title": record[1],
            "isbn13": int(record[2]),
            "language_code": record[4],
            "language_name": record[5],
            "publisher":record[7],
        }
        transformed_data.append(transformed_record)
    return transformed_data

# Transform data from Oracle to MongoDB format
def transform_data_author(data):
    transformed_data = []
    for record in data:
        transformed_record = {
            "_id": int(record[0]),
            "name": record[1],
        }
        transformed_data.append(transformed_record)
    return transformed_data


def transform_data_order(data):
    transformed_data = []
    for record in data:
        transformed_record = {
            "_id": int(record[0]),
            "order_date": record[1].strftime("%Y-%m-%d"),
            "method_name": record[3],
            "method_cost": float(record[4]),
        }
        transformed_data.append(transformed_record)
    return transformed_data

def transform_data_order_status(data):
    transformed_data = []
    for record in data:
        transformed_record = {
            "_id": int(record[0]),
            "value": record[1]
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
        }
        transformed_data.append(transformed_record)
    return transformed_data

def transform_data_addresses (data):
    transformed_data = []
    for record in data:
        transformed_record = {
            "_id": int(record[0]),
            "street_number": int(record[1]),
            "street_name": record[2],
            "city": record[3],
            "country":record[6]
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
    uri = "bolt://localhost:7687"  # Change to your Neo4j server URI
    username = "neo4j"             # Default username
    password = "password"          
    
    cursor = connection.cursor()
    # Create a driver instance
    driver = GraphDatabase.driver(uri, auth=(username, password))
    

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
    data_book = transform_data_book(rows)
    print("Fetching Authors ...")
    author_query = """
    SELECT * FROM AUTHOR
    """
    cursor.execute(author_query)
    authors = cursor.fetchall()
    print("Processing Authors ...")
    data_authors = transform_data_author(authors)

    print("Fetching orders ...")
    order_query = """
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
    cursor.execute(order_query)
    rows = cursor.fetchall()
    #transformar os dados
    print("Processing orders ...")
    data_order = transform_data_order(rows)

    print("Fetching orders status...")
    status_query = """
    SELECT
        ORDER_STATUS.STATUS_ID,
        ORDER_STATUS.STATUS_VALUE
    FROM ORDER_STATUS 
    """
    cursor.execute(status_query)
    rows = cursor.fetchall()
    #transformar os dados
    print("Processing orders status ...")
    data_order_status = transform_data_order_status(rows)

    print("Fetching customers ...")
    customers_query = """
    SELECT 
        CUSTOMER.CUSTOMER_ID,
        CUSTOMER.FIRST_NAME,
        CUSTOMER.LAST_NAME,
        CUSTOMER.EMAIL
    FROM CUSTOMER
    """
    cursor.execute(customers_query)
    rows = cursor.fetchall()
    #transformar os dados
    print("Processing customers ...")
    data_order_customers = transform_data_customers(rows)

    print("Fetching addresses ...")
    addresses_query = """
        SELECT * FROM ADDRESS INNER JOIN COUNTRY ON ADDRESS.COUNTRY_ID = COUNTRY.COUNTRY_ID
    """
    cursor.execute(addresses_query)
    rows = cursor.fetchall()
    #transformar os dados
    print("Processing addresses ...")
    data_addresses = transform_data_addresses(rows)









    #mandar os dados
    print("NEO4J Nodes ...")
    # Send the data
    print("Sending books to Neo4j ...")
    for item in data_book:  # Iterate through items directly, not indices
        records, summary, keys = driver.execute_query(
            """
            CREATE (b:Book {id: $id, title: $title, isbn13: $isbn13, language_code: $language_code, 
                    language_name: $language_name, publisher: $publisher})
            """,
            id=item['_id'],         
            title=item['title'],
            isbn13=item['isbn13'],
            language_code=item['language_code'],
            language_name=item['language_name'],
            publisher=item['publisher'],
            database_="neo4j",
        )
    print("Sending Authors to Neo4j ...")
    for item in data_authors:  # Iterate through items directly, not indices
        records, summary, keys = driver.execute_query(
            """
            CREATE (a:Author {id: $id,
            name: $name})
            """,
            id=item['_id'],         
            name=item['name'],
            database_="neo4j",
        )
    print("Sending Orders to Neo4j ...")
    for item in data_order:  # Iterate through items directly, not indices
        records, summary, keys = driver.execute_query(
            """
            CREATE (o:Order {id: $id,
            order_date: $order_date,
            method_name: $method_name,
            method_cost: $method_cost})
            """,
            id=item['_id'],         
            order_date=item['order_date'],
            method_name=item['method_name'],
            method_cost=item['method_cost'],
            database_="neo4j",
        )

    print("Sending Status to Neo4j ...")
    for item in data_order_status:  # Iterate through items directly, not indices
        records, summary, keys = driver.execute_query(
            """
            CREATE (s:Status {id: $id,
            value: $value})
            """,
            id=item['_id'],         
            value=item['value'],
            database_="neo4j",
        )

    print("Sending Customers to Neo4j ...")
    for item in data_order_customers:  # Iterate through items directly, not indices
        records, summary, keys = driver.execute_query(
            """
            CREATE (c:Customer {id: $id,
            first_name: $first_name,last_name: $last_name,
            email: $email})
            """,
            id=item['_id'],         
            first_name=item['first_name'],
            last_name=item['last_name'],
            email=item['email'],
            database_="neo4j",
        )

    print("Sending Addresses to Neo4j ...")
    for item in data_addresses:  # Iterate through items directly, not indices
        records, summary, keys = driver.execute_query(
            """
            CREATE (a:Address {id: $id,
            street_number: $street_number,
            street_name: $street_name,
            city: $city,
            country: $country})
            """,
            id=item['_id'],         
            street_number=item['street_number'],
            street_name=item['street_name'],
            city=item['city'],
            country=item['country'],
            database_="neo4j",
        )
    

    print("Creating Relationships ...")
    # Create relationships between books and authors
    # Sql query on book_author table
    print("Creating Relationships between books and authors ...")
    book_author_query = """
    SELECT BOOK_AUTHOR.BOOK_ID,
        BOOK_AUTHOR.AUTHOR_ID
    FROM BOOK_AUTHOR
    """
    cursor.execute(book_author_query)
    rows = cursor.fetchall()
    #transformar os dados
    for record in rows:
        records, summary, keys = driver.execute_query(
            """
            MATCH (b:Book {id: $book_id}), (a:Author {id: $author_id})
            CREATE (b)-[:WRITTEN_BY]->(a)
            """,
            book_id=record[0],
            author_id=record[1],
            database_="neo4j",
        )
    # Create relationships between orders and customers
    # Sql query on cust_order table
    print("Creating Relationships between orders and customers and orders and address ...")
    cust_order_query = """
    SELECT * FROM CUST_ORDER
    """
    cursor.execute(cust_order_query)
    rows = cursor.fetchall()
    #transformar os dados
    for record in rows:
        records, summary, keys = driver.execute_query(
            """
            MATCH (o:Order {id: $order_id}), (c:Customer {id: $customer_id})
            CREATE (c)-[:PLACED]->(o)
            """,
            order_id=record[0],
            customer_id=record[2],
            database_="neo4j",
        )
                # Create relationships between orders and addresses
        records, summary, keys = driver.execute_query(
            """
            MATCH (o:Order {id: $order_id}), (a:Address {id: $address_id})
            CREATE (o)-[:TO]->(a)
            """,
            order_id=record[0],
            address_id=record[4],
            database_="neo4j",
        )
    # Create relationships between orders and status
    # Sql query on order_status table
    print("Creating Relationships between orders and status ...")
    order_status_query = """
    SELECT * FROM ORDER_HISTORY
    """
    cursor.execute(order_status_query)
    rows = cursor.fetchall()
    #transformar os dados
    for record in rows:
        records, summary, keys = driver.execute_query(
            """
            MATCH (o:Order {id: $order_id}), (s:Status {id: $status_id})
            CREATE (o)-[:HAS_STATUS {status_date:$status_date}]->(s)
            """,
            order_id=record[1],
            status_id=record[2],
            status_date=record[3].strftime("%Y-%m-%d"),
            database_="neo4j",
        )
    
    print("Creating Relationships between customers and addresses ...")
    # Create relationships between customers and addresses
    # Sql query on customer_address table
    customer_address_query = """
    SELECT * FROM CUSTOMER_ADDRESS
    INNER JOIN ADDRESS_STATUS ON CUSTOMER_ADDRESS.STATUS_ID = ADDRESS_STATUS.STATUS_ID
    """
    cursor.execute(customer_address_query)
    rows = cursor.fetchall()
    #transformar os dados
    for record in rows:
        records, summary, keys = driver.execute_query(
            """
            MATCH (c:Customer {id: $customer_id}), (a:Address {id: $address_id})
            CREATE (c)-[:LIVES {status:$status}]->(a)
            """,
            customer_id=record[0],
            address_id=record[1],
            status=record[4],
            database_="neo4j",
        )

    print("Creating Relationships between books and orders ...")
    # Create relationships between books and orders
    # Sql query on book_order table
    book_order_query = """
    SELECT * FROM ORDER_LINE
    """
    cursor.execute(book_order_query)
    rows = cursor.fetchall()
    #transformar os dados
    for record in rows:
        records, summary, keys = driver.execute_query(
            """
            MATCH (b:Book {id: $book_id}), (o:Order {id: $order_id})
            CREATE (o)-[:CONTAINS {price: $price}]->(b)
            """,
            book_id=record[2],
            order_id=record[1],
            price = float(record[3]),
            database_="neo4j",
        )
except Exception as e:
    print(e)