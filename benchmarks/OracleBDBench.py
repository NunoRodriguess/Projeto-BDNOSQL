import oracledb
import time

# Connection settings
USERNAME = "bookstore"
PASSWORD = "bookstore"
DSN = "localhost:1521/ORCLCDB"

# Number of repetitions for each query
REPETITIONS = 10

# Dictionary of queries 
QUERIES = {
    "Query 1: Bestselling Books in Portugal": '''
        SELECT 
            BOOK.BOOK_ID,
            BOOK.TITLE,
            COUNTRY.COUNTRY_NAME,
            COUNT(CUST_ORDER.ORDER_ID) AS SALES
        FROM 
            BOOK
            INNER JOIN ORDER_LINE ON BOOK.BOOK_ID = ORDER_LINE.BOOK_ID
            INNER JOIN CUST_ORDER ON ORDER_LINE.ORDER_ID = CUST_ORDER.ORDER_ID
            INNER JOIN ADDRESS ON CUST_ORDER.DEST_ADDRESS_ID = ADDRESS.ADDRESS_ID
            INNER JOIN COUNTRY ON ADDRESS.COUNTRY_ID = COUNTRY.COUNTRY_ID
        WHERE 
            COUNTRY.COUNTRY_NAME = 'Portugal'
        GROUP BY 
            BOOK.BOOK_ID, BOOK.TITLE, COUNTRY.COUNTRY_NAME
        ORDER BY 
            SALES DESC
    ''',

    "Query 2: Authors Who Earned the Most": '''
        SELECT 
            AUTHOR.AUTHOR_ID,
            AUTHOR.AUTHOR_NAME,
            ROUND(SUM(BOOKSALES.TOTAL/BOOKSALES.NUM_AUTHORS), 2) AS EARNINGS 
        FROM (
            SELECT 
                BOOK.BOOK_ID,
                SUM(ORDER_LINE.PRICE) as TOTAL,
                (SELECT COUNT(*) FROM BOOK_AUTHOR WHERE BOOK_AUTHOR.BOOK_ID = BOOK.BOOK_ID) AS NUM_AUTHORS
            FROM BOOK
            INNER JOIN ORDER_LINE ON BOOK.BOOK_ID = ORDER_LINE.BOOK_ID
            GROUP BY BOOK.BOOK_ID
        ) BOOKSALES
        INNER JOIN BOOK_AUTHOR ON BOOKSALES.BOOK_ID = BOOK_AUTHOR.BOOK_ID
        INNER JOIN AUTHOR ON BOOK_AUTHOR.AUTHOR_ID = AUTHOR.AUTHOR_ID
        GROUP BY AUTHOR.AUTHOR_ID, AUTHOR.AUTHOR_NAME
        ORDER BY EARNINGS DESC
    ''',

    "Query 3: Country Where the 'Harper' Group Sold the Most": '''
        SELECT 
            COUNTRY.COUNTRY_NAME,
            COUNT(ORDER_LINE.BOOK_ID) AS SALES
        FROM BOOK
            INNER JOIN PUBLISHER ON BOOK.PUBLISHER_ID = PUBLISHER.PUBLISHER_ID
            INNER JOIN ORDER_LINE ON BOOK.BOOK_ID = ORDER_LINE.BOOK_ID
            INNER JOIN CUST_ORDER ON ORDER_LINE.ORDER_ID = CUST_ORDER.ORDER_ID
            INNER JOIN ADDRESS ON CUST_ORDER.DEST_ADDRESS_ID = ADDRESS.ADDRESS_ID
            INNER JOIN COUNTRY ON ADDRESS.COUNTRY_ID = COUNTRY.COUNTRY_ID
        WHERE 
            PUBLISHER.PUBLISHER_NAME LIKE '%Harper%'
        GROUP BY COUNTRY.COUNTRY_NAME
        ORDER BY SALES DESC
    ''',

    "Query 4: Customers from China and Their Orders": '''
        SELECT 
            c.customer_id, 
            c.first_name, 
            c.last_name,
            c.email, 
            o.order_id,

            -- Total order value
            (SELECT SUM(ol_inner.price) 
            FROM order_line ol_inner 
            WHERE ol_inner.order_id = o.order_id) AS total_value,

            -- Number of items in the order
            (SELECT COUNT(*) 
            FROM order_line ol_inner 
            WHERE ol_inner.order_id = o.order_id) AS total_items,

            -- Days since last status update
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM order_history oh_inner 
                    WHERE oh_inner.order_id = o.order_id
                ) THEN 
                    ROUND(CURRENT_DATE - (
                        SELECT MAX(oh_inner.status_date) 
                        FROM order_history oh_inner 
                        WHERE oh_inner.order_id = o.order_id
                    ), 0)
                ELSE NULL
            END AS days_since_last_status

        FROM 
            customer c
        JOIN 
            cust_order o ON o.customer_id = c.customer_id

        WHERE 
            EXISTS (
                SELECT 1 
                FROM customer_address ca
                JOIN address a ON a.address_id = ca.address_id
                JOIN country co ON a.country_id = co.country_id
                WHERE ca.customer_id = c.customer_id
                AND co.country_id = 42
            )

        ORDER BY 
            c.customer_id, o.order_id
    '''
}

def run_benchmark(cursor, query, query_name):
    times = []
    print(f"\n Running benchmark: {query_name}")

    for i in range(REPETITIONS):
        start_time = time.time()
        cursor.execute(query)
        cursor.fetchall()
        end_time = time.time()
        duration = end_time - start_time
        times.append(duration)
        print(f"  Execution {i+1:02d}: {duration:.4f} seconds")

    average_time = sum(times) / len(times)
    print(f"Average time ({query_name}): {average_time:.4f} seconds")

def main():
    try:
        connection = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=DSN)
        cursor = connection.cursor()

        for query_name, query in QUERIES.items():
            run_benchmark(cursor, query, query_name)

        cursor.close()
        connection.close()

    except oracledb.DatabaseError as e:
        print("Error connecting or executing:", e)

if __name__ == "__main__":
    main()
