import oracledb

try:
    connection = oracledb.connect(
        user="bookstore",
        password="bookstore",
        dsn="localhost/orclcdb"
    )
    
    cursor = connection.cursor()
    
    cursor.execute("SELECT 'Connection Successful!' FROM DUAL")
    
    result = cursor.fetchone()
    print(result[0])
    
    cursor.close()
    connection.close()
    
except Exception as e:
    print(e)