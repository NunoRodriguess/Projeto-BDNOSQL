"""
CREATE INDEX idx_book_title ON book(title);
CREATE INDEX idx_customer_email ON customer(email);
CREATE INDEX idx_order_date ON cust_order(order_date);
CREATE INDEX idx_address_country ON address(country_id);
COMMIT;

"""
from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(username, password))

def create_index(tx):
    tx.run("CREATE INDEX idx_book_title IF NOT EXISTS FOR (n:Book) ON (n.title)")
    tx.run("CREATE INDEX idx_customer_email IF NOT EXISTS FOR (n:Customer) ON (n.email)")
    tx.run("CREATE INDEX idx_order_date IF NOT EXISTS FOR (n:Order) ON (n.order_date)")
    tx.run("CREATE INDEX idx_address_country IF NOT EXISTS FOR (n:Address) ON (n.country)")

with driver.session() as session:
    session.execute_write(create_index)

driver.close()
