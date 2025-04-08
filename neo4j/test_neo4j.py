from neo4j import GraphDatabase

uri = "bolt://localhost:7687"  # Change to your Neo4j server URI
username = "neo4j"             # Default username, change if needed
password = "password"          # Change to your password

# Create a driver instance
driver = GraphDatabase.driver(uri, auth=(username, password))

# Function to execute a test query
def test_connection():
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN count(n) AS node_count")
        record = result.single()
        print(f"Connection successful! Database contains {record['node_count']} nodes.")

# Test the connection
try:
    test_connection()
except Exception as e:
    print(f"Connection failed: {e}")
finally:
    # Always close the driver when done
    driver.close()