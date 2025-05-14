from neo4j import GraphDatabase
import time
import psutil
import os
from tabulate import tabulate

# Neo4j connection
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "bookstore"
DRIVER = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

REPETITIONS = 10
PROCESS = psutil.Process(os.getpid())

# Dictionary of Cypher queries
QUERIES = {
    "Query 1: Bestselling Books in Portugal": """
        MATCH (o:Order)-[c:CONTAINS]->(b:Book)
        WITH o, c, b
        MATCH (o:Order)-[:TO]->(a:Address {country:"Portugal"})
        RETURN b.id, b.title, a.country, count(c) AS Sales
        ORDER BY Sales DESC
    """,
    
    "Query 2: Authors Who Earned the Most": """
        MATCH (b:Book)-[:WRITTEN_BY]->(a:Author)
        WITH b, count(a) AS num_authors
        MATCH (b)<-[c:CONTAINS]-(o:Order)
        WITH b, num_authors, sum(c.price) AS book_total
        MATCH (b)-[:WRITTEN_BY]->(author:Author)
        WITH author, book_total/num_authors AS author_earnings
        RETURN 
            author.id AS AUTHOR_ID,
            author.name AS AUTHOR_NAME,
            round(sum(author_earnings), 2) AS EARNINGS
        ORDER BY EARNINGS DESC
    """,
    
    "Query 3: Country Where the 'Harper' Group Sold the Most": """
        MATCH (b:Book)<-[:CONTAINS]-(o:Order)
        WITH b, o
        MATCH (o)-[:TO]->(a:Address)
        WHERE b.publisher CONTAINS 'Harper'
        RETURN a.country, count(*) as SALES
        ORDER BY SALES DESC
    """,
    
    "Query 4: Customers from China and Their Orders": """
        MATCH (c:Customer)-[:LIVES]->(a:Address)
        WHERE a.country = "China"
        MATCH (c)-[:PLACED]->(o:Order)
        WITH c, o
        OPTIONAL MATCH (o)-[cont:CONTAINS]->(b:Book)
        WITH c, o, COLLECT({book: b, price: cont.price}) AS lines, SUM(cont.price) AS total_value
        WITH c, o, total_value, SIZE(lines) AS total_items
        OPTIONAL MATCH (o)-[h:HAS_STATUS]->(s:Status)
        WITH c, o, total_value, total_items, COLLECT(h.status_date) AS status_dates
        WITH c, o, total_value, total_items, status_dates,
             REDUCE(latest = null, date IN status_dates | 
                 CASE WHEN latest IS NULL OR date > latest THEN date ELSE latest END
             ) AS last_status_date
        WITH c, o, total_value, total_items, last_status_date,
             CASE 
               WHEN last_status_date IS NOT NULL 
               THEN duration.inDays(datetime(last_status_date), datetime()).days
               ELSE NULL 
             END AS days_since_last_status
        WITH c, 
             COLLECT({
               _id: o.id,
               total_value: total_value,
               total_items: total_items,
               days_since_last_status: days_since_last_status
             }) AS orders
        RETURN {
          _id: c.id,
          first_name: c.first_name,
          last_name: c.last_name,
          email: c.email,
          orders: orders
        } AS result
        ORDER BY c.id
    """
}

def run_benchmark(driver, query, query_name):
    times = []
    cpu_usages = []
    mem_usages = []

    with driver.session() as session:
        for i in range(REPETITIONS):
            PROCESS.cpu_percent(interval=None)  # Reset CPU
            start_mem = PROCESS.memory_info().rss / (1024 ** 2)  # MB
            start_time = time.time()

            list(session.run(query))  # Execute Cypher query

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
        result = run_benchmark(DRIVER, query, query_name)
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
    
    print("\n=== Neo4j Benchmark Summary ===")
    print(tabulate(table_data, headers=headers, tablefmt="github"))

if __name__ == "__main__":
    main()