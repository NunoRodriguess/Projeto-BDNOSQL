# Do resultado das nossas pesquisas, entendemos que a melhor correspondência a uma "View" do SQL é uma querie em Cypher


#    -- View list books and their authors

    MATCH (b:Book)-[r:WRITTEN_BY]->(a:Author) 
    RETURN b.id AS BOOK_ID, b.title AS TITLE, a.name AS AUTHOR_NAME 
    LIMIT 25

#   -- View to list orders and their current status

    MATCH (o:Order)-[h:HAS_STATUS]->(s:Status)
    ORDER BY h.status_date DESC
    WITH o, collect({status: s.value, status_date: h.status_date, rel: h})[0] as latest_status
    RETURN o.id AS ORDER_ID, o.order_date AS ORDER_DATE,latest_status.status AS STATUS_VALUE
    LIMIT 25
