validate_email

CALL apoc.trigger.install(
  'neo4j',
  'validate_email',
  '
   UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, "email") AS prop
   CALL apoc.util.validate(
     NOT prop.new =~ "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$", 
     "Invalid email format. Got %s", 
     [prop.new]
   ) RETURN null
   ',
  {phase: 'before'}
)


insert_order_history


CALL apoc.trigger.install(
   'neo4j',
   'insert_order_history',
   '
   UNWIND $createdNodes AS node
   WITH node
   WHERE "Order" IN labels(node)
   Merge (node)-[rel:HAS_STATUS {status_date:date()}]->(:Status {id:1})
  ',
    {phase:"before"}

) 


prevent_book_deletion

CALL apoc.trigger.install(
   'neo4j',
   'prevent_book_deletion',
   '
   UNWIND $deletedNodes AS node
   WITH node
   WHERE "Book" IN labels(node)
   OPTIONAL MATCH (:Order)-[c:CONTAINS]->(node)
   CASE c
    WHEN IS NULL "ok"
    WHEN IS NOT NULL "Cannot delete book as it exists in orders."
   END AS result
  ',
    {phase:"before"}

) 

