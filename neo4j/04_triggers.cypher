validate_email

CALL apoc.trigger.install(
  'neo4j',
  'validate_email',
  '
  // Validate created nodes
  UNWIND $createdNodes AS node
  WITH node
  WHERE "Customer" IN labels(node)
    AND (node.email IS NULL OR NOT node.email =~ "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$")
  CALL apoc.util.validate(
    true,
    "Invalid email format on create: " + coalesce(toString(node.email), "null"),
    []
  )
  RETURN null
  UNION

  // Validate updated nodes
  UNWIND keys($assignedNodeProperties) AS nodeId
  WITH apoc.node.fromNodeId(toInteger(nodeId)) AS node
  WHERE "Customer" IN labels(node)
    AND exists(node.email)
    AND (node.email IS NULL OR NOT node.email =~ "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$")
  CALL apoc.util.validate(
    true,
    "Invalid email format on update: " + coalesce(toString(node.email), "null"),
    []
  )
  RETURN null
  ',
  {phase: "rollback"}
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

