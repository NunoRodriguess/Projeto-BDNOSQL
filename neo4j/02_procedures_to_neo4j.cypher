CALL apoc.custom.declareProcedure(
  'update_order_status(p_order_id :: INTEGER, p_status_id :: INTEGER) :: (out :: STRING)',
  '
    MATCH (o:Order {id: $p_order_id})
    MATCH (s:Status {id: $p_status_id})
    OPTIONAL MATCH (o)-[h:HAS_STATUS]->(s)
    WITH o, s, h, $p_order_id AS orderId, $p_status_id AS statusId

    CALL apoc.do.when(
      h IS NULL,
      "
        MERGE (o)-[rel:HAS_STATUS]->(s)
        SET rel.status_date = date()
        RETURN \'Status atualizado com sucesso para o pedido \' + toString(orderId) AS out
      ",
      "
        RETURN \'O status do pedido \' + toString(orderId) + \' já está como \' + toString(statusId) AS out
      ",
      {o: o, s: s, orderId: orderId, statusId: statusId}
    ) YIELD value

    RETURN value.out AS out
  ',
  'write'
)
