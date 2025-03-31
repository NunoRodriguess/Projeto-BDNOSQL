CREATE OR REPLACE PROCEDURE update_order_status (
    p_order_id IN INT,
    p_status_id IN INT
) AS
    v_previous_status_id INT;
BEGIN
    SELECT status_id
    INTO v_previous_status_id
    FROM order_history
    WHERE order_id = p_order_id
    ORDER BY status_date DESC
    FETCH FIRST ROW ONLY;

    IF v_previous_status_id != p_status_id THEN

        INSERT INTO order_history (history_id, order_id, status_id, status_date)
        VALUES (seq_orderhist.NEXTVAL, p_order_id, p_status_id, SYSDATE);
        
        DBMS_OUTPUT.PUT_LINE('Status atualizado com sucesso para o pedido ' || p_order_id);
    ELSE
        DBMS_OUTPUT.PUT_LINE('O status do pedido ' || p_order_id || ' já está como ' || p_status_id);
    END IF;
END;

