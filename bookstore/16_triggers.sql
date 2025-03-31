CREATE OR REPLACE TRIGGER validate_email
BEFORE INSERT OR UPDATE ON customer
FOR EACH ROW
BEGIN
    IF NOT REGEXP_LIKE(:NEW.email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN
        RAISE_APPLICATION_ERROR(-20003, 'Invalid email format.');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER insert_order_history
AFTER INSERT ON cust_order
FOR EACH ROW
BEGIN
    INSERT INTO order_history (order_id, status_id, status_date)
    VALUES (:NEW.order_id, 1 , SYSDATE);
END;
/


CREATE OR REPLACE TRIGGER prevent_book_deletion
BEFORE DELETE ON book
FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM order_line WHERE book_id = :OLD.book_id;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Cannot delete book as it exists in orders.');
    END IF;
END;
/


