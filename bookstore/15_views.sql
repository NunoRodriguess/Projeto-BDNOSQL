-- View list books and their authors
CREATE OR REPLACE VIEW book_with_authors AS
SELECT b.book_id, b.title, a.author_name
FROM book b
JOIN book_author ba ON b.book_id = ba.book_id
JOIN author a ON ba.author_id = a.author_id;

-- View to list orders and their current status
CREATE OR REPLACE VIEW orders_with_status AS
SELECT o.order_id, o.order_date, os.status_value
FROM cust_order o
JOIN order_history oh ON o.order_id = oh.order_id
JOIN order_status os ON oh.status_id = os.status_id
WHERE oh.status_date = (SELECT MAX(status_date) 
                        FROM order_history 
                        WHERE order_id = o.order_id);

COMMIT;
