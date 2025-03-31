CREATE INDEX idx_book_title ON book(title);
CREATE INDEX idx_customer_email ON customer(email);
CREATE INDEX idx_order_date ON cust_order(order_date);
CREATE INDEX idx_address_country ON address(country_id);
COMMIT;