CREATE TABLE customers (
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    email VARCHAR(100)  );

CREATE TABLE orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_amount INT,
    order_date DATE  );
