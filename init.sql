-- Create departments table
CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(255)
);

-- Create categories table
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_department_id INTEGER,
    category_name VARCHAR(255),
    FOREIGN KEY (category_department_id) REFERENCES departments(department_id)
);

-- Create products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_category_id INTEGER,
    product_name VARCHAR(255),
    product_description VARCHAR(255),
    product_price NUMERIC(10, 2),
    product_image VARCHAR(255),
    FOREIGN KEY (product_category_id) REFERENCES categories(category_id)
);

-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_fname VARCHAR(255),
    customer_lname VARCHAR(255),
    customer_email VARCHAR(255),
    customer_password VARCHAR(255),
    customer_street VARCHAR(255),
    customer_city VARCHAR(255),
    customer_state VARCHAR(255),
    customer_zipcode INTEGER
);

-- Create orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_customer_id INTEGER,
    order_status VARCHAR(255),
    FOREIGN KEY (order_customer_id) REFERENCES customers(customer_id)
);

-- Create order_items table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_item_order_id INTEGER,
    order_item_product_id INTEGER,
    order_item_quantity INTEGER,
    order_item_subtotal NUMERIC(10, 2),
    order_item_product_price NUMERIC(10, 2),
    FOREIGN KEY (order_item_order_id) REFERENCES orders(order_id),
    FOREIGN KEY (order_item_product_id) REFERENCES products(product_id)
);