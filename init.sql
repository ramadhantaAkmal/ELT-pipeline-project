-- Create departments table
CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create categories table
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_department_id INTEGER,
    category_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    customer_zipcode INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_customer_id INTEGER,
    order_status VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_item_order_id) REFERENCES orders(order_id),
    FOREIGN KEY (order_item_product_id) REFERENCES products(product_id)
);

-- Insert data into departments (3 departments)
INSERT INTO departments (department_id, department_name) VALUES
(1, 'Electronics'),
(2, 'Clothing'),
(3, 'Books');

-- For Electronics (department_id 1)
INSERT INTO categories (category_id, category_department_id, category_name) VALUES
(1, 1, 'Smartphones'),
(2, 1, 'Laptops'),
(3, 1, 'Tablets'),
(4, 1, 'Audio Devices'),
(5, 1, 'Accessories');

-- For Clothing (department_id 2)
INSERT INTO categories (category_id, category_department_id, category_name) VALUES
(6, 2, 'Mens Wear'),
(7, 2, 'Womens Wear'),
(8, 2, 'Kids Wear'),
(9, 2, 'Shoes'),
(10, 2, 'Bags');

-- For Books (department_id 3)
INSERT INTO categories (category_id, category_department_id, category_name) VALUES
(11, 3, 'Fiction'),
(12, 3, 'Non-Fiction'),
(13, 3, 'Science'),
(14, 3, 'History'),
(15, 3, 'Childrens Books');