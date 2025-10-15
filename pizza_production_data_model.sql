CREATE SCHEMA IF NOT EXISTS pizza_factory;
SET search_path TO pizza_factory;

-- Ingredient master
CREATE TABLE ingredient (
    ingredient_id SERIAL PRIMARY KEY,
    ingredient_name VARCHAR(100) NOT NULL,
    unit_cost NUMERIC(10,2) NOT NULL,
    unit VARCHAR(10) NOT NULL
);

-- Pizza type master
CREATE TABLE pizza_type (
    pizza_type_id SERIAL PRIMARY KEY,
    pizza_name VARCHAR(100) NOT NULL,
    base_price NUMERIC(10,2) NOT NULL,
    size VARCHAR(20),
    category VARCHAR(50)
);

-- Recipe linking pizza to ingredients
CREATE TABLE pizza_recipe (
    recipe_id SERIAL PRIMARY KEY,
    pizza_type_id INT REFERENCES pizza_type(pizza_type_id),
    ingredient_id INT REFERENCES ingredient(ingredient_id),
    quantity_used NUMERIC(10,2) NOT NULL
);

-- Production batches
CREATE TABLE production_batch (
    batch_id SERIAL PRIMARY KEY,
    pizza_type_id INT REFERENCES pizza_type(pizza_type_id),
    batch_date DATE NOT NULL,
    quantity_produced INT NOT NULL,
    labor_cost NUMERIC(10,2),
    energy_cost NUMERIC(10,2)
);

-- Sales data
CREATE TABLE pizza_sales (
    sale_id SERIAL PRIMARY KEY,
    pizza_type_id INT REFERENCES pizza_type(pizza_type_id),
    sale_date DATE NOT NULL,
    quantity_sold INT NOT NULL,
    sale_price NUMERIC(10,2)
);
