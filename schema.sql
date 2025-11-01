/*
=========================================================================
    BMW Sales Database Schema Creation.
=========================================================================

Databse Name: bmw_sales_db
-- Description:
-- This script creates the MySQL database 'bmw_sales_db' and the table
-- 'car_sales' based on the results of a data audit of the sales CSV file.
-- The table columns are designed with appropriate data types:
--   - VARCHAR for categorical/text fields
--   - INT for whole numbers (years, mileage, sales volume)
--   - DECIMAL for numeric fields with decimals (price, engine size)
-- The primary key is 'id', and special characters in column names
-- have been sanitized to follow standard conventions.

Table: car_sales for(Model,Year,Region,Color,Fuel_Type,Transmission,Engine_Size_L,Mileage_KM,Price_USD,Sales_Volume,Sales_Classification)
-- Description:
-- This table contains sales data for BMW cars including model,year, region, color, fuel type, transmission, engine size, mileage, 
   price,sales volume, and sales classification.
-- Columns:
    - id: INT, Primary Key, Auto Increment
       - model: VARCHAR(50)
       - year: INT
       - region: VARCHAR(50)
       - color: VARCHAR(30)
       - fuel_type: VARCHAR(20)
       - transmission: VARCHAR(20)
       - engine_size_l: DECIMAL(3,1)
       - mileage_km: INT
       - price_usd: DECIMAL(10,2)
       - sales_volume: INT
       - sales_classification: VARCHAR(20)
   
===========================================================================
*/

-- Database creation if not exists
CREATE DATABASE IF NOT EXISTS bmw_sales_db;
USE bmw_sales_db; -- Switch to the created database

-- Table creation
DROP TABLE IF EXISTS car_sales; -- Drop table if it already exists
CREATE TABLE car_sales (
    Model VARCHAR(50) NOT NULL,
    Year INT NOT NULL,
    Region VARCHAR(50),
    Color VARCHAR(30),
    Fuel_Type VARCHAR(20),
    Transmission VARCHAR(20),
    Engine_Size_L DECIMAL(3,1),
    Mileage_KM INT,
    Price_USD DECIMAL(10,2),
    Sales_Volume INT,
    Sales_Classification VARCHAR(20)
);

-- Verify table creation
SHOW TABLES;
DESCRIBE car_sales;