CREATE DATABASE sales;

\c sales

-- Create schema for raw data
CREATE SCHEMA IF NOT EXISTS raw;

-- Create raw sales table
CREATE TABLE IF NOT EXISTS raw.sales (
    "SaleID" VARCHAR(50),
    "ProductID" VARCHAR(50),
    "ProductName" VARCHAR(255),
    "Brand" VARCHAR(100),
    "Category" VARCHAR(100),
    "RetailerID" VARCHAR(50),
    "RetailerName" VARCHAR(255),
    "Channel" VARCHAR(50),
    "Location" VARCHAR(255),
    "Quantity" VARCHAR(50),
    "Price" VARCHAR(100),
    "Date" VARCHAR(50),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(100),
    source_file VARCHAR(255)
);
