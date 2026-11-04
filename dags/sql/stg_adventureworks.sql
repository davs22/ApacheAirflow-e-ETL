CREATE SCHEMA IF NOT EXISTS stg;

DROP TABLE IF EXISTS stg.SalesOrderDetail CASCADE;
DROP TABLE IF EXISTS stg.SalesOrderHeader CASCADE;
DROP TABLE IF EXISTS stg.Customer CASCADE;
DROP TABLE IF EXISTS stg.SalesTerritory CASCADE;
DROP TABLE IF EXISTS stg.Product CASCADE;
DROP TABLE IF EXISTS stg.ProductSubcategory CASCADE;
DROP TABLE IF EXISTS stg.ProductCategory CASCADE;

CREATE TABLE stg.ProductCategory (
    ProductCategoryID INT PRIMARY KEY, Name VARCHAR(50), ModifiedDate TIMESTAMP
);

CREATE TABLE stg.ProductSubcategory (
    ProductSubcategoryID INT PRIMARY KEY, ProductCategoryID INT, Name VARCHAR(50), ModifiedDate TIMESTAMP
);

CREATE TABLE stg.Product (
    ProductID INT PRIMARY KEY, Name VARCHAR(50), ProductNumber VARCHAR(25), 
    ProductSubcategoryID INT, StandardCost NUMERIC(15,2), ModifiedDate TIMESTAMP
);

CREATE TABLE stg.SalesTerritory (
    TerritoryID INT PRIMARY KEY, Name VARCHAR(50), CountryRegionCode VARCHAR(3), 
    "group" VARCHAR(50), ModifiedDate TIMESTAMP
);

CREATE TABLE stg.Customer (
    CustomerID INT PRIMARY KEY, PersonID INT, StoreID INT, TerritoryID INT, ModifiedDate TIMESTAMP
);

CREATE TABLE stg.SalesOrderHeader (
    SalesOrderID INT PRIMARY KEY, OrderDate TIMESTAMP, CustomerID INT, 
    TerritoryID INT, ModifiedDate TIMESTAMP
);

CREATE TABLE stg.SalesOrderDetail (
    SalesOrderID INT, SalesOrderDetailID INT PRIMARY KEY, OrderQty INT, 
    ProductID INT, UnitPrice NUMERIC(15,2), UnitPriceDiscount NUMERIC(15,2), 
    LineTotal NUMERIC(15,2), ModifiedDate TIMESTAMP
);
