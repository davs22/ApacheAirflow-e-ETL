CREATE SCHEMA IF NOT EXISTS stg;


CREATE TABLE IF NOT EXISTS stg.ProductCategory (
    ProductCategoryID INT PRIMARY KEY, Name VARCHAR(50), ModifiedDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.ProductSubcategory (
    ProductSubcategoryID INT PRIMARY KEY, ProductCategoryID INT, Name VARCHAR(50), ModifiedDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.Product (
    ProductID INT PRIMARY KEY, Name VARCHAR(50), ProductNumber VARCHAR(25),
    ProductSubcategoryID INT, StandardCost NUMERIC(19,4), ModifiedDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.SalesTerritory (
    TerritoryID INT PRIMARY KEY, Name VARCHAR(50), CountryRegionCode VARCHAR(3),
    "group" VARCHAR(50), ModifiedDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.Customer (
    CustomerID INT PRIMARY KEY, PersonID INT, StoreID INT, TerritoryID INT, ModifiedDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.SalesOrderHeader (
    SalesOrderID INT PRIMARY KEY, OrderDate TIMESTAMP, CustomerID INT,
    TerritoryID INT, ModifiedDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.SalesOrderDetail (
    SalesOrderID INT, SalesOrderDetailID INT PRIMARY KEY, OrderQty INT,
    ProductID INT, UnitPrice NUMERIC(19,4), UnitPriceDiscount NUMERIC(19,4),
    LineTotal NUMERIC(38,6), ModifiedDate TIMESTAMP
);
ALTER TABLE stg.customer ADD COLUMN IF NOT EXISTS nome_cliente VARCHAR(255);
CREATE SCHEMA IF NOT EXISTS etl;
CREATE TABLE IF NOT EXISTS etl.watermark(tabela TEXT PRIMARY KEY, watermark TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS etl.extraction_log(
    id BIGSERIAL PRIMARY KEY, run_id TEXT NOT NULL, tabela TEXT NOT NULL,
    limite_inferior TIMESTAMP, limite_superior TIMESTAMP NOT NULL,
    lidos INT NOT NULL, alterados INT NOT NULL, criado_em TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);
