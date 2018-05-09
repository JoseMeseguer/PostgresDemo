-- cambio del esquema por defecto para la simplificacion de nombres
-- como administradores debemos controlar en todo momento nuestro esquema de trabajo
-- por lo que he insistido en que lo escribamos para que nos quede grabado
set search_path to schemaTesteo;

-- FUNCTION CALLS
select  createLogicalSchema();  	-- tables, indexes & restrictions
select  createTables();			-- only tables
select  dropTables();
select  addRestrictions();		-- only restrictions
select  dropRestrictions();
select  addIndexs();		-- add the default indexes
select  dropIndexs();		-- drop the default indexes


-- create tables with or without key restictions, and prepare specify load 
select  prepareDb(0, 1000, 1000, 10000);  -- basic load with default indexes without restrictions
select  prepareDb(1, 1000, 1000, 10000);  -- basic load with default indexes and restrictions


-- fill the tables with a minim load (clients, products, opers)
select  loadData(  1000,   10000,   10000);  
--medium load
select  loadData( 100000,  1000000,  10000000);  
--massive load
select  loadData(1000000, 10000000, 100000000); 

--we can add the restriccions later, with massive load can be a very slowly task 
select  addRestrictions();



-- TABLE CONTENTS
select * from  countrys;
select * from  clients;
select * from  products;
select * from  sales;
select * from  saledetails;

-- ERASE DATA
truncate   countrys;
truncate   clients;
truncate   products;
truncate   sales;
truncate   saledetails;