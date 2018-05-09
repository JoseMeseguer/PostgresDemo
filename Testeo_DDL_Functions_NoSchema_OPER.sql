-- cambio del esquema por defecto para la simplificacion de nombres
-- como administradores debemos controlar en todo momento nuestro esquema de trabajo
-- por lo que he insistido en que lo escribamos para que nos quede grabado
set search_path to schemaTesteo;

-- create tables without key restrictions
CREATE OR REPLACE FUNCTION  createTables() RETURNS integer AS $$
BEGIN           
    create table   Countrys  (
        id         serial      primary key , 
        name       varchar(50)    
    );
    create table   Clients  (
        id          serial      primary key,
        name        varchar(50),
        dni         varchar(20), 
        country     int
    );
    CREATE TABLE    Categories (
        cat_id      integer primary key,
        cat_name    varchar(40),
        cat_parent_id integer
    );
    create table   Products  (
        id          serial     primary key,
        name        varchar(50),
        description varchar(200),
        prod_categ integer
    );
    create table   Sales  (
        id          serial     primary key, 
        beginDate   timestamptz,
        endDate     timestamptz,
        price       float,
        client      int    
    );
    create table   SaleDetails  (
        saleid      int,
        productid   int,
        name        varchar(50),
        units       int,
        unitprice   float    
    ); 
    CREATE TABLE    stock (
        prod        integer     primary key,
        units       integer,
        uncheck_units integer,
        checked_row  boolean
    );
-- TABLA UNLLOGGED (MAYOR VELOCIDAD ESCRITURA) PARA ALMACENAR TEMPORALMENTE CARRITOS
-- PODEMOS INCLUIR UNA TABLA DE DETALLES O BIEN MANEJAR ARRAYS YA QUE EN AMBOS CASOS
-- NECESITAREMOS HACER UN TRATAMIENTO FILA A FILA CUANDO RECUPEREMOS EL CARRO Y COMPROBEMOS
-- DISPONIBILIDAD DE PRODUCTOS Y EXISTENCIAS
    CREATE UNLOGGED TABLE  client_saved_carts (
        clientcode  int PRIMARY KEY,
        cartname    varchar (50),
        session_ts  timestamptz,
        products    integer[], 
        units       integer[]
    );-- NOTA:  SI HEMOS DE COMPROBAR LA SUMA DE VALORES DEL VECTOR UNITS:
    -- SELECT sum(u) AS total FROM (SELECT unnest(units) AS u FROM client_saved_carts) as a;
    return 1;
END;
$$ LANGUAGE plpgsql;



-- add the primary/foregin key restrictions
CREATE OR REPLACE FUNCTION  addRestrictions() RETURNS integer AS $$
BEGIN           
    alter table   Clients  add constraint clientcountry 
    foreign key (country) references   Countrys (id) 
    on update cascade;

    alter table   Sales  add constraint clientref 
    foreign key (client) references   Clients (id) 
    on update cascade;

    alter table   SaleDetails  add constraint productref 
    foreign key (productid) references   Products (id) 
    on update cascade;

    alter table   SaleDetails  add constraint saleref 
    foreign key (saleid) references   Sales (id) 
    on update cascade on delete cascade;
    return 1;
END;
$$ LANGUAGE plpgsql;



-- add the indexss
CREATE OR REPLACE FUNCTION  addIndexs() RETURNS integer AS $$
BEGIN           
   -- create index idx_saled_id       on   SaleDetails  (saleid);
    create index idx_saled_idprod   on   SaleDetails  (productid);
    create index idx_saled_saleprod on   SaleDetails  (saleid, productid);

    create index idx_sale_vipclient on   Sales  (client) 
                where client <=5000;
   -- create index idx_sale_client    on   Sales  (client) ;
    create index idx_sale_begindate on   Sales  (begindate DESC);
    create index idx_sale_price     on   Sales  (price) ;
    create index idx_sale_enddate    on   Sales  (enddate DESC NULLS FIRST);
   -- create index idx_sale_clientbdate on   Sales  (client ASC, begindate DESC);
    create index idx_sale_clientpricebdate on   Sales  
                (client ASC, begindate DESC, price DESC);
    return 1;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION  dropIndexs() RETURNS integer AS $$
BEGIN           
     --drop index  idx_saled_id ;
     drop index  idx_saled_idprod ;
     drop index  idx_saled_saleprod ;

     drop index  idx_sale_vipclient ;
--     drop index  idx_sale_client ;
     drop index  idx_sale_begindate ;
     drop index  idx_sale_price  ;
     drop index  idx_sale_enddate ;
--     drop index  idx_sale_clientbdate ;
     drop index  idx_sale_clientpricebdate ;
    return 1;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION  dropRestrictions() RETURNS integer AS $$
BEGIN           
    alter table   Clients      drop constraint clientcountry;
    alter table   Sales        drop constraint clientref;
    alter table   SaleDetails  drop constraint productref;
    alter table   SaleDetails  drop constraint saleref;
    return 1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION  createLogicalSchema() RETURNS integer AS $$
BEGIN  
    --execute 'select  createTables()' ;
    --execute 'select  addRestrictions()';
    --execute 'select  addIndexs()';
    perform  createTables() ;
    perform  addRestrictions();
    perform  addIndexs();
    return 0;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION  dropTables() RETURNS integer AS $$
BEGIN           
    drop table 	 client_saved_carts;
	drop table 	 stock;
	drop table   SaleDetails ;
    drop table   Sales ;
    drop table   Products ;
	drop table 	 Categories;
    drop table   Clients ;
    drop table   Countrys ;
    RETURN 1;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION  loadData(clients int, products int, opers int) RETURNS integer AS $$
DECLARE result int;
DECLARE delay int := 30;  -- tiempo maximo entre 2 ventas
BEGIN           
    result :=  fillCountrys();
    result :=  fillClients(clients);
    result :=  fillCategories();
    result :=  fillProducts(products);
    result :=  fillSales(opers, delay, clients, products);
    RETURN result;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION  prepareDB(type int, clients int, products int, opers int) RETURNS integer AS $$
DECLARE result int;
BEGIN           
    if type > 0
        then result :=  createLogicalSchema();
        else result :=  createTables();
    end if;    
    result :=  loadData (clients, products, opers);
    RETURN result;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION  fillCountrys() RETURNS integer AS $$
BEGIN           
    insert into   Countrys  (name) values ('Spain');
    insert into   Countrys  (name) values ('France');
    insert into   Countrys  (name) values ('Italy');
    insert into   Countrys  (name) values ('Germany');
    insert into   Countrys  (name) values ('UK');
    RETURN 1;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION  fillClients(numclients int) RETURNS integer AS $$
declare country_list int[];
declare country_size int;
declare country int;
declare cclients int := 1;
declare cname varchar := 'client_';
BEGIN
    select  array_agg(id)  from   Countrys  into country_list;
    country_size := array_length(country_list, 1) -1;

    WHILE cclients <= numClients LOOP
        country := country_list[1 + random()*country_size];
        cname = 'client_' || cclients::text;
        insert into   Clients  (name, dni, country) values (cname, null, country);
        cclients := cclients + 1;
    END LOOP;
    RETURN cclients;    
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION  fillCategories () RETURNS integer AS $$
BEGIN     
    INSERT INTO categories values   (1, 'moda', 0), (2, 'informatica', 0), (3, 'hogar', 0),
                                (4, 'zapatos', 1), (5, 'bolsos', 1),
                                (6, 'pc', 2), (7, 'componentes', 2),
                                (8, 'cuadros', 3),  (9, 'cajas', 3),
                                (10, 'almacenamiento', 7), (11, 'memoria', 7),
                                (12, 'ssd', 10);
    
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

--NUEVA FUNCION PARA EL LLENADO ALEATORIO DE LOS PRODUCTOS INCLUYENDO INFORMACION SOBRE SU CATEGORIA
CREATE OR REPLACE FUNCTION  fillProducts(numproducts int) RETURNS integer AS $$
declare cproducts int := 1;
declare cat int;
declare maxcat int;
declare cname varchar:= 'product_';
BEGIN     
    select count(*) from categories into maxcat;
    WHILE cproducts <= numproducts LOOP    
        cname := 'product_' || cproducts::text;  
        cat := 1 + random()*maxcat;
        insert into   Products  values (default, cname, null, cat);
        cproducts := cproducts + 1;
    END LOOP;
    
    RETURN cproducts;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION  fillSales(numSales int, delay int, numClients int, numProducts int ) RETURNS integer AS $$
declare csales int := 1;
declare clientaleat int;
declare bdate timestamp := '2015-01-01 00:00:00';  
declare timeinterval1 varchar;
declare timeinterval2 varchar;
declare edate timestamp;
declare result float;
BEGIN    
    WHILE csales <= numSales LOOP        
        timeinterval1 := random()*delay || ' seconds';
        clientaleat := 1 + random() * (numClients-1);  
              
        bdate := bdate + timeinterval1::interval; 
        timeinterval2 := (1 + (random()*10))::text || ' days';          
        edate := bdate + timeinterval2::interval;

        insert into   Sales  (begindate, enddate, price, client) 
        values ( bdate, edate, 0.0, clientaleat);
        
        result :=  fillSaleDetails (csales, 10, numProducts);
        update   Sales  SET price = result where id = csales;
        csales := csales + 1;
    END LOOP;    
    RETURN csales;
END;
$$ LANGUAGE plpgsql;


-- function for include into FillSale 
--DROP FUNCTION  fillSaleDetails (numSale int, numDetails int, numProducts int) ;
CREATE OR REPLACE FUNCTION  fillSaleDetails (numSale int, numDetails int, numProducts int) RETURNS float AS $$
declare productAleat int;
declare unitsAleat int;
declare unitPrice float;
declare linePrice float;
declare subtotal float;
declare productname varchar;
declare detailsAleat int;
declare cdetails int;
BEGIN              
        detailsAleat := 1 + random()*numDetails;
        cdetails := 0;
        linePrice := 0;
        WHILE cdetails < detailsAleat LOOP        
            productAleat := 1 + random() * (numProducts-1); 
            productname := 'product_' || productAleat::text;  
            unitPrice := 4.99 + (random()*100);
            unitsAleat := 1 + random()*10;
            subtotal := unitPrice * unitsAleat;
            linePrice = linePrice + subtotal;

            insert into   SaleDetails  (saleid, productid, name, units, unitprice)
                        values (numSale, productAleat, productname, unitsAleat, unitPrice);
            cdetails := cdetails + 1;
        END LOOP;    
        select TRUNC( CAST(lineprice as NUMERIC),2) into lineprice;
    RETURN linePrice;
END;
$$ LANGUAGE plpgsql;