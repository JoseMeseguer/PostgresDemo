-- TABLA UNLOGGED DE RAPIDA ACTUALIZACION PARA GUARDAR PREFERECIAS DEL CLIENTE
CREATE UNLOGGED TABLE  client_preferences (
	clientcode int PRIMARY KEY,
	language int,
	categories int[],
	wish_list int[],
	shipping_address varchar(200)  
);

-- TABLA UNLLOGGED (MAYOR VELOCIDAD ESCRITURA) PARA ALMACENAR TEMPORALMENTE CARRITOS
-- PODEMOS INCLUIR UNA TABLA DE DETALLES O BIEN MANEJAR ARRAYS YA QUE EN AMBOS CASOS
-- NECESITAREMOS HACER UN TRATAMIENTO FILA A FILA CUANDO RECUPEREMOS EL CARRO Y COMPROBEMOS
-- DISPONIBILIDAD DE PRODUCTOS Y EXISTENCIAS
CREATE UNLOGGED TABLE  client_saved_carts (
	clientcode 	int PRIMARY KEY,
    cartname 	varchar (50),
	session_ts  timestamptz,
	products 	integer[], 
	units       integer[]
);
-- NOTA:  SI HEMOS DE COMPROBAR LA SUMA DE VALORES DEL VECTOR UNITS:
-- SELECT sum(u) AS total FROM (SELECT unnest(units) AS u FROM client_saved_carts) as a;


-- PROCESO A DESARROLLAR

-- UTILIZAREMOS COMO CARRITO UNA TABLA TEMPORAL, QUE TENDRA UN TRIGGER PARA RESERVAR UNIDADES TEMPORALMENTE EN LA TABLA DE STOCK
-- COMPROBANDO SIEMPRE QUE TENGAMOS EXISTENCIAS NO RESERVADAS. CON UNA FUNCION CONFIRMAREMOS LA OPERACION DE VENTA CREANDO UNA 
-- NUEVA VENTA, DONDE UN TRIGGER EN LA TABLA SALE DETAILS SE ENCARGARA DE ACTUALIZAR LA TABLA DE STOCK, QUE A SU VEZ TENDRA UN
-- TRIGGER QUE ACTUALIZARA LA VISTA MATERIALIZADA CUANDO ALGUN PRODUCTO MARCADO COMO CHECKED QUE SIN EXISTENCIAS

-- TABLA TEMPORAL PARA GUARDAR EL CARRITO DE COMPRA ACTUAL DONDE XXX ES EL CODIGO DEL USUARIO.
-- COMO SOLO PERTENECE AL USUARIO XXX Y EN TODOS LOS PROCEDIMIENTOS PEDIREMOS SU CODIGO PARA 
-- REDIRIGIRLO A SU TABLA NO ES NECESARIO INCLUIR ESE DATO EN LA TABLA
-- NO TRABAJAREMOS CON ARRAY PORQUE SU MANEJO REQUIERE MAS RECURSOS QUE UN USO "TRADICIONAL"
-- SOBRE UNA PEQUEÑA TABLA, Y GUARDAMOS EL INSTANTE EN QUE SE HA AÑADIDO CADA LINEA
CREATE TEMP TABLE current_cart_XXX (
	ts  		timestamptz,
	products 	integer
	units 		integer
);

-- UNA TABLA PARA GUARDAR LAS DIFERENTES PROMOCIONES REALIZADAS
--drop table promos;
CREATE TABLE promos (
	prom_id integer primary key,
	prom_name varchar(40),
	prom_begin timestamptz,
	prom_finish timestamptz,
	prom_list integer[],
	conditions varchar(300)
);
-- PREPARAMOS ALGUNAS PROMOS AL AZAR
INSERT INTO promos values (1, 'first', '2018-03-01', '2018-03-08', ARRAY[1,2,3,4,5,6,7,8,9,10], null),
				(2, 'second', '2018-03-01', '2018-03-08', ARRAY[11,12,13,14,15,16,17,18,19,20], null),
				(3, 'third', '2018-03-08', '2018-03-15', ARRAY[21,22,23,24,25,26,27,28,29,30], null),
				(4, 'fourth', '2018-03-15', '2018-03-22', ARRAY[31,32,33,34,35,36,37,38,39,40], null),
				(5, 'actual', '2018-05-01', '2018-05-15', ARRAY[41,42,43,44,45,46,47,48,49,50], null);

-- PREPARAMOS UNA TABLA DE STOCK SOLO PARA LOS PRIMEROS X PRODUCTOS QUE SON LOS QUE TESTEAMOS
-- PARA IR RAPIDO PONEMOS UN PAR DE UNIDADES DE CADA PRODUCTO
-- Y FIJAMOS QUE TODAS LAS FILAS SEAN COMPROBADAS Y QUE NINGUN CARRITO TIENE RESERVADA NINGUNA UNIDAD
CREATE OR REPLACE FUNCTION  fillstock(cproducts int) RETURNS integer AS $$
BEGIN     
    WHILE cproducts <= 1000  LOOP   
        insert into stock  values (cproducts, 2, 0, true);
        cproducts := cproducts + 1;
    END LOOP;
    RETURN cproducts;
END;
$$ LANGUAGE plpgsql;


-- CREAMOS UNA VISTA MATERIALIZADA PARA MOSTRAR UNICAMENTE LAS PROMOCIONES
-- ACTIVAS EN EL DIA ACTUAL Y COMPROBANDO QUE AUN QUEDE UNIDADES EN STOCK
-- se introduce la funcion OVERLAPS para conocer si un rango de fechas esta incluido dentro de otro rango de fechas
CREATE MATERIALIZED VIEW promos_today as
select prd.id as ref, prd.name as name, prd.description as especifications, 
		prd.prod_categ as category, s.units as units, prm.prom_finish as finish
from products as prd inner join promos as prm  on prd.id = ANY (prm.prom_list)
inner join stock as s on prd.id = s.prod
where (current_timestamp, current_timestamp) OVERLAPS (prm.prom_begin, prm.prom_finish) and s.units > 0;
--WITH NO DATA;    --POSIBILIDAD PARA ALMACENAR EL CODIGO DE LA VISTA PERO NO CARGAR SUS DATOS HASTA EL PRIMER REFRESCO

--drop materialized view promos_today
--refresh materialized view promos_today

-- creacion de la funcion que sera llamada por cualquier trigger, no depende de uno en concreto directamente
CREATE OR REPLACE FUNCTION refreshview()  RETURNS TRIGGER AS $$
BEGIN
    refresh materialized view promos_today;
    RETURN NULL;
END;
$$  LANGUAGE plpgsql;

-- creacion del trigger sobre stock, pero cuidado que solo salte ante actualizaciones del campo units
-- y siempre que esa fila haya sido "marcada" para su vigilancia. Comprobara cada fila que se actualice por separado
-- y llamara la funcion necesaria. El trigger solo salta cuando realmente estamos sin stock real de un producto
CREATE TRIGGER trg_check_stock AFTER UPDATE OF units ON stock
FOR EACH ROW  WHEN (OLD.checked_row = true AND NEW.units=0 AND NEW.uncheck_units=0)
EXECUTE PROCEDURE refreshview();
--DROP TRIGGER trg_check_stock ON stock;  --borramos el trigger



-- CREAREMOS PROCEDIMIENTOS SOBRE LOS QUE EL USUARIO USERPRIV TENGA PERMISOS DE EJECUCION 
-- procedimiento que crea la tabla temporal para guardar el carrito de compra
-- debido a que el manejo de arrays implica mayor gasto de recursos nos sale mas a cuenta
-- implementar una tabla con formato clasico
-- hemos de diseñar un triiger especifico a la tabla como el siguiente, especificando el codigo de cliente
--CREATE TRIGGER trg_check_stock AFTER UPDATE ON current_cart_XXXX  FOR EACH ROW EXECUTE PROCEDURE cart_reserve_units();
-- Y LO GENERAREMOS DINAMICAMENTE JUNTO A LA TABLA TEMPORAL ESPECIFICA DONDE GUARDAREMOS LA COMPRA DEL CLIENTE
--DROP FUNCTION prepareCart( int)
CREATE OR REPLACE FUNCTION prepareCart(codeuser int)  RETURNS void  AS $$
declare myquery varchar := 'create temp table current_cart_';
BEGIN
    myquery := myquery || codeuser || ' ( 	ts timestamptz, 
    										product integer, 
                                            productname varchar,
    										units integer,
                                            unitprice float );';
    execute myquery;  
    myquery := 'CREATE TRIGGER trg_check_stock AFTER UPDATE OR INSERT OR DELETE ON current_cart_' || codeuser || 
                ' FOR EACH ROW EXECUTE PROCEDURE cart_reserve_units();';
    execute myquery;  
END; 
$$ LANGUAGE plpgsql;

-- PROTOTIPO DE LA FUNCION SIGUIENDO UN PATRON SIMILAR A LA FUNCION DESARROLLADA
-- PARA EL TRIGGER DE LA TABLA DE DETALLES
CREATE OR REPLACE FUNCTION cart_reserve_units()  RETURNS TRIGGER AS $$
BEGIN
	RETURN NULL;
END; 
$$ LANGUAGE plpgsql;

-- PRUEBAS
drop table current_cart_2000
select prepareCart(2000); -- ejecutamos el procedimiento para crear una tabla temporal current_cart_2000
select * from current_cart_2000;  -- comprobamos directamente antes de realizar una vista especifica
Select addtocart (2000, 5, 3);

-- NECESITAMOS PROCEDIENTOS PARAMETRIZABLES PARA EL CRUD SOBRE LA TABLA TEMPORAL ESPECIFICA DEL USUARIO
-- (algun procedimiento contendra acciones logicas de verificacion que en sistemas con alta demanda habria que 
-- delegar en la aplicacion y estos procedimientos centrarse solo en su funcion basica)
-- EN CAMBIO, LAS OPERACIONES DE ACTUALIZACION SOBRE EL STOCK SI TIENEN SENTIDO QUE SE REALIZEN AUTOMATICAMENTE
-- (todo proceso de actualizacion retorna las unidades que componen el carrito, pero podrian no retornar nada)

-- procedimiento para retornar el carrito como una vista
CREATE OR REPLACE FUNCTION showCart(codeuser int)  
RETURNS TABLE (daterow timestamptz, prod integer, prodname varchar, units int, price float)  AS $$
BEGIN
    RETURN QUERY execute 'select ts, product, productname, units, unitprice from current_cart_' || codeuser;
END; 
$$  LANGUAGE plpgsql;

--procedimiento para añadir lineas de compra al carrito (comprobando si ya existe el producto para en ese caso solo añadir cantidad)
CREATE OR REPLACE FUNCTION addtoCart(codeuser int, codeprod int, prodname varchar, units int, price float)  RETURNS int  AS $$
declare sumunits int;
BEGIN
	execute format ('insert into current_cart_%s values (current_timestamp, $1, $2, $3, $4);', codeuser) 
	USING codeprod, prodname, units, price;
	execute format ('SELECT sum(units) FROM current_cart_%s', codeuser) INTO sumunits;  -- deberiamos ahorrar este calculo
    --y que el numero de unidades maximas, como regla de negocio fuera comprobado por la APP
    RETURN sumunits;
END; 
$$  LANGUAGE plpgsql; 

--prueba cargando el carrito con varias compras
select addtoCart (2000, 100, 'p100', 3, 10);
select addtoCart (2000, 101, 'p101', 2, 5);
select addtoCart (2000, 102, 'p102', 5, 2);
select addtoCart (2000, 103, 'p103', 1, 100);
select addtoCart (2000, 104, 'p104', 2, 25);select showCart(2000)
SELECT sum(units) FROM current_cart_2000 

-- Procedimiento para modificar SOLAMENTE cantidades de un producto del carrito
CREATE OR REPLACE FUNCTION modifyUnitsCart(codeuser int, codeprod int, units int)  RETURNS int  AS $$
BEGIN
    execute format ('update current_cart_%s set units = $1 where product = $2', codeuser) USING units, codeprod;
    RETURN 1;
END; 
$$  LANGUAGE plpgsql;

select modifyUnitsCart (2000, 5, 8); --comprobacion

-- Procedimiento para eliminar productos del carrito
CREATE OR REPLACE FUNCTION takeoutCart(codeuser int, codeprod int)  RETURNS int  AS $$
BEGIN
    execute format ('delete from current_cart_%s where product = $1', codeuser) USING codeprod;
    RETURN 1;
END; 
$$  LANGUAGE plpgsql;

select takeoutCart(2000, 5); -- comprobacion

-- Procedimiento que guarda el carrito para una posterior revision, se cancelan las unidades reservadas
-- si por reglas de negocio interesa guardar las cantidades de cada producto aunque sea sin reserva, se implementa
CREATE OR REPLACE FUNCTION saveCart(codeuser int, name varchar)  RETURNS int  AS $$
declare v_prods int[];
declare v_units int[];
BEGIN
    execute format ('select array_agg(product), array_agg(units) from current_cart_%s', codeuser) 
    INTO v_prods, v_units;

    execute format ('insert into client_saved_carts values ($1, null, current_timestamp, $2, $3)
                    ON CONFLICT (clientcode) DO UPDATE SET session_ts=current_timestamp, products=$2, units=$3;') 
                    USING codeuser, v_prods, v_units; 
    RETURN 0;
END; 
$$ LANGUAGE plpgsql;

-- comprobacion
select saveCart(2000, null);
select * from client_saved_carts;
--truncate client_saved_carts;

-- procedimiento para cargar uno de los carritos guardados como el actual
CREATE OR REPLACE FUNCTION loadCart(codeuser int, name varchar)  RETURNS int  AS $$
declare numprods int;
declare i int := 1;
declare v_prods int[];
declare v_units int[];
declare myquery varchar := '';
declare lines varchar[];
BEGIN
    select array_length(products,1), products, units 
    from client_saved_carts where clientcode = codeuser 
    INTO numprods, v_prods, v_units;

    IF numprods >= 1    -- SI EL CLIENTE NO TIENE CARRITO GUARDADO NO PROVOCAMOS UN ERROR
    THEN
        WHILE i <= numprods  LOOP
            lines[i] :=  '(current_timestamp, '  || v_prods[i] || ', ' || v_units[i] || ')';
            i := i +1;
        END LOOP;
        myquery := format ('insert into current_cart_%s values ', codeuser ) || array_to_string (lines, ',') || ';' ;
        execute myquery; 
    END IF;
    RETURN 0;
END; 
$$  LANGUAGE plpgsql;
-- VALIDACIONES
--truncate current_cart_2000
select * from current_cart_2000
select loadCart(2000, 'jose') -- comprobacion

-- Procedimiento para validar la compra a partir del carrito actual
CREATE OR REPLACE FUNCTION acceptCart(codeuser int)  RETURNS int  AS $$
declare totalprice float8 := 0;
declare i int := 1;
declare codesale int;
declare myquery varchar := '';
declare details varchar;
declare lines varchar[];
declare cur_cart REFCURSOR; --cuando se abre el cursor dinamicamente es de tipo refcursor
-- el uso de variables de tipo REFCURSOR es incompatible con bucles FOR por lo que pasamos a LOOP
declare registro RECORD;
BEGIN
    OPEN cur_cart FOR EXECUTE format ('select * from current_cart_%s', codeuser);
	-- cursor que recorra la tabla temporal y realice inserciones en sale y saledetails
     LOOP
            FETCH cur_cart INTO registro;
			EXIT WHEN NOT FOUND;
			raise notice 'product%  units%', registro.product, registro.units;
			totalprice := totalprice + (registro.units * registro.unitprice);
            lines[i] :='($1,'||registro.product||','''||(registro.productname)||''','||registro.units||','||registro.unitprice||')';
            i := i +1;
    END LOOP;   --salimos del cursor con los valores a insertar cargados en la consulta
	
    -- primero insertamos la venta recogiendo el codigo de venta
    insert into sales values (default, current_timestamp, null, totalprice, codeuser) RETURNING id INTO codesale;
    
    -- preramos la consulta de insercion de la lista de prodcutos vendido y la insertamos
    select array_to_string (lines, ',') into details;
	RAISE notice 'details =  %', details;
    myquery := 'insert into saledetails values ' || details;  
	RAISE notice 'query =  %', myquery;
	EXECUTE format(myquery) USING codesale ;
    RETURN i-1;
END; 
$$  LANGUAGE plpgsql;

-- PROTOTIPO: procedimiento para descartar el carrito actual
CREATE OR REPLACE FUNCTION discardCart(codeuser int)  RETURNS int  AS $$
BEGIN
    -- cursor que recorra la tabla temporal y realice inserciones en sale y saledetails


    RETURN 0;
END; 
$$  LANGUAGE plpgsql;

-- una posible funcion a llamar cada X minutos para que eliminara todos los carritos guardados
-- que superen el margen de tiempo establecido y para los que solo han superado el tiempo de reserva
-- establecer su carrito como "expired" (añadiremos un campo booleano) y eliminamos las reservas de producto
-- PROTOTIPO: 
CREATE OR REPLACE FUNCTION manageSavedCarts ( minuts_reserv int, minuts_max int)  RETURNS int  AS $$
BEGIN

    RETURN 0;
END; 
$$  LANGUAGE plpgsql;


-- PARA TERMINAR
-- TRIGGER MAS FUNCION ASOCIADA PARA LA TABLA DE DETALLES DE VENTAS
-- CONTROLA LAS OPERACIONES DE ESCRITURA PARA SINCRONIZAR EL ESTADO DE LA TABLA DE STOCK

-- ORDEN DE COMPROBACION: PRIMERO DEBE ESTAR INSERTADO, SE QUITAN LAS UNIDADES RESERVADAS SIEMPRE QUE NO SEAN INFERIORES A LAS UNIDADES A RESTAR
-- DESPUES SE PODRA BORRAR DONDE SE DEVUELVEN LAS UNIDADES DIRECTAMENTE A UNITS YA QUE DESAPARECIERON DE LAS RESERVADAS AL INSERTAR
-- O BIEN MODIFICAR DONDE LAS CANTIDADES DEBERIAN SALIR DIRECTAMENTE DE UNITS YA QUE NO HAY NADA RESERVADO 
-- ESTAS 2 ULTIMAS ACCIONES NO SON LAS USUALES DE NEGOCIO, IMPLICA QUE ALGUIEN CON PERMISOS LAS REALICE MANUALMENTE
-- PERO AUN ASI DEBERIAN VERIFICARSE
CREATE OR REPLACE FUNCTION fn_update_stock() RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') 
            THEN 
                --RAISE NOTICE 'quitando unidades reservadas';
                update stock set uncheck_units= uncheck_units - NEW.units 
                where prod= NEW.productid and (uncheck_units - NEW.units) >=0;
                IF NOT FOUND
                THEN    RAISE EXCEPTION 'product % not found, or no units enough', NEW.productid;
                END IF;
        ELSIF (TG_OP = 'DELETE')   -- se quitan unidades directamente de units si es posible
            THEN 
            --RAISE NOTICE 'retornando unidades eliminadas';
            update stock set units = units + OLD.units  where prod= OLD.productid;
            IF NOT FOUND 
                THEN RAISE EXCEPTION 'product % not found', NEW.productid;
            END IF;
        ELSIF (TG_OP = 'UPDATE') --la actualizacion puede añadir o quitar unidades
            THEN 
                    IF ( NEW.units > OLD.units )  -- si han modificado para pedir mas producto se descuentan de units si es posible
                        THEN 
                            --RAISE NOTICE 'tomando nuevas unidades';
                            update stock set units= units - (NEW.units - OLD.units)
                            where prod= OLD.productid and units - (NEW.units - OLD.units) >=0;
                            IF NOT FOUND 
                            THEN RAISE EXCEPTION 'product % not found, or no units enough', OLD.productid;
                            END IF; 
                        ELSIF ( NEW.units < OLD.units ) -- si se toman menos unidades se devuelven directamente a units teniendo en cuenta el signo de diff
                                THEN 
                                --RAISE NOTICE 'retornando unidades devueltas';
                                update stock set units= units + (OLD.units - NEW.units) where prod=OLD.productid;
                                IF NOT FOUND 
                                THEN RAISE EXCEPTION 'product % not found', OLD.productid;
                                END IF; 
                    END IF; 
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

--DROP TRIGGER trg_idupdate_stock ON saledetails
CREATE TRIGGER trg_idupdate_stock AFTER INSERT OR DELETE  ON saledetails
FOR EACH ROW EXECUTE PROCEDURE fn_update_stock();

-- AFINAMOS AL MAXIMO EL CONTROL DEL DISPARO DEL TRIGGER 
--DROP TRIGGER trg_update_stock ON saledetails
CREATE TRIGGER trg_update_stock AFTER UPDATE OF units ON saledetails
FOR EACH ROW WHEN (NEW.units IS DISTINCT FROM OLD.units)  
EXECUTE PROCEDURE fn_update_stock();

-- COMPROBACIONES
insert into stock values (15, 3, 3);
select * from stock where prod = 15;
insert into saledetails values(10000000, 15, null, 2, 0)
insert into saledetails values(10000001, 15, null, 2, 0)
update saledetails set units = 1 where saleid =10000000 and productid = 15
update saledetails set units = 3 where saleid =10000000 and productid = 15
select * from saledetails where saleid =10000000 and productid = 15
delete from saledetails where saleid =10000000 and productid = 15