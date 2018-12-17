drop table if exists block;
create table block(
	block_id varchar(64) primary key, 
	timestamp datetime, 
	height int,
	-- difficulty double,
	nbits bigint,
	insertTime timestamp
); 

drop table if exists txo;
create table txo(
	output_txid varchar(64), 
	output_idx int, 
	value DECIMAL(38), 
	script text, 
	input_txid varchar(64), 
	input_idx int, 
	insertTime timestamp,
	script_type varchar(20),
	addresses text,
	primary key (output_txid, output_idx), 
	index(input_txid)
);

drop table if exists transaction;
create table transaction(
	txid varchar(64) primary key, 
	block_id varchar(64), 
	timestamp datetime, 
	is_coinbase boolean,
	insertTime timestamp,
	index(block_id)
);

drop table if exists address;
create table address(
	address varchar(34) primary key,
	entity_id int unsigned,
	output_time int,
	input_time int,
	first_tx varchar(64),
	updateTime timestamp,
	index(first_tx),
	index(entity_id)
);


DROP TABLE IF EXISTS Entity_UFT;
create table Entity_UFT (
	parent int unsigned,
	child int unsigned auto_increment,
	primary key(child),
	index(parent)
); 

DELIMITER //
DROP FUNCTION IF EXISTS fn_tree_root;
CREATE FUNCTION fn_tree_root(entity_id INT) 
RETURNS INT
BEGIN
	DECLARE nid INT DEFAULT entity_id;
	WHILE entity_id IS NOT NULL DO
		SET nid := entity_id;
		SET entity_id := (SELECT parent from Entity_UFT where child=nid);
	END WHILE;
	RETURN nid;
END //
DELIMITER ;

DELIMITER //
DROP FUNCTION IF EXISTS fn_new_entity;
CREATE FUNCTION fn_new_entity() 
RETURNS INT
BEGIN
	INSERT INTO Entity_UFT() values();
	RETURN LAST_INSERT_ID();
END //
DELIMITER ;

DELIMITER //
CREATE TRIGGER before_insert_new_address
BEFORE INSERT ON address
FOR EACH ROW
  IF new.entity_id IS NULL
  THEN
    SET new.entity_id = fn_new_entity();
  END IF//
DELIMITER ;
  
drop table if exists log;
create table log(sp varchar(80), msg text, time timestamp);

delimiter //
drop procedure if exists log;
create procedure log(IN sp varchar(80), IN msg text)
BEGIN
	INSERT INTO `log`(sp, msg) values(sp, msg);
END //
delimiter ;

delimiter //
drop procedure if exists sp_merge_entity;
CREATE procedure sp_merge_entity(IN addresses TEXT) 
BEGIN 
	-- CALL log('sp_merge_entity', CONCAT('args+', addresses));
	SET @check_sql = CONCAT('SELECT min(r),max(r) INTO @min_r, @max_r '
	' FROM (SELECT fn_tree_root(entity_id) r FROM address WHERE address IN ( ',
	addresses,
	' )) _b ');
	prepare stmt FROM @check_sql;
	execute stmt;
	CALL log('sp_merge_entity', CONCAT('sql1+',@check_sql));
	IF @min_r <> @max_r THEN
		SET @update_sql = CONCAT('UPDATE Entity_UFT SET parent=@min_r '
		' WHERE child IN (SELECT fn_tree_root(entity_id) r FROM address where address in(',
		addresses,
		' ))  and child<>@min_r');
	CALL log('sp_merge_entity', CONCAT('sql2+',@update_sql));
		deallocate prepare stmt;
		prepare stmt FROM @update_sql;
		execute stmt;
	END IF;
	deallocate PREPARE stmt;
END//
DELIMITER ; 


delimiter //
drop procedure if exists sp_flat;
CREATE procedure sp_flat() 
BEGIN 
	UPDATE address SET entity_id=fn_tree_root(entity_id);
	DELETE FROM Entity_UFT WHERE parent is not null or child not in(select distinct entity_id from address);
END//
DELIMITER ; 