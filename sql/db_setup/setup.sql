/*
-- This is done manually
create database files_db
with
    OWNER = postgres
    encoding= 'UTF8'
    connection limit = -1;
*/

create role
	files_db_pool
with
	encrypted password 'KhY/z@zaTN~k{&5;!g3+dzj5VmWKJ[.%'
	login;