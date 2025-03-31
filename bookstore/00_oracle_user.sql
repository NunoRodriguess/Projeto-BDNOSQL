alter session set "_ORACLE_SCRIPT"=true;

CREATE TABLESPACE bookstore_tables DATAFILE 'BOOK_FILES.dbf' SIZE 500m;

CREATE USER bookstore IDENTIFIED BY bookstore;

GRANT CONNECT TO bookstore;
GRANT ALL PRIVILEGES TO bookstore;