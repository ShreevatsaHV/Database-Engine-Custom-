Name : Shreevatsa Hosakere Vittal Rao

The application is submitted in a zipped folder.

The application supports following queries.

* The spaces and other punctuations have to be in the format provided in the below queries.
* The root is present in second 512 bytes of the tables.
* When a table is created, a leaf page and a root is created.
* Queries are case-insensitive.

Queries format

show tables;

create table table_name(col1_name int pri no,col2_name text yes,col3_name text yes,col4_name text yes);      // pri - to represent primary key(has to be in the first column position only and an integer data type). yes,no are used to represent nullable(yes) and not nullable(no). Has to be given.

insert into table (col1,col2,col3,col4) table_name values (value1,value2,value3,value4);                     // All columns have to be provided in the list and in order of created table columns. All column values also have to be given and in order.

insert into table () table_name values (value1,value2,value3,value4);                                        // Insert into table without giving column list. But the brackets are mandatory in place of column list. All column values also have to be given and in order.

select * from table_name;                                                                                    // selects all column values

select col1_name,col2_name from table_name;                                                                  // col1_name,col2_name - can be more than 2 columns

select * from table_name where col_name = value;                                                             // operator can be =, <, >

select col1_name,col2_name from table_name where col_name = value;                                           // col1_name,col2_name - can be more than 2 columns. operator can be =, <, >

update table_name set col_name = value;                                                                      // sets all row values for the given column to the value. 

update table_name set col_name = value where col_name = value;                                               // Operator can be =, <, >

delete from table table_name;                                                                                // deletes all values

delete from table table_name where col_name = value;	                                                     // operator can be =, <, >	

drop table table_name;                                                                                       // drops the table

quit;                                                                                                        // to quit the application

help;                                                                                                        // gives overview of query formats. But refer this readme file for confirmation.

version;                                                                                                     // gives version information of the application


