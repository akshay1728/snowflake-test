create or replace table {{ DB }}.test.employee(id int,name varchar(200));
insert into {{ DB }}.test.employee(id,name) values (2,'akshay');
