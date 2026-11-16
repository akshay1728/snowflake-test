create or replace table test.employee(id int,name varchar(200));
insert into {{ DB }}.test.employee(id,name) values (1,'akshay');
