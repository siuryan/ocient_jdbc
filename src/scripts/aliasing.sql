/* THIS SCRIPT REQUIRES VERIFICATION OF VALUES */

select 1, 1, 1 from sys.dummy1 /* definite */;
select c1, c1, c1 from sys.dummy1 /* definite */;
select byte(c1), byte(c1), byte(c1) from sys.dummy1 /* definite */;
select 1 as first, 1 as second from sys.dummy1 /* definite */;
select 1 as first, 1 as first from sys.dummy1 /* ambigcol */;
select c1 as first, c1 as second from sys.dummy1 /* definite */;
select c1 as first, c1 as first from sys.dummy1 /* ambigcol */;
select byte(c1) as first, byte(c1) as second from sys.dummy1 /* definite */;
select byte(c1) as first, byte(c1) as first from sys.dummy1 /* ambigcol */;
select c1, 1 as c1 from sys.dummy1 /* definite */;
select 1 as c1, c1 from sys.dummy1 /* definite */;
