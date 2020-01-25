select * from tpch.lineitem /* definite */ ;
select l_linenumber from tpch.lineitem /* definite */ ;
select l_linenumber from tpch.lineitem where l_linenumber = 4 /* definite */ ;
select l_orderkey from tpch.lineitem where l_linenumber = 4 /* definite */ ;
select l_orderkey from tpch.lineitem where l_linenumber <= 4 /* definite */ ;
select l_orderkey from tpch.lineitem where l_linenumber >= 4 /* definite */ ;
select l_orderkey from tpch.lineitem where not l_linenumber = 4 /* definite */ ;
select l_orderkey from tpch.lineitem where l_linenumber <> 4 /* definite */ ;
select l_orderkey, l_linenumber from tpch.lineitem /* definite */ ;
select l_orderkey from tpch.lineitem where l_linenumber > 2 and l_linenumber < 5 /* definite */ ;
select l_orderkey from tpch.lineitem where l_linenumber = 3 or l_linenumber = 6 /* definite */ ;
select l_linenumber + 1 from tpch.lineitem /* definite */ ;
select l_orderkey + l_linenumber from tpch.lineitem /* definite */ ;
select l_linenumber + 1 as linenum from tpch.lineitem /* definite */ ;
select power(2, l_linenumber) from tpch.lineitem /* definite */ ;
select count(*) from tpch.lineitem /* definite */ ;
select count(l_orderkey) from tpch.lineitem /* definite */ ;
select l_linenumber, count(*) from tpch.lineitem group by l_linenumber /* definite */ ;
select max(l_linenumber) from tpch.lineitem /* definite */ ;
select l_linenumber, max(l_orderkey) from tpch.lineitem group by l_linenumber /* definite */ ;
select min(l_linenumber) from tpch.lineitem /* definite */ ;
select l_linenumber, min(l_orderkey) from tpch.lineitem group by l_linenumber /* definite */ ;
select l_partkey, l_suppkey, count(*) as cnt from tpch.lineitem group by l_partkey, l_suppkey /* definite */ ;
select max(l_linenumber), sum(l_linenumber) from tpch.lineitem /* definite */ ;
select l_partkey, l_suppkey, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_partkey, l_suppkey /* definite */ ;
select l_returnflag, l_linestatus, count(*) as cnt from tpch.lineitem group by l_returnflag, l_linestatus /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by s /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by s asc /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by l_returnflag desc, l_linestatus /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus desc /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by l_returnflag asc, l_linestatus desc /* definite */ ;
select * from tpch.lineitem order by l_orderkey, l_linenumber /* definite */ ;
select * from tpch.lineitem order by l_orderkey, l_linenumber limit 10 /* definite */ ;
select * from tpch.lineitem limit 10 /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by l_returnflag asc, l_linestatus desc limit 10 /* definite */ ;
select * from tpch.lineitem order by l_orderkey, l_linenumber offset 10 /* definite */ ;
select * from tpch.lineitem offset 10 /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by l_returnflag asc, l_linestatus desc offset 10 /* definite */ ;
select * from tpch.lineitem order by l_orderkey, l_linenumber limit 10 offset 10 /* definite */ ;
select * from tpch.lineitem limit 10 offset 10 /* definite */ ;
select l_returnflag, l_linestatus, max(l_linenumber) as m, sum(l_linenumber) as s from tpch.lineitem group by l_returnflag, l_linestatus order by l_returnflag asc, l_linestatus desc limit 10 offset 10 /* definite */ ;
select sum(l_linenumber)+1 from tpch.lineitem /* definite */ ;
select (min(l_linenumber) + max(l_linenumber)) / 2 from tpch.lineitem /* definite */ ;
select l_linenumber, count(l_orderkey)+1 from tpch.lineitem group by l_linenumber having count(l_orderkey) > 10000 /* definite */ ;
select l_linenumber, count(l_orderkey)+1 from tpch.lineitem group by l_linenumber having count(l_orderkey) > 10000 order by l_linenumber limit 5 /* definite */ ;
select l_linenumber from tpch.lineitem group by l_linenumber having count(l_orderkey) > 10000 /* definite */ ;
select l_linenumber from tpch.lineitem group by l_linenumber having count(l_orderkey)+1 > 10000 /* definite */ ;
select * from tpch.lineitem where l_linestatus = 'R' /* definite */ ;
select l_returnflag from tpch.lineitem where l_linestatus = 'R' /* definite */ ;
select avg(l_linenumber) from tpch.lineitem /* definite */ ;
select stdev(l_linenumber) from tpch.lineitem /* definite */ ;
select variance(l_linenumber) from tpch.lineitem /* definite */ ;
select * from tpch.lineitem where l_suppkey is null /* definite */ ;
select l_orderkey from tpch.lineitem where l_linenumber between 2 and 5 /* definite */ ;
select * from tpch.lineitem where l_suppkey is not null /* definite */ ;
select * from tpch.lineitem where not l_suppkey is null /* definite */ ;
select * from tpch.lineitem where l_shipdate > date('1994-01-01') /* definite */ ;
select l_orderkey from tpch.lineitem where l_linenumber in (3, 6) /* definite */ ;
select 1 from tpch.region /* definite */ ;
select 'a' from tpch.region /* definite */ ;
select * from tpch.lineitem where l_linenumber in (3,6) /* definite */ ;
select variance(l_linenumber), stdev(l_linenumber) from tpch.lineitem /* definite */ ;
select * from tpch.lineitem where l_partkey = l_suppkey /* definite */ ;
select * from tpch.lineitem where l_commitdate = l_receiptdate /* definite */ ;
select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from tpch.lineitem where l_shipdate <= date('1998-12-01') - days(90) group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus /* definite */ ;
select o_orderpriority, count(*) as order_count from tpch.orders where o_orderdate >= date('1993-07-01') and o_orderdate < date('1993-07-01') + months(3) group by o_orderpriority order by o_orderpriority /* definite */ ;
select sum(l_extendedprice*l_discount) as revenue from tpch.lineitem where l_shipdate >= date('1994-01-01') and l_shipdate < date('1994-01-01') + years(1) and l_discount between 0.06-0.01 and 0.06+0.01 and l_quantity < 24 /* definite */ ;
select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from tpch.lineitem where l_shipdate >= date('1996-01-01') and l_shipdate < date('1996-01-01') + months(3) group by l_suppkey /* definite */ ;
select l_orderkey from tpch.lineitem group by l_orderkey having sum(l_quantity) > 300 /* definite */ ;