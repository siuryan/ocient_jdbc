/* THIS SCRIPT REQUIRES VERIFICATION OF VALUES */
;
select 0 from tpch.region /* definite */ ;
select 0 from /* a comment */ tpch.region /* definite */ ;
select 0 from tpch.reg/* a comment in the middle of a word */ion /* definite */ ;
/* a comment at the beginning */ select 0 from tpch.region /* definite */ ;
select 0 from tpch.region /* a comment near the end */ ;
select 0 from tpch.region; /* a comment at the end */
select 0 from /* a multiline
comment */ tpch.region;
select 0 from /* a multiline comment;
the previous line ends in a semicolon */ tpch.region;
select 0 from /* a
three-line
comment */ tpch.region;
select 0 from /* multiple */ tpch.reg/* comments */ion /* definite */ ;
select 0 from /* multiple */ tpch.reg/* comments;
some of them have semicola */ion;
select 0 from /* /* /* an odd-looking comment which is in fact fine */ tpch.region /* definite */;
select 0 from /* an odd comment which is indeed a syntax error */ */ */ tpch.region /* stxerror */;
select 0 from -- a single-line comment
tpch.region;
select 0 from -- a single-line comment, ending in a semicolon;
tpch.region;
select 0 from /* a comment--with a dash */ tpch.region;
select 0 from -- a single-line comment with */ a star and a slash
tpch.region;
select 0 from TpCh."region" /* definite */ ;
select 0 from TpCh."REGION" /* notfound */ ;
select 'a LITERAL string' from tpch.region /* definite */ ;
select 'a LITERAL string with "double quotes" in it' from tpch.region /* definite */ ;
select 'a LITERAL string with " one literal double quote' from tpch.region /* definite */ ;
select 'a LITERAL string with '' one literal single quote' from tpch.region /* definite */ ;
select 'a LITERAL string /* with a comment in it */' from tpch.region /* definite */ ;
select 'a LITERAL string --with a single-line comment in it */' from tpch.region /* definite */ ;
select 'a LITERAL string; it has a semicolon' from tpch.region /* definite */ ;
select 'a LITERAL string;
it has a semicolon' from tpch.region;
