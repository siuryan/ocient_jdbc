/* THIS SCRIPT REQUIRES VERIFICATION OF VALUES */
;
list tables /* definite */ ;
list /* a comment */ tables /* definite */ ;
list tab/* a comment in the middle of a word */les /* definite */ ;
/* a comment at the beginning */ list tables /* definite */ ;
list tables /* a comment near the end */ ;
list tables; /* a comment at the end */
list /* a multiline
comment */ tables;
list /* a multiline comment;
the previous line ends in a semicolon */ tables;
list /* a
three-line
comment */ tables;
list /* multiple */ tab/* comments */les /* definite */ ;
list /* multiple */ tab/* comments;
some of them have semicola */les;
list /* /* /* an odd-looking comment which is in fact fine */ tables /* definite */;
list /* an odd comment which is indeed a syntax error */ */ */ tables /* stxerror */;
list -- a single-line comment
tables;
list -- a single-line comment, ending in a semicolon;
tables;
list /* a comment--with a dash */ tables;
list -- a single-line comment with */ a star and a slash
tables;
describe system.ingresstest /* definite */ ;
describe SyStEm.INGRESSTEST /* definite */ ;
describe SyStEm."ingresstest" /* definite */ ;
describe SyStEm."INGRESSTEST" /* notfound */ ;
select 'a LITERAL string' from system.ingresstest /* definite */ ;
select 'a LITERAL string with "double quotes" in it' from system.ingresstest /* definite */ ;
select 'a LITERAL string with " one literal double quote' from system.ingresstest /* definite */ ;
select 'a LITERAL string with '' one literal single quote' from system.ingresstest /* definite */ ;
select 'a LITERAL string /* with a comment in it */' from system.ingresstest /* definite */ ;
select 'a LITERAL string --with a single-line comment in it */' from system.ingresstest /* definite */ ;
select 'a LITERAL string; it has a semicolon' from system.ingresstest /* definite */ ;
select 'a LITERAL string;
it has a semicolon' from system.ingresstest;
