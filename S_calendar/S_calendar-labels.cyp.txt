    
//S_calendar labels
CREATE CONSTRAINT ON (n:S_calendar) ASSERT n.identifier IS UNIQUE;
CREATE CONSTRAINT ON (n:C_yyyymmdd) ASSERT n.identifier IS UNIQUE;
CREATE CONSTRAINT ON (n:C_month) ASSERT n.identifier IS UNIQUE;
match(p:C_yyyymmdd) 
set p:S_calendar return count(p);
match(p:C_month) 
set p:S_calendar return count(p);
