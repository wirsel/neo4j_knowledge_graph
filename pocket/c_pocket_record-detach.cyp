//c_pocket_record: detache
MATCH (n:c_pocket_record)-[e]->(o)
set e.status='detached'
return count(e) as cnt
union
MATCH (n:c_pocket_record)<-[e]-(o)
set e.status='detached'
return count(e) as cnt
