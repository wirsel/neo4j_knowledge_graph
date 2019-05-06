//update C_cyp_query
:param CYP_ID => 'test';
:param SEP => ',';
//:param "CYP_LINES" => ["match(q:C_cyp_query) \\t where exists(q.identifier) \\t  with q  \\t return count(q)"];
:param "CYP_LINES" => ["A,B,C,E,D"];
:param STEPS => [0,1,2,3,4,5,6,7,8];

//////////////////////////////////////////////
//0 derive tobe and merge C_cyp_query
with $STEPS as steps where 0 in steps
with $CYP_ID as CYP_ID, $SEP as SEP , $CYP_LINES as CYP_LINES
with apoc.map.fromPairs([
	["CYP_ID",CYP_ID],
    ["SEP",SEP], 
    ["CYP_LINES",CYP_LINES]
    // ["CYP_LINES",[]]
    ]) as p
where 
	apoc.meta.type(p.CYP_ID)='STRING'  
    and apoc.meta.type(p.CYP_LINES)='LIST' 
    and not p.CYP_ID='' 
with p, [ y in apoc.coll.flatten([x in p.CYP_LINES | split(x,p.SEP)],true) | trim(y) ]  as lines
with p,	{ 
		cnt:size(lines),
        nodes:[x in range(0,size(lines)-1)|-1],
		props:[i in range(0, size(lines)-1)|{ 
					identifier:p.CYP_ID+"/"+tostring(i)+"/"+lines[i],
    				name:p.CYP_ID+"/"+tostring(i)+"/"+lines[i],
    				line:lines[i], 
    				index:tostring(i)}]	} as tobe
with p, tobe, tobe.props as propsList
with p, tobe, apoc.coll.flatten([x in tobe.props | apoc.map.values(x, ['identifier','name','line','index'])] ,true) as prop_values

MERGE    (q:C_cyp_query {identifier:p.CYP_ID})
set q += {name:p.CYP_ID, type:'C_cyp_query'}
set q.tobe_cnt=size(tobe.nodes)
set q.tobe_nodes=[x in range(0,tobe.cnt-1)|-1]
set q.tobe_props_keys=['identifier','name','line','index']
set q.tobe_props_values=apoc.coll.flatten([x in tobe.props | apoc.map.values(x, q.tobe_props_keys)],true)
return count(distinct q);

/////////////////////////////////////////////////////////////////////
// 1: create new C_cyp_query_version
with $STEPS as steps where 1 in steps
with $CYP_ID as CYP_ID

MATCH(q:C_cyp_query {identifier:CYP_ID})
OPTIONAL MATCH(q)-[qbe:BUFFER]->(qb:C_cyp_query_version)
set qbe.status="fromer"
with q, size(collect(distinct qb)) as cnt

//merge C_cyp_query_version
CREATE    (qb:C_cyp_query_version {identifier:tostring(cnt+1)+"/"+q.identifier})
set qb += {name:q.identifier, type:'C_cyp_query_version'}
set qb.nodes=[]
 
merge(q)-[qbe:BUFFER]->(qb)
set qbe.status="actual"
return count(distinct qb);

/////////////////////////////////////////////////////////////////////
// 2: reuse existing C_cyp_line
with $STEPS as steps, $CYP_ID as CYP_ID where 2 in steps
MATCH(q:C_cyp_query {identifier:CYP_ID})-[qbe:BUFFER {status:"actual"}]->(qb:C_cyp_query_version {name:CYP_ID})
with q, qb
Call apoc.coll.partition(q.tobe_props_values,size(q.tobe_props_keys)) Yield value as val
with q, qb, val, apoc.map.fromLists(q.tobe_props_keys, val) as tobe_prop
MATCH(q)-[ee:HAS_LINE]->(l:C_cyp_line {identifier:tobe_prop.identifier})
MERGE(qb)-[e:HAS_LINE]->(l) 
Set e.index=tointeger(tobe_prop.index)
return count(distinct l);

/////////////////////////////////////////////////////////////////////
// 3: set not equal C_cypPline to D_cyp_line
with $STEPS as steps, $CYP_ID as CYP_ID where 3 in steps
MATCH(qb:C_cyp_query_version {name:CYP_ID})<-[qbe:BUFFER {status:"actual"}]-(q:C_cyp_query {name:CYP_ID})-[e:HAS_LINE]->(l:C_cyp_line)
where not (l)-[:HAS_LINE]-(qb)
Set l:D_cyp_line
remove l.C_cyp_line
return count(distinct l);

/////////////////////////////////////////////////////////////////////
// 4: reuse existing D_cyp_line
with $STEPS as steps, $CYP_ID as CYP_ID where 4 in steps
match(q:C_cyp_query {identifier:CYP_ID})
Call apoc.coll.partition(q.tobe_props_values,size(q.tobe_props_keys)) Yield value as aval
with q, collect(aval) as vals
unwind vals as val
with q, apoc.map.fromLists(q.tobe_props_keys, val) as tobe_prop
MATCH(qb:C_cyp_query_version {name:q.identifier})<-[qbe:BUFFER {status:"actual"}]-(q)-[e:HAS_LINE]->(ld:D_cyp_line {line:tobe_prop.line})
with qb,q,tobe_prop, collect(distinct ld) as lds
with qb, q, tobe_prop, lds[0] as ld
MERGE(qb)-[e:HAS_LINE]->(ald)
set e.index=tointeger(tobe_prop.index)
Set ld:C_cyp_line
Remove ld:D_cyp_line
set ld.index=tobe_prop.index
set ld.identifier=tobe_prop.identifier
return count(distinct ld);

/////////////////////////////////////////////////////////////////////
// 5: create new C_cyp_line
with $STEPS as steps, $CYP_ID as CYP_ID where 4 in steps
match(q:C_cyp_query {identifier:CYP_ID})
Call apoc.coll.partition(q.tobe_props_values,size(q.tobe_props_keys)) Yield value as values
with q, apoc.map.fromLists(q.tobe_props_keys, values) as tobe_prop
MATCH(qb:C_cyp_query_version {name:q.identifier})<-[qbe:BUFFER {status:"actual"}]-(q)
where not (qb)-[:HAS_LINE]->(:C_cyp_line {identifier:tobe_prop.identifier})
MERGE (l:C_cyp_line {identifier:tobe_prop.identifier})
set l +=tobe_prop
Set l.type="C_cyp_line"
MERGE (q)-[e:HAS_LINE]->(l)
Set e.index=tointeger(tobe_prop.index)
MERGE (qb)-[ee:HAS_LINE]->(l)
Set ee.index=tointeger(tobe_prop.index)
return count(distinct l);
