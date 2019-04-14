:param CYP_ID => 'test';
:param SEP => '\\t';
:param "CYP_LINES" => ["match(q:C_cyp_query) \\t where exists(q.identifier) \\t  with q  \\t return count(q)"];
/////////////////////////////////////////
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

//derive tobe
with p, [ y in apoc.coll.flatten([x in p.CYP_LINES | split(x,p.SEP)],true) | trim(y) ]  as lines
with p, 
	{ 
		cnt:size(lines),
        nodes:[x in range(0,size(lines)-1)|-1],
		props:
    		[
    			i in range(0, size(lines)-1)| 
        		{ 
					identifier:p.CYP_ID+"/"+tostring(i)+"/"+lines[i],
    				name:p.CYP_ID+"/"+tostring(i)+"/"+lines[i],
    				line:lines[i], 
    				index:tostring(i)
            	}
      		]
	} as tobe
with p, tobe, tobe.props as propsList
with p, tobe, apoc.coll.flatten([x in tobe.props | apoc.map.values(x, ['identifier','name','line','index'])] ,true) as prop_values

/////////////////////////////////////////////////////////////////////
//A merge C_cyp_query
MERGE    (q:C_cyp_query {identifier:p.CYP_ID})
set q += {name:p.CYP_ID, type:'C_cyp_query'}
set q.tobe_cnt=size(tobe.nodes)
set q.tobe_nodes=[x in range(0,tobe.cnt-1)|-1]
set q.tobe_props_keys=['identifier','name','line','index']
set q.tobe_props_values=apoc.coll.flatten([x in tobe.props | apoc.map.values(x, q.tobe_props_keys)],true)

//B merge B_cyp_query
MERGE    (qb:B_cyp_query {identifier:p.CYP_ID})
set qb += {name:p.CYP_ID, type:'C_cyp_query'}
set qb.nodes=[]
//B 
merge(q)-[qbe:BUFFER]->(qb)
return p, properties(q), prop_values;

/////////////////////////////////////////////////////////////////////
// C: reuse existing C_cyp_line
MATCH(q:C_cyp_query {identifier:'test'})-[qbe:BUFFER]->(qb:B_cyp_query {identifier:'test'})
with q, qb
Call apoc.coll.partition(q.tobe_props_values,size(q.tobe_props_keys)) Yield value as val
with q, qb, val, apoc.map.fromLists(q.tobe_props_keys, val) as tobe_prop
MATCH(q)-[ee:HAS_LINE]->(l:C_cyp_line {identifier:tobe_prop.identifier})
MERGE(qb)-[e:HAS_LINE]->(l) 
Set e.index=tointeger(tobe_prop.index)
return count(distinct l);

/////////////////////////////////////////////////////////////////////
// D: set not equal C_cypPline to D_cyp_line
MATCH(qb:B_cyp_query {identifier:'test'})<-[qbe:BUFFER]-(q:C_cyp_query {identifier:'test'})-[e:HAS_LINE]->(l:C_cyp_line)
where not (l)-[:HAS_LINE]-(qb)
Set l:D_cyp_line
remove l.C_cyp_line
return count(distinct l);

/////////////////////////////////////////////////////////////////////
// E: reuse existing D_cyp_line
match(q:C_cyp_query {identifier:'test'})
Call apoc.coll.partition(q.tobe_props_values,size(q.tobe_props_keys)) Yield value as val
with q, apoc.map.fromLists(q.tobe_props_keys, val) as tobe_prop
MATCH(qb:B_cyp_query {identifier:'test'})<-[qbe:BUFFER]-(q)-[e:HAS_LINE]->(ld:D_cyp_line {line:tobe_prop.line})
with qb,q,ld,tobe_prop limit 1
MERGE(qb)-[e:HAS_LINE]->(ld)
set e.index=tointeger(tobe_prop.index)
Set ld:C_cyp_line
Remove ld:D_cyp_line
set ld.index=tobe_prop.index
set ld.identifier=tobe_prop.identifier
return count(distinct ld);

/////////////////////////////////////////////////////////////////////
// F: create new C_cyp_line
match(q:C_cyp_query {identifier:'test'})
Call apoc.coll.partition(q.tobe_props_values,size(q.tobe_props_keys)) Yield value as values
with q, apoc.map.fromLists(q.tobe_props_keys, values) as tobe_prop
MATCH(qb:B_cyp_query {identifier:'test'})<-[qbe:BUFFER]-(q)
where not (qb)-[:HAS_LINE]->(:C_cyp_line {identifier:tobe_prop.identifier})
MERGE (l:C_cyp_line {identifier:tobe_prop.identifier})
set l +=tobe_prop
Set l.type="C_cyp_line"
MERGE (q)-[e:HAS_LINE]->(l)
Set e.index=tointeger(tobe_prop.index)
MERGE (qb)-[ee:HAS_LINE]->(l)
Set ee.index=tointeger(tobe_prop.index)
return count(distinct l);
