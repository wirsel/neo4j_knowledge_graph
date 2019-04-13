//merge cypher statement 
//with apoc.map.fromPairs([CYP_ID,$CYP_ID],[CYP_STR,$CYP_STR],[CYP_LINES,$CYP_LINES]) as p
with apoc.map.fromPairs([
	["CYP_ID","test"],
    ["CYP_STR","match(q:C_cyp_query) where exists(q.identifier) \\t with q  \\t return count(q)"],
    //["CYP_STR",""],
    ["CYP_LINES",[]]
    //["CYP_LINES",["match(q:C_cyp_query)","return count(q)"]] 
    ]) as p
//check params
where apoc.meta.type(p.CYP_ID)='STRING' with p
where apoc.meta.type(p.CYP_STR)='STRING' with p    
where apoc.meta.type(p.CYP_LINES)='LIST' with p 
where not p.CYP_ID="" with p
//WHERE not (p.CYP_STR="" and p.CYP_LINES=[] ) WITH p
//WHERE size(p.CYP_STR)>0 or size(p.CYP_LINES)>0 WITH p 
where not (size(p.CYP_STR)>0 and size(p.CYP_LINES)>0)

//derive map (lines_map) of lines in the cypher query
with p,	case     
	when size(p.CYP_STR)>0 and p.CYP_STR CONTAINS ';'   then split(p.CYP_STR,';')    
    when size(p.CYP_LINES)>0                            then p.CYP_LINES     
    when size(p.CYP_STR)>0 and p.CYP_STR CONTAINS '\\n' then split(p.CYP_STR,'\\n') 
    when size(p.CYP_STR)>0 and p.CYP_STR CONTAINS '\\t' then split(p.CYP_STR,'\\t')
	else []     
    end as coll
with p, {
    keys: [x  in range(0, size(coll)-1) | tostring(x)],
    coll: [x in coll | trim(x) ] ,
    map: apoc.map.fromLists(
			[x in range(0, size(coll)-1) | toString(x)], 
    		[x in coll | trim(x) ])
	} as tobe,
	{identifier:p.CYP_ID, name:p.CYP_ID, type:'C_cyp_query'} as cyp_query_props 
MERGE    (q:C_cyp_query {identifier:cyp_query_props.identifier})
set q = cyp_query_props
with p, tobe, q //with p, lines_map,{identifier:p.CYP_ID} as q

//delete edges to existing C_cyp_line and clear existing C_cyp_line
OPTIONAL MATCH    (q)-[ell:HAS_LINE]->(cll:C_cyp_line)
set cll.status = 'not actual'
with p, tobe, q, collect(cll) as aColl
with p, tobe, q, {
	keys: [x in aColl | toString(x.index)],
	nodes: aColl,
	coll: [x in aColl | x.line] ,
    map: apoc.map.fromLists(
		[x in aColl | toString(x.index)], 
    	[x in aColl | {line:x.line,identifier:x.identifier}]
    ) } as asis

//check which existing C_cypher_line can be reused
//UNWIND keys(lines_map) as index_str
//with p, lines_map, q, cll_lines_map, index_str, 
	//tointeger(index_str) as index
	//when cell.line=lineStr and 


//delete edges to existing C_cyp_line and clear existing C_cyp_line
OPTIONAL MATCH    (q)-[ell:HAS_LINE]->(cll:C_cyp_line)
with p, tobe, q, asis, cll, CASE
	when cll.status='actual'    then 'to be detached'
    when cll.status='detached'  then 'detached'
    else 'to be detached'
    end as new_status
set cll.status = new_status


//create new C_cyp_lines
with p, tobe, q, asis
UNWIND keys(tobe.map) as tobe_map_key
with p, q, tobe_map_key,
	tobe.map[tobe_map_key] as tobe_lineStr
where apoc.meta.type(tobe_lineStr)='STRING'
with p, q, tobe_map_key, tobe_lineStr, q.identifier+"/"+tobe_map_key as tobe_line_id
with p, q, tobe_map_key, tobe_lineStr, {
	identifier: tobe_line_id, 
    name:tobe_line_id, 
    type:'C_cyp_line',
    line:tobe_lineStr, 
    index:tointeger(tobe_map_key), 
    status:'actual'
    } as tobe_line_props

MERGE    (l:C_cyp_line {identifier:tobe_line_props.identifier})
set l += tobe_line_props
MERGE      (q)-[el:HAS_LINE]->(l)

with q, collect(l) as new_lines
MATCH    (q)-[elll:HAS_LINE]->(clll:C_cyp_line)
with q, clll, apoc.create.uuid() as uuid
with q, clll, uuid,
	case 
    when clll.status='to be detached' then {
		status :'detached', 
    	identifier_old : clll.identifier,
    	identifier: tostring(uuid)
    	}
     else {} 
     end as cleared_props

//update edge to C_cyp_query for non actual C_cyp_line
Call apoc.do.when(not clll.status='actual','match(l:C_cyp_line {identifier:$ID})<-[ell:HAS_LINE]-(q:C_cyp_query) with l, q, ell merge(q)-[eee:DETACHED_LINE]->(l) delete ell return count(ell)','',{ID:cleared_props.identifier_old}) YIELD value as value2

//update labels for non actual C_cyp_line
Call apoc.do.when(not clll.status='actual','match(l:C_cyp_line {identifier:$ID}) REMOVE l:C_cyp_line SET l:D_cyp_line return count(l)','',{ID:cleared_props.identifier_old}) YIELD value as value1

//update props for non actual C_cyp_line
Call apoc.do.when(not cleared_props={},'match(l:D_cyp_line {identifier:$ID}) Set l += $cleared_props return count(l)','',{ID:cleared_props.identifier_old, cleared_props:cleared_props}) YIELD value as val3

return *
