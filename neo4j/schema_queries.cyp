//batch: schema: create cronstraints
//match(b:H_batch {identifier:'batch: schema: merge constraints'})
//detach delete b
//return 'step 0' as step, '' as res1, '' as res2, '' as res3;

//union
with {
PROTOCOL : 'file:///',
STEPS : [
	'x0 create constraint for LABEL_COLUMN',
    'x1 create constraint for LABEL_NEW_COLUMN',
    //handle type attribute
    'x2 dump wrong c.type="C_label" for each LABEL_COLUMN',
    'x3 dump wrong c.type="C_label" for each LABEL_NEW_COLUMN',    
    'x4 enforce c.type="C_label" for each LABEL_COLUMN',
    'x5 enforce c.type="C_label" for each LABEL_NEW_COLUMN',
    //handle new Labels
    '6 count nodes per label to be relabeled',    
    ''
],
PATH : 'w:/github/neo4j_knowledge_graph/neo4j/',
FILE : 'space_labels.csv',
SEP : '|',
HEADER : true,
LABEL_COLUMN : 'label_name',
LABEL_NEW_COLUMN : 'label_rename',
identifier:'batch: schema: merge constraints'
} as props
match(b:H_batch {identifier:'batch: schema: merge constraints'})
//with b, keys(b) as keys
//with b, apoc.map.removeKeys(keys,['identifier','type']) as keysToRemove
set b = props
return 'step 1' as step, properties(b) as res1, '' as res2, '' as res3;

//------------------
with '0' as s
match(b:H_batch {identifier:'batch: schema: merge constraints'})
where s+' create constraint for LABEL_COLUMN' = b.STEPS[tointeger(s)]
with s, b
Call apoc.load.csv(b.PROTOCOL+b.PATH+b.FILE, {header:b.HEADER,sep:b.SEP})  yield  lineNo,list, map
where not map[b.LABEL_COLUMN]= "" 
with s, b, collect(map[b.LABEL_COLUMN]) as keys1
with s, b, [x in keys1 | trim(x) ] as keys
with s, b, keys, [x in range(0,size(keys)-1) | ['identifier']] as values
with s, b, keys, values, apoc.map.fromLists(keys,values) as map
call apoc.schema.assert({},map,false) yield label
return 'step '+s as step, map as  res1, '' as res2, '' as res3;

//------------------
with '1' as s
match(b:H_batch {identifier:'batch: schema: merge constraints'})
where s+' create constraint for LABEL_NEW_COLUMN' = b.STEPS[tointeger(s)]
with b, s
Call apoc.load.csv(b.PROTOCOL+b.PATH+b.FILE, {header:b.HEADER,sep:b.SEP})  yield  lineNo,list, map
where not map[b.LABEL_NEW_COLUMN]= "" 
with b, s, collect(map[b.LABEL_NEW_COLUMN]) as keys1
with b, s, [x in keys1 | trim(x) ] as keys
with b, s, keys, [x in range(0,size(keys)-1) | ['identifier']] as values
with b, s, keys, values, apoc.map.fromLists(keys,values) as map
call apoc.schema.assert({},map,false) yield label
return 'step '+s as step, map as  res1, '' as res2, '' as res3;

//------------------
with '2' as s
match(b:H_batch {identifier:'batch: schema: merge constraints'})
where s+' dump wrong c.type="C_label" for each LABEL_COLUMN' = b.STEPS[tointeger(s)]
with b, s
Call apoc.load.csv(b.PROTOCOL+b.PATH+b.FILE, {header:b.HEADER,sep:b.SEP})  yield  lineNo,list, map
where not map[b.LABEL_COLUMN]= ''
//with map, b //where map[b.LABEL_COLUMN]= 'C_month'
with b, s, map[b.LABEL_COLUMN] as label
with b, s, label, replace('
match(n:<label>) where not coalesce(n.type,"")="<label>" 
with n, "<label>" as labels
with labels, size(collect(n)) as cnt
return labels+"="+tostring(cnt) as cnt
' ,'<label>', label) as cyp
Call apoc.do.when(True, cyp, '',{}) yield value
with b, s, apoc.text.join( collect(value.cnt),' | ') as res
Set b += apoc.map.fromValues(['step'+s,res])
return 'step '+s as step, res as  res1, '' as res2, '' as res3;

//------------------
with '3' as s
match(b:H_batch {identifier:'batch: schema: merge constraints'})
where s+' dump wrong c.type="C_label" for each LABEL_NEW_COLUMN' = b.STEPS[tointeger(s)]
with s, b
Call apoc.load.csv(b.PROTOCOL+b.PATH+b.FILE, {header:b.HEADER,sep:b.SEP})  yield  lineNo,list, map
where not map[b.LABEL_NEW_COLUMN]= ''
//with map, b //where map[b.LABEL_COLUMN]= 'C_month'
with s, b, map[b.LABEL_NEW_COLUMN] as label
with s, b, label, replace('
match(n:<label>) where not coalesce(n.type,"")="<label>" 
with n, "<label>" as labels
with labels, size(collect(n)) as cnt
return labels+"="+tostring(cnt) as cnt
' ,'<label>', label) as cyp
Call apoc.do.when(True, cyp, '',{}) yield value
with s, b, apoc.text.join( collect(value.cnt),' | ') as res
Set b += apoc.map.fromValues(['step'+s,res])
return 'step '+s as step, res as  res1, '' as res2, '' as res3;

//------------------
with '4' as s
match(b:H_batch {identifier:'batch: schema: merge constraints'})
where s+' enforce c.type="C_label" for each LABEL_COLUMN' = b.STEPS[tointeger(s)]
with s, b
Call apoc.load.csv(b.PROTOCOL+b.PATH+b.FILE, {header:b.HEADER,sep:b.SEP})  yield  lineNo,list, map
where not map[b.LABEL_COLUMN]= ''
//with map, b //where map[b.LABEL_COLUMN]= 'C_month'
with s, b, map[b.LABEL_COLUMN] as label
with s, b, label, replace('
match(n:<label>) where not coalesce(n.type,"")="<label>" 
set n.type="<label>"
return count(n) as cnt
' ,'<label>', label) as cyp
Call apoc.do.when(True, cyp, '',{}) yield value
with s, b, label, value.cnt as cnt
where cnt>0
with s, b, apoc.text.join(collect( label+"="+tostring(cnt)),'|') as res
Set b += apoc.map.fromValues(['step'+s,res])
return 'step '+s as step, res as  res1, '' as res2, '' as res3;

//------------------
with '5' as s
match(b:H_batch {identifier:'batch: schema: merge constraints'})
where s+' enforce c.type="C_label" for each LABEL_NEW_COLUMN' = b.STEPS[tointeger(s)]
with s, b
Call apoc.load.csv(b.PROTOCOL+b.PATH+b.FILE, {header:b.HEADER,sep:b.SEP})  yield  lineNo,list, map
where not map[b.LABEL_NEW_COLUMN]= ''
//with map, b //where map[b.LABEL_NEW_COLUMN]= 'C_month'
with b, s, map[b.LABEL_NEW_COLUMN] as label
with b, s, label, replace('
match(n:<label>) where not coalesce(n.type,"")="<label>" 
set n.type="<label>"
return count(n) as cnt
' ,'<label>', label) as cyp
Call apoc.do.when(True, cyp, '',{}) yield value
with b, s, label, value.cnt as cnt
where cnt>0
with b, s, apoc.text.join(collect( label+"="+tostring(cnt)),'|') as res
Set b += apoc.map.fromValues(['step'+s,res])
return 'step '+s as step, res as  res1, '' as res2, '' as res3;

//------------------
with '6' as s
match(b:H_batch {identifier:'batch: schema: merge constraints'})
where s+' count nodes per label to be relabeled' = b.STEPS[tointeger(s)]
with s, b 
Call apoc.load.csv(b.PROTOCOL+b.PATH+b.FILE, {header:b.HEADER,sep:b.SEP})  yield  lineNo,list, map
with s, b, map[b.LABEL_COLUMN] as asis, map[b.LABEL_NEW_COLUMN] as tobe
where not asis=tobe and not (asis='' or tobe='')
//with map, b //where map[b.LABEL_NEW_COLUMN]= 'C_month'
with s, b, asis, tobe, replace('
match(n:<asis>) 
return count(n) as cnt
' ,'<asis>', asis) as cyp
Call apoc.do.when(True, cyp, '',{}) yield value
with s, b, collect( asis+"="+tostring(value.cnt))as res
Set b += apoc.map.fromValues(['step'+s,res])
return 'step '+s as step, res as  res1, '' as res2, '' as res3;
