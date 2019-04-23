//param dict
:param p => {
level: 1,
zero: 0,
pathSep: '/',
indexSep: '.',
listSep: '°',
stepts: [
{cyp:'nodes.parent.cyphers.erase'}
],
steps: [
{cyp:'nodes.parent.cyphers.merge'},
{cyp:'nodes.childs.cyphers.merge'},
{cyp:'edges.pa2ch.cyphers.prepareForUpdate'},
{cyp:'edges.pa2ch.cyphers.update'},
{cyp:'edges.ch2ch.cyphers.prepareForUpdate'}
],
nodes: {
level:2,
parent: {
name: '-----------------------------    nodes.parent    ------------------------------',
level: 3,
primaryKey: 'identifier',
var: 'pa',
label: 'C_cyp_query',
space:'S_cyp',
action: 'merge',
cyphers:{
level: 4,
erase: {
level: 5,
name: '-------nodes.parent.cyphers.erase-----------------------------------',
params: [],
replaces: [ {
nt: 'nodes.parent.var',
nt_class: 'nodes.parent.label',
nt_pk_val: 'nodes.parent.inputs.identifier',
nt_pk_key: 'nodes.parent.primaryKey'
}],
cyp:'
OPTIONAL MATCH(<<nt>>: <<nt_class>> { <<nt_pk_key>> :"<<nt_pk_val>>"}) 
DETACH DELETE <<nt>>
RETURN CASE WHEN <<nt>> is null THEN "/<<nt_class>>:<<nt_pk_val>>/not found" ELSE "/<<nt_class>>:<<nt_pk_val>>/ deleted" END as result'
},
merge: {
level: 5,
name: '-------nodes.parent.cyphers.merge-------------------------------------',
params: [],
replaces: [ {
nt: 'nodes.parent.var',
nt_class: 'nodes.parent.label',
nt_pk_val: 'nodes.parent.inputs.identifier',
nt_pk_key: 'nodes.parent.primaryKey'
}],
cyp:'
MERGE(<<nt>>: <<nt_class>> { <<nt_pk_key>> :"<<nt_pk_val>>"}) 
Set <<nt>>.name = <<nt>>.<<nt_pk_key>>
Set <<nt>>.type = "<<nt_class>>"
Set <<nt>>.path = <<nt>>.<<nt_pk_key>>
return null',
return: 'return <<nt>> as <<nt>>'
},
match: {
level: 5,
name: '-------nodes.parent.cyphers.match---------------------------------------',
params: [],
replaces: [ {
nt: 'nodes.parent.var',
nt_class: 'nodes.parent.label',
nt_pk_val: 'nodes.parent.inputs.identifier',
nt_pk_key: 'nodes.parent.primaryKey'
}],
cyp: '
MATCH(<<nt>>:<<nt_class>> { <<nt_pk_key>> : "<<nt_pk_val>>"})
return null',
return: 'return <<nt>> as <<nt>>'
}},
inputs: {
level: 4,
identifier: 'new'
},
props: {
level: 4,
name: 'pa.identifier',
thisName: 'pa.identifier',
path: 'pa.identifier',
content: 'pa.identifier',
index: -1,
version: -1
}},
childs: {
level: 3,
name: '--------------------     nodes.childs    ----------------------------------------------',
primaryKey: 'identifier',
action: 'merge', 
var: 'ch',
label: 'C_cyp_line_pos',
space:'S_cyp',
cyphers: {
level: 4,
merge: { 
name: '-------nodes.childs.cyphers.merge---------------------------------------',
level: 5,
params: [],
replaces: [ {
childs: 'nodes.childs.inputs.content',
list_sep: 'listSep',
nt: 'nodes.parent.var',
nt_class: 'nodes.parent.label',
nt_pk_val: 'nodes.parent.inputs.identifier',
nt_pk_key: 'nodes.parent.primaryKey',
index_sep: 'indexSep',
path_sep: 'pathSep',
no: 'nodes.childs.var', 
no_class: 'nodes.childs.label',
no_pk_key: 'nodes.childs.primaryKey'
}],
cyp:'
With "<<childs>>" as childsstr
With [x in split(childsstr,"<<list_sep>>") | trim(x)] as childs
MATCH(<<nt>>:<<nt_class>> { <<nt_pk_key>> : "<<nt_pk_val>>" })
With *
UNWIND range(0, size(childs)-1) as index
With *, apoc.text.lpad(tostring(index),3,"0") as indexStr
With *, <<nt>>.path +"<<path_sep>>"+ indexStr+"<<index_sep>>"+ childs[index] as id
MERGE(<<no>>: <<no_class>> { <<no_pk_key>> : id})
set <<no>>.name = <<no>>.<<nt_pk_key>>
set <<no>>.thisName = childs[index]
set <<no>>.type = "<<no_class>>"
set <<no>>.path = <<no>>.<<no_pk_key>>
set <<no>>.index = index
With <<nt>>, 
COLLECT(<<no>>) as coll
return null',
return: 'return coll'
}},
inputs: {
level: 4,
content: 'f°b °c °d °e'
},
props: {
level: 4,
identifier: '',
name: '',
thisName: '',
path: '',
index: -1,
version: -1
}}},
edges: {
name: '============= EDGES ========================
level:2,
ch2ch:{
level:3,
primaryKey: '',
var: 'ch2ch',
edgeType: 'NEXT_LINE_POS',
space:'S_cyp',
cyphers:{
level: 4,
prepareForUpdate: { 
name: '-------edges.ch2ch.cyphers.prepareForUpdate-------------------------------',
level: 5,
params: [],
replaces: [ {
nt: 'nodes.parent.var',
nt_class: 'nodes.parent.label',
nt_pk_val: 'nodes.parent.inputs.identifier',
nt_pk_key: 'nodes.parent.primaryKey',
no: 'nodes.childs.var', 
no_class: 'nodes.childs.label',
ed: 'edges.pa2ch.var',
ed_type: 'edges.pa2ch.edgeType'
nn: 'nodes.childs.var', 
nn_class: 'nodes.childs.label',
en: 'edges.ch2ch.var',
en_type: 'edges.ch2ch.edgeType'
}],
cyp:'
MATCH(<<nt>>:<<nt_class>> { <<nt_pk_key>> : "<<nt_pk_val>>" })
With <<nt>>
MATCH(<<nt>>)-[ :<<ed_type>>]->(<<no>>:<<no_class>>)
With <<no>>
MATCH(<<no>>)-[ <<edn>>:<<edn_type>>]->(<<nn>>:<<nn_class>>)

Set <<ed>>.status="to be updated"
With <<nt>>, 
COLLECT(<<ed>>) as coll
return null',
return: 'return coll'
}}},
pa2ch:{
level:3,
primaryKey: '',
var: 'pa2ch',
edgeType: 'HAS_LINE',
space:'S_cyp',
action: 'merge',
cyphers:{
level: 4,
prepareForUpdate: { 
name: '-------edges.childs.cyphers.prepareForUpdate-------------------------------',
level: 5,
params: [],
replaces: [ {
nt: 'nodes.parent.var',
nt_class: 'nodes.parent.label',
nt_pk_val: 'nodes.parent.inputs.identifier',
nt_pk_key: 'nodes.parent.primaryKey',
no: 'nodes.childs.var', 
no_class: 'nodes.childs.label',
ed: 'edges.pa2ch.var',
ed_type: 'edges.pa2ch.edgeType'
}],
cyp:'
MATCH(<<nt>>:<<nt_class>> { <<nt_pk_key>> : "<<nt_pk_val>>" })
With <<nt>>
MATCH(<<nt>>)-[ <<ed>>:<<ed_type>>]->(<<no>>:<<no_class>>)
Set <<ed>>.status="to be updated"
With <<nt>>, 
COLLECT(<<ed>>) as coll
return null',
return: 'return coll'
},
update: {
name: '-------edges. pa2ch.cyphers.update---------------------------------------',
level: 5,
params: [],
replaces: [ {
childs: 'nodes.childs.inputs.content',
list_sep: 'listSep',
nt: 'nodes.parent.var',
nt_class: 'nodes.parent.label',
nt_pk_val: 'nodes.parent.inputs.identifier',
nt_pk_key: 'nodes.parent.primaryKey',
index_sep: 'indexSep',
path_sep: 'pathSep',
no: 'nodes.childs.var', 
no_class: 'nodes.childs.label',
no_pk_key: 'nodes.childs.primaryKey',
ed: 'edges.pa2ch.var',
ed_type: 'edges.pa2ch.edgeType'
}],
cyp:'
With "<<childs>>" as childsstr
With [x in split(childsstr,"<<list_sep>>") | trim(x)] as childs
MATCH(<<nt>>:<<nt_class>> { <<nt_pk_key>> : "<<nt_pk_val>>" })
With *
UNWIND range(0, size(childs)-1) as index
With *, apoc.text.lpad(tostring(index),3,"0") as indexStr
With *, <<nt>>.path +"<<path_sep>>"+ indexStr+"<<index_sep>>"+ childs[index] as id
MATCH(<<no>>: <<no_class>> { <<no_pk_key>> : id})
MERGE(<<nt>>)-[<<ed>>:<<ed_type>>]->(<<no>>)
set <<ed>>.index = index
Set <<ed>>.status="actual"
With <<nt>>, 
COLLECT(<<ed>>) as coll
MATCH(<<nt>>)-[<<ed>>:<<ed_type>>]->(<<no>>:<<no_class>>)
where <<ed>>.status="to be updated"
Set <<ed>>.status="detached"
With <<nt>>, 
COLLECT(<<ed>>) as coll
return null',
return: 'return coll'
}},
inputs: {
level: 4
},
props: {
level: 4
}}}};
