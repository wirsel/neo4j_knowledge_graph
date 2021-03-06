{
    source: 'S_json_load_check_pk.cyp' ,
    test: 'test',
    cypher: [
{ line: 'with §lg as lg ', comment: '//a C_log node is expected here as an input'},
{ line: 'with *, ""<<url>>"" as file ', comment: '// url to the locarion of the file that contains the json'},
{ line: 'with *, ""<<array>>"" as array ', comment: '//the element in the json that contains the array of items to loadk'},
{ line: 'with *, ""<<pk>>""as pk ', comment: '//the key in the json items that is considered to be a primary key '},
{ line: 'call apoc.load.json(file,"""",{}) yield value'},
{ line: 'UNWIND value[""<<array>>""] as item'},
{ line: 'with lg,item[""<<pk>>""] as <<pk>>_value, '},
{ line: 'COLLECT(apoc.convert.toString(item[""<<pk>>""])) as coll'},
{ line: 'with lg,'},
{ line: 'COLLECT([<<pk>>_value, size(coll)])as collcoll'},
{ line: 'with lg, filter(x in collcoll where x[1]> <<max_cnt>> ) as res'},
{ line: 'with lg, res, '},
{ line: 'CASE '},
{ line: 'WHEN res=[] THEN ""SUCCESS! All valuess for key /<<pk>>/ occurred not more than <<max_cnt>> times in the json file""'},
{ line: 'ELSE ""ERROR! The following valuess for key /<<pk>>/ occurred more the <<max_cnt>> times in the json file := ""+ apoc.convert.toString(res) '},
{ line: 'END as logstr'},
{ line: 'set lg.<<resKey>> = logstr'},
{ line: 'return 0'}
],
    in:['url', 'array', 'pk', 'max_cnt', 'pk', 'resKey'],
    out:['reskey'],
    params:['lg']
}
