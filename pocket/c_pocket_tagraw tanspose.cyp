MATCH (ymd_hms:c_yyyymmdd_hhmmss)-[]-(r:c_pocket_record)-[]-(rtags:c_pocket_rawtags)-[]-(n:c_pocket_tagraw) 
where 
 n.identifier =~ '([^\\.]+)\\.{2}([^!~]*)!~\\d+$' 
 //and n.identifier =~ '[^~]+~\\d+$'
//set n.status='wrong:xxx...yyyy!'
with n,  [i in split(n.identifier,'!') | replace(i, '~','')] as list
set n.pos=tointeger(list[1])
set n.tag_without_pos=list[0]+'!'
return n.identifier, n.tag_without_pos, n.pos
