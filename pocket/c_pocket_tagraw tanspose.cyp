MATCH (ymd_hms:c_yyyymmdd_hhmmss)-[]-(r:c_pocket_record)-[]-(rtags:c_pocket_rawtags)-[]-(n:c_pocket_tagraw) 
where 
not exists(n.tag_without_pos)
and n.identifier =~ '([^\\.]+)\\.{2}([^!~]*)!~\\d+$' 
with n,  [i in split(n.identifier,'!') | replace(i, '~','')] as list
set n.pos=tointeger(list[1])
set n.tag_without_pos=list[0]+'!'
set n.pos_delimiter='!'
return n.identifier, n.tag_without_pos, n.pos

union all
MATCH (ymd_hms:c_yyyymmdd_hhmmss)-[]-(r:c_pocket_record)-[]-(rtags:c_pocket_rawtags)-[]-(n:c_pocket_tagraw) 
where 
not exists(n.tag_without_pos)
and n.identifier =~ '([^\\.]+)\\.{2}([^!~]*)!\\d+$' 
with n,  [i in split(n.identifier,'!') | replace(i, '~','')] as list
set n.pos=tointeger(list[1])
set n.tag_without_pos=list[0]+'!'
set n.pos_delimiter='!'
return n.identifier, n.tag_without_pos, n.pos

MATCH (ymd_hms:c_yyyymmdd_hhmmss)-[]-(r:c_pocket_record)-[]-(rtags:c_pocket_rawtags)-[]-(n:c_pocket_tagraw) 
where 
not exists(n.tag_without_pos)
and n.identifier =~ '([^\\.!/]+)\\.{2}([^/]+)/\\d+!' 
with n,  [i in split(n.identifier,'/') | replace(i, '!','')] as list
set n.pos=tointeger(list[1])
set n.tag_without_pos=list[0]+'!'
set n.pos_delimiter='/'
//return list[0] as tag, list[1] as pos order by tag//n.identifier, n.tag_without_pos, n.pos
return distinct list[0] as tag order by tag
