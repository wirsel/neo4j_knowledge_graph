LOAD CSV FROM 'file:\\20200127_1048_pocket.html' AS line FIELDTERMINATOR '°'
with line limit 100000
with replace(line[0], '<li><a href="','') as line
with line, split(line, '" time_added="') as list
with line, apoc.coll.flatten([list[0]]+split(list[1], '" tags="')) as tags
with line, apoc.coll.flatten(tags[0..-1]+split(tags[-1],'">'))as rest
with line, [i in rest | replace(i, '</a></li>','')] as final 
//with final where  left(final[0],4)="c"
//return distinct left(final[4],3)//, tags 

//return distinct size(final)//, tags 
//return left(final[0],4) limit 10
with line,final,  {type:'c_pocket_record', name:line } as props
merge(n:c_pocket_record {identifier:trim(tolower(props.name))})
set n += props

with line, final,n as r
with line, final,r, {type:'c_url', name:final[0] } as props
merge(n:c_url {identifier:trim(tolower(props.name))})
set n += props
with line, final, r, n as u
merge(r)<-[e:is_url_in_pocket_record]-(u)
set r.url=u.identifier

with line, final, r, u
with line, final,r, u, {type:'c_pocket_timeadded', name:final[1] } as props
merge(n:c_pocket_timeadded {identifier:trim(tolower(props.name))})
set n += props
with line, final, r, u, n as t
merge(r)-[e:time_added]->(t)
set r.time_added=t.identifier

with line, final, r, u, t
with line, final,r, u, t, {type:'c_pocket_rawtags', name:final[2] } as props
merge(n:c_pocket_rawtags {identifier:trim(tolower(props.name))})
set n += props
with line, final, r, u, t, n as rtags
merge(r)-[e:has_raw_tags]->(rtags)
set r.rawtags=rtags.identifier

with line, final, r, u, t, rtags, split(final[2],',') as rtagList
unwind rtagList as item 
	with line, final,r, u, t,rtags, item, {type:'c_pocket_tagraw', name:item } as props
	merge(n:c_pocket_tagraw {identifier:trim(tolower(props.name))})
	set n += props
	with line, final, r, u, t, rtags,item, n as rtag
	merge(r)-[e:has_raw_tag]->(rtag)
	merge(rtags)-[ee:contains_raw_tag]->(rtag)
with line, final, r, u, t, rtags, collect(item) as list



//return rtag
//with line, final, r, u, t, rtags, collect(rtag)
//return rtagList limit 10
return count(distinct final[2] )
