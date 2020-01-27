With datetime() as time
with 
	time.year
    +case when time.month<10 then '0'+tostring(time.month) ELSE  tostring(time.month)  END
    +case when time.day<10 then '0'+tostring(time.day) ELSE  tostring(time.day)  END
    +case when time.hour<10 then '0'+tostring(time.hour) ELSE  tostring(time.hour)  END 
    +case when time.minute<10 then '0'+tostring(time.minute) ELSE  tostring(time.minute)  END 
    +case when time.second<10 then '0'+tostring(time.second) ELSE  tostring(time.second)  END 
    +case when time.millisecond<10 then '0'+tostring(time.millisecond) ELSE  tostring(time.millisecond)  END
 as now

//LOAD CSV FROM 'file:\\20200127_1048_pocket.html' AS line FIELDTERMINATOR '°'
CALL apoc.load.csv('FILE:///20200127_1048_pocket.html',{sep:'°',header:False}) YIELD lineNo, map, list
with now, apoc.convert.toString(list[0]) as line limit 100000
with now, replace(line, '<li><a href="','') as line
with now, line, split(line, '" time_added="') as list
with now, line, list, apoc.coll.flatten([list[0]]+split(list[1], '" tags="')) as tags
//with line, list, CASE WHEN list[1] contains ' tags=">' 	THEN replace(list[1], ELSE 
with now, line, tags,
CASE When left(tags[2],1)=">" THEN apoc.coll.flatten(tags[0..-1]+['']+split(tags[-1],'">')) 
ELSE apoc.coll.flatten(tags[0..-1]+split(tags[-1],'">')) END as rest
with now, line, [i in rest | replace(i, '</a></li>','')] as final 
with now, line, final, {
	url:final[0] , 
    time_added:tostring(final[1]),
    tags: tostring(coalesce(final[2],'')), 
    description: tostring(coalesce(final[3],''))}
as fp


//----merge(c_pocket_record)------
with now, line, fp, {type:'c_pocket_record', name:fp.url+'|'+ fp.time_added, last_update:now } as props
merge(n:c_pocket_record {identifier:trim(tolower(props.name))})
set n += props
set n += fp
with now, line, fp, n as r

//----merge(c_url)------
//----merge(c_pocket_record)-[is_url_in_pocket_record]-(c_url)
with now, line, fp, r, {type:'c_url', name:fp.url, last_update:now } as props
merge(n:c_url {identifier:trim(tolower(props.name))})
set n += props
with now, line, fp, r, n as u
merge(r)<-[e:is_url_in_pocket_record]-(u)
set u.pocket_record_id=r.identifier

//----merge(c_pocket_timeadded)------
//----merge(c_pocket_record)-[time_added]-(c_pocket_timeadded)
with line, fp, r, u
with line, fp, r, u, {type:'c_pocket_timeadded', name:fp.time_added } as props
merge(n:c_pocket_timeadded {identifier:trim(tolower(props.name))})
set n += props
with line, fp, r, u, n as t
merge(r)-[e:time_added]->(t)
set t.pocket_record_id=r.identifier

//----merge(c_pocket_rawtags)------
//----merge(c_pocket_record)-[has_raw_tags]-(c_pocket_rawtags)
with line, fp, r, u, t
with line, fp, r, u, t, {type:'c_pocket_rawtags', name:fp.tags } as props
merge(n:c_pocket_rawtags {identifier:trim(tolower(props.name))})
set n += props
with line, fp, r, u, t, n as rtags
merge(r)-[e:has_raw_tags]->(rtags)
set rtags.pocket_record_id=r.identifier

//----merge(c_pocket_tagraw)------
//----merge(c_pocket_record)-[has_raw_tag]-(c_pocket_tagraw)
//----merge(c_pocket_rawtags)-[contains_raw_tag]-(c_pocket_tagraw)
with line, fp, r, u, t, rtags, split(fp.tags,',') as rtagList
unwind rtagList as item 
	with line, fp, r, u, t,rtags, item, {type:'c_pocket_tagraw', name:trim(item) } as props
	merge(n:c_pocket_tagraw {identifier:trim(tolower(props.name))})
	set n += props
	with line, fp, r, u, t, rtags,item, n as rtag
	merge(r)-[e:has_raw_tag]->(rtag)
	merge(rtags)-[ee:contains_raw_tag]->(rtag)
    set rtag.pocket_record_id=r.identifier
with line, fp, r, u, t, rtags, collect(item) as list

return count(r)
