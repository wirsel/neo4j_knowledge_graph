with 'pocket.item_saved_5.tsv' as csv
with csv, ['AddedAt','Title','Excerpt','ImageUrl','Tags','Url','CuratedTitle'] as key
with csv, apoc.coll.sort(key) as keys
//CALL apoc.load.csv('file:///w:/'+csv,{}) YIELD lineNo, list, map
CALL apoc.load.csv('file:///w:/'+csv,{sep:'TAB'}) YIELD lineNo, list, map
//LOAD CSV WITH HEADERS FROM 'file:///'+csv as line FIELDTERMINATOR '\t'
with keys, lineNo, map as line
where apoc.coll.disjunction(keys, keys(line)) =[]
with line, lineNo, split(line.AddedAt,' ') as date_arr
with line, lineNo, date_arr, tolower(date_arr[0]) as month
with line, lineNo, date_arr, month, tointeger(replace(date_arr[1],',','')) as day
with line, lineNo, date_arr, month, day, tointeger(date_arr[2]) as year
with line, lineNo, date_arr, month, day, year, split(date_arr[4],':') as timearr
with line, lineNo, date_arr, month, day, year, timearr, tointeger(timearr[0]) as hourpmam
with line, lineNo, date_arr, month, day, year, timearr, hourpmam, tointeger(left(timearr[1],2)) as minute
with line, lineNo, date_arr, month, day, year, timearr, hourpmam, minute, right(timearr[1],2) as pmam
with line, lineNo, date_arr, month, day, year, timearr, hourpmam, minute, pmam, 
	CASE 
    	WHEN pmam='AM' and hourpmam=12 THEN 0
        WHEN pmam='PM' and hourpmam=12 THEN hourpmam
        WHEN not hourpmam=12 and pmam='PM' THEN hourpmam+12 
        WHEN not hourpmam=12 and pmam='AM' THEN hourpmam 
     END AS hour

match(m:C_month {identifier:month})
with line, lineNo, LocalDateTime({year:year, month:m.index, day:day,hour:hour, minute:minute}) as datetime
with line, lineNo, datetime,
	toString( datetime.year)+
	apoc.text.lpad(toString( datetime.month),2,'0')+
	apoc.text.lpad(toString( datetime.day),2,'0')+'-'+
	apoc.text.lpad(toString( datetime.hour),2,'0') +
	apoc.text.lpad(toString( datetime.minute),2,'0') 
as datetimeStr
with line, lineNo, datetime,datetimeStr,
	toString( datetime.year)+
	apoc.text.lpad(toString( datetime.month),2,'0')+
	apoc.text.lpad(toString( datetime.day),2,'0')
as yyyymmdd

merge(ymd:C_yyyymmdd {identifier:yyyymmdd})
on create set ymd.name=ymd.identifier, ymd.type='C_yyyymmdd'

//create url
merge(u:C_url {identifier:trim(line.Url)})
on create set u.name=u.identifier, u.type='C_url'

with u, line, lineNo, datetime, datetimeStr+'_'+apoc.text.lpad(tostring(lineNo),7,'0')+"_"+line.Url as id
With u, line, lineNo, datetime, id, line.Title as title_orig
With u, line, lineNo, datetime, id, title_orig, CASE When line.CuratedTitle='' THEN line.Title ELSE line.CuratedTitle END AS title
merge(p:C_pocket_entry_gsheet {identifier:id })
on create set p.name=p.id, p.type='C_pocket_entry_gsheet', p.url=line.Url, p.index=lineNo+1, p += line
set p.title_orig = title_orig
set p.title=title
//on match set p.name=p.id, p.type='C_pocket_entry_gsheet', p.url=line.Url, p.index=lineNo+1, p += line
merge(p)-[eu:HAS_URL]->(u)
merge(p)-[eymd:RECORDED_AT]->(ymd)

with p, u , split(p.Tags,',') as tags
unwind tags as tag
merge(t:C_pocket_tag {identifier:trim(tag)})
on create set t.name=t.identifier, t.type='C_pocket_tag'
merge(p)-[et:HAS_TAG]->(t)

return count(distinct p)
//return distinct '2' as task, apoc.text.join([tostring(year),tostring(month),tostring(day)],'') as  res1
