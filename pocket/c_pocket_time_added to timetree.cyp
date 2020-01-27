MATCH (r:c_pocket_record)-[]-(t:c_pocket_timeadded) 
with  r, t, tointeger(t.identifier) as epoch
with r, t, epoch, datetime({ epochSeconds:epoch }) as str

with *, {typ:'c_yyyy', name:apoc.date.format(epoch, 's','yyyy')} as props
merge (n:c_yyyy {identifier:trim(tolower(props.name))})
set n += props
with r, t, epoch, n as y

with *, {typ:'c_yyyymm', name:apoc.date.format(epoch, 's','yyyyMM')} as props
merge (n:c_yyyymm {identifier:trim(tolower(props.name))})
set n += props
with r, t, epoch, y, n as ym
merge(y)-[e:has_month]->(ym)

with *, {typ:'c_yyyymmdd', name:apoc.date.format(epoch, 's','yyyyMMdd')} as props
merge (n:c_yyyymmdd {identifier:trim(tolower(props.name))})
set n += props
with r, t, epoch, y, ym, n as ymd
merge(ym)-[e:has_day]->(ymd)

with *, {typ:'c_yyyymmdd_hh', name:apoc.date.format(epoch, 's','yyyyMMdd_hh')} as props
merge (n:c_yyyymmdd_hh {identifier:trim(tolower(props.name))})
set n += props
with r, t, epoch, y, ym, ymd, n as ymd_h
merge(ymd)-[e:has_hour]->(ymd_h)

with *, {typ:'c_yyyymmdd_hhmm', name:apoc.date.format(epoch, 's','yyyyMMdd_hhmm')} as props
merge (n:c_yyyymmdd_hhmm {identifier:trim(tolower(props.name))})
set n += props
with r, t, epoch, y, ym, ymd, ymd_h, n as ymd_hm
merge(ymd_h)-[e:has_minute]->(ymd_hm)

with *, {typ:'c_yyyymmdd_hhmmss', name:apoc.date.format(epoch, 's','yyyyMMdd_hhmmss')} as props
merge (n:c_yyyymmdd_hhmmss {identifier:trim(tolower(props.name))})
set n += props
with r, t, epoch, y, ym, ymd, ymd_h, ymd_hm, n as ymd_hms
merge(ymd_hm)-[e:has_second]->(ymd_hms)
merge (r)-[ee:time_added]->(ymd_hms) 

return count(ymd_hms)
