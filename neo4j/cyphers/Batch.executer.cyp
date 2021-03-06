with "https://raw.githubusercontent.com/wirsel/neo4j_knowledge_graph/master/neo4j/cyphers/S_json/S_json_verification.batch.txt" as file
With file, datetime() as dt
MERGE( lg: C_log { identifier: apoc.convert.toString(dt)})
Set lg.name=lg.identifier
Set lg.type= "C_log"
Set lg.dt= dt
Set lg.laststep=0
Set lg.status = "actual"
set lg.batchfile= file

Set lg.laststep=lg.laststep+1 with * CALL apoc.create.setProperty( [lg], 'step'+apoc.text.lpad(tostring(lg.laststep) ,3,"0"), "C_log successfully created") YIELD node with lg

//A 2
OPTIONAL MATCH(lh:C_log {status:"actual"} ) 
Where not lh.identifier=lg.identifier
detach delete lh;

MATCH(lg:C_log {status:"actual"} ) 
with lg, lg.batchfile as file
call apoc.load.json(file,"",{}) yield value
with lg, value.batch as batch
with lg, batch, filter( x in batch.steps where x.modus=1 ) as activSteps
with *, trim(batch.baseurl) as burl
with *, case when right(burl,1)="/" THEN burl ELSE burl+"/" END as baseurl
with lg, batch, activSteps, [ x in activSteps | replace(replace(baseurl+x.ref,"//","/"),":/","://")] as cypUrls

unwind range(0, size(cypUrls)-1) as x
with lg, batch, activSteps, x, cypUrls[x] as url
with lg, batch, activSteps, x, url, activSteps[x] as step
call apoc.load.json(url,"",{}) yield value as cypJso
with lg, batch, step, url, x, cypJso, [y in cypJso.in | "<<"+y+">>"] as inkeys
with lg, batch, step, url, x, cypJso, inkeys
with lg, batch, step, url, cypJso, inkeys, batch.in[step.in-1] as kvMap
with lg, batch, step, url, cypJso, inkeys, kvMap
with lg, batch, step, url, cypJso, inkeys, kvMap, apoc.text.join( [x in cypJso.cypher | x.line] , " ") as cypParam
with lg, batch, step, url, cypJso, inkeys, cypParam, 
reduce(s=cypParam, x in range(0, size(inkeys)-1) | replace(s,inkeys[x], kvMap[cypJso.in[x]])) as cypx
with *, replace(cypx, "§", "$") as cyp



set lg.cyp = cyp
with *
call apoc.periodic.commit(cyp, {lg:lg}) yield updates, executions, runtime, batches, failedBatches, batchErrors, failedCommits, commitErrors, wasTerminated 

Set lg.return = apoc.convert.toString({update: apoc.convert.toString(updates), executions: apoc.convert.toString(executions), runtime: apoc.convert.toString(runtime), batches: apoc.convert.toString(batches), failedBatches: apoc.convert.toString(failedBatches), batchErrors: apoc.convert.toString(batchErrors), failedCommits: apoc.convert.toString(failedCommits), commitErrors: apoc.convert.toString(commitErrors), wasTerminated: apoc.convert.toString(wasTerminated)}) 

return cyp
