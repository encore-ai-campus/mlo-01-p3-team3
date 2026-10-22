// Models commands only; never loads a MongoDB driver or opens a network connection.
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const [scenario,path]=process.argv.slice(2);
const spec={_id:'rs-products',members:[0,1,2].map(i=>({_id:i,host:`mongo0${i+1}.pj3.internal:27017`,votes:1,priority:1}))};
const secret={username:'fixture-admin',password:'fixture-password-not-a-real-credential',server_version:'9.0.4'};
let authenticated=!['init','admin'].includes(scenario),creates=0,connections=0;
const phase=scenario.includes('init')?'init':scenario.includes('admin')?'admin':'verify';
const outputs=[];
const admin={
  auth(user,password){assert.equal(password,secret.password);if(!authenticated||scenario==='wrong_auth'){throw Object.assign(new Error('auth'),{code:18});}return {ok:1};},
  createUser(user,wc){assert.deepEqual(wc,{w:'majority',j:true,wtimeout:30000});assert.equal(user.roles[0].role,'root');creates++;authenticated=true;},
  runCommand(command){
    if(command.hello)return {setName:'rs-products',isWritablePrimary:true};
    if(command.replSetInitiate){if(scenario==='resume_init')throw Object.assign(new Error('already'),{code:23});return {ok:1};}
    if(command.replSetGetConfig){const copy=JSON.parse(JSON.stringify(spec));if(scenario==='wrong_members')copy.members.pop();if(scenario==='arbiter')copy.members[2].arbiterOnly=true;return {ok:1,config:copy};}
    if(command.replSetGetStatus)return {ok:1,set:'rs-products',members:spec.members.map((m,i)=>({name:m.host,health:scenario==='unhealthy'?0:1,state:i===0?1:2,stateStr:i===0?'PRIMARY':'SECONDARY'}))};
    if(command.buildInfo)return {version:scenario==='wrong_version'?'8.3.7':'9.0.4'};
    throw Error('unexpected command');
  }
};
function Mongo(){connections++;return {getDB(){return admin;},close(){}};}
const context={process:{env:{TEAM3_MONGO_BUNDLE:'/fixture',TEAM3_MONGO_PHASE:phase}},
 require(name){assert.equal(name,'fs');return {readFileSync(p){return JSON.stringify(p.endsWith('/cluster.json')?spec:secret);}};},
 db:{getSiblingDB(){return admin;}},Mongo,print(value){outputs.push(value);},sleep(){}};
let error=null;
try{vm.runInNewContext(fs.readFileSync(path,'utf8'),context);}catch(e){error=e;}
const mustFail=['wrong_members','arbiter','wrong_version','unhealthy','wrong_auth'].includes(scenario);
assert.equal(Boolean(error),mustFail,error?.stack);
if(scenario==='admin')assert.equal(creates,1);
if(scenario==='resume_admin')assert.equal(creates,0);
if(scenario==='verify')assert.equal(connections,3);
assert(!outputs.join('\n').includes(secret.password));
console.log(JSON.stringify({passed:true,scenario,applicationDataWrites:0,networkConnections:0}));
