// Root-owned execution via node.sh. Never print credentials or put them on argv.
const fs = require('fs');
const dir = process.env.TEAM3_MONGO_BUNDLE;
const phase = process.env.TEAM3_MONGO_PHASE;
const config = JSON.parse(fs.readFileSync(dir + '/cluster.json', 'utf8'));
const credentials = JSON.parse(fs.readFileSync(dir + '/admin.json', 'utf8'));
if (!/^[A-Za-z][A-Za-z0-9_-]{0,62}$/.test(config._id) || !Array.isArray(config.members) ||
    config.members.length < 1 || config.members.length > 7 ||
    new Set(config.members.map(m => m._id)).size !== config.members.length ||
    new Set(config.members.map(m => m.host)).size !== config.members.length ||
    config.members.some(m => m.arbiterOnly || m.hidden || m.votes !== 1 || m.priority !== 1)) {
  throw new Error('INVALID_VOTING_DATA_MEMBER_CONFIG');
}
const admin = db.getSiblingDB('admin');
function authenticate(target) {
  const result = target.auth(credentials.username, credentials.password);
  if (!(result === 1 || result?.ok === 1)) throw new Error('ADMIN_AUTHENTICATION_FAILED');
}
function assertConfig() {
  const response = admin.runCommand({replSetGetConfig: 1});
  if (response.ok !== 1) throw new Error('REPLICA_CONFIG_UNCONFIRMED');
  const actual = response.config;
  const members = actual.members;
  if (actual._id !== config._id || members.length !== config.members.length || actual.writeConcernMajorityJournalDefault === false ||
      members.some(m => m.arbiterOnly || m.hidden || (m.votes ?? 1) !== 1 || (m.priority ?? 1) !== 1) ||
      JSON.stringify(members.map(m => [m._id, m.host]).sort()) !==
      JSON.stringify(config.members.map(m => [m._id, m.host]).sort())) {
    throw new Error('REPLICA_CONFIG_CONFLICT_NO_FORCE_RECONFIG');
  }
}
if (phase === 'init') {
  try { authenticate(admin); } catch (_) { /* fresh localhost exception is intentional */ }
  let result;
  try { result = admin.runCommand({replSetInitiate: config}); }
  catch (error) {
    if (error.code !== 23) throw error;
    result = {ok: 0, code: 23};
  }
  if (result.ok !== 1 && result.code !== 23) throw new Error('REPLICA_INIT_FAILED_' + result.code);
  if (result.code === 23) {
    // A resumed initialized deployment must authenticate before it is accepted.
    // Before first user creation, defer authenticated config verification to admin phase.
    print(JSON.stringify({phase: 'init', status: 'ALREADY_INITIALIZED_VERIFY_AFTER_AUTH'}));
  } else print(JSON.stringify({phase: 'init', status: 'INITIATED'}));
} else if (phase === 'admin') {
  const hello = admin.runCommand({hello: 1});
  if (hello.setName !== config._id || !hello.isWritablePrimary) throw new Error('PRIMARY_REQUIRED');
  let authenticated = false;
  try { authenticate(admin); authenticated = true; } catch (_) {}
  if (!authenticated) {
    admin.createUser({user: credentials.username, pwd: credentials.password,
      roles: [{role: 'root', db: 'admin'}]}, {w: 'majority', j: true, wtimeout: 30000});
    authenticate(admin);
  }
  assertConfig();
  print(JSON.stringify({phase: 'admin', status: 'ADMIN_CONFIRMED', passwordPrinted: false}));
} else if (phase === 'verify') {
  authenticate(admin);
  assertConfig();
  let status;
  for (let attempt = 0; attempt < 60; attempt++) {
    status = admin.runCommand({replSetGetStatus: 1});
    if (status.ok === 1 && status.members.length === config.members.length &&
        status.members.every(m => m.health === 1 && [1, 2].includes(m.state)) &&
        status.members.filter(m => m.state === 1).length === 1) break;
    sleep(2000);
  }
  if (!status || status.ok !== 1 || status.set !== config._id || status.members.length !== config.members.length ||
      status.members.some(m => m.health !== 1 || ![1, 2].includes(m.state)) ||
      status.members.filter(m => m.state === 1).length !== 1) throw new Error('VOTING_DATA_MEMBERS_NOT_READY');
  const versions = [];
  for (const member of config.members) {
    const connection = new Mongo('mongodb://' + member.host +
      '/?directConnection=true&tls=true&tlsCAFile=%2Fetc%2Fmongodb%2Fpki%2Fca.crt&serverSelectionTimeoutMS=10000');
    const target = connection.getDB('admin');
    authenticate(target);
    const build = target.runCommand({buildInfo: 1});
    if (build.version !== credentials.server_version) throw new Error('MEMBER_VERSION_CONFLICT');
    versions.push({host: member.host, version: build.version});
    if (typeof connection.close === 'function') connection.close();
  }
  print(JSON.stringify({phase: 'verify', status: 'REPLICA_SET_READY', setName: status.set,
    members: status.members.map(m => ({name: m.name, health: m.health, state: m.stateStr})),
    versions, applicationDataWritten: false, failoverTested: false}));
} else throw new Error('UNKNOWN_BOOTSTRAP_PHASE');
