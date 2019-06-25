#!/usr/bin/env node
const DEBUG = true;

const program = require('commander');
const request = require('request');
const Mongo = require('mongodb').MongoClient;
const assert = require('assert');
const Rx = require('rxjs/Rx');
const Observable = require('rxjs/Observable').Observable;

program.version('0.3.0', '-v, --version')
  .option('-m, --mongo-url <url>', 'MongoDB connection URL')
  .option('-d, --db-name [value]', 'Database name', 'sxa')
  .option('-e, --delete', 'Delete subscribers in CC+')
  .option('-a, --acs-url [value]', 'ACS URL, e.g. http://gcs:8081', 'none')
  .option('-o, --org-id <id>', 'Organization ID')
  .option('-t, --last-date <value>', 'Using rule that the subscriber was created (ISO 8601 format) before the last date, e.g. 2018-06-01T00:00:00Z')
  .option('-p, --pattern <pattern>', 'Using rule that the valid or invalid pattern of customId in subscriber, e.g. .*:.*')
  .option('-l, --valid', 'Using rule that the pattern is for valid customId or not', true)
  .option('-n, --non-device', 'Using rule that the subscriber has no device')
  .option('-r, --non-pr', 'Using rule that the subscriber has no provisioning record')
  .option('--debug', 'Print debug logs')
  .parse(process.argv);

if (typeof program.mongoUrl === 'undefined') {
  console.error('Missing Mongo URL');
  process.exit(1);
}
if (typeof program.orgId === 'undefined') {
  console.error('Missing Organization ID');
  process.exit(1);
}
if (typeof program.acsUrl === 'undefined') {
  console.error('Missing ACS URL')
  process.exit(1);
}

console.log('Using MongoDB - ' + program.mongoUrl); 
console.log('Database name - ' + program.dbName); 
console.log('Organization ID - ' + program.orgId);
console.log('Delete subscriber - ' + program.delete);
console.log('ACS URL - ' + program.acsUrl);
console.log('Rule: Subscriber created before the last date - ' + program.lastDate);
console.log('Rule: Subscriber customId is in pattern - ' + program.pattern);
console.log('Rule: Subscriber pattern is for valid customId - ' + program.valid);
console.log('Rule: Subscriber has no device - ' + program.nonDevice);
console.log('Rule: Subscriber has no provisioning record - ' + program.nonPr);
console.log('');

// process.exit(1);

const MongoConnectRx = Observable.bindNodeCallback(Mongo.connect);

MongoConnectRx(program.mongoUrl).subscribe(client => {
  const db = client.db("sxa");
  const subCol = db.collection('sxa-subscribers');
  const prCol = db.collection('sxacc-provisioning-records');
  assert.ok(subCol);
  assert.ok(prCol);

  let quit = function (c) { process.exit(c); client.close(); }

  let query = {
    orgId: program.orgId
  }
  if (program.lastDate != null) {
    query['createTime'] = { '$lt': new Date(program.lastDate) }
  }
  if (program.pattern != null) {
    if (program.valid)
      query['customId'] = new RegExp('^((?!' + program.pattern + ').)*$')
    else
      query['customId'] = new RegExp('^' + program.pattern + '$')
  }
  if (program.nonDevice) {
    query['$or'] = [
      {'locations.devices': { '$exists': false}},
      {'locations.devices': { '$size': 0 } }
    ]
  }
  if (true || program.debug) {
    let replacer = (k, v) => {
      if (k == 'customId') return v.toString();
      return v;
    };
    console.info('Query sxa-subscibers: ' + JSON.stringify(query, replacer, 2));
  }

  let findRx = Observable.bindNodeCallback((q, cb) => subCol.find(q).toArray(cb));
  findRx(query).subscribe(subscribers => {
    let _total = 0;
    let _deleted = 0;
    Observable.from(subscribers)
      .map(s => ({ _id: s._id, name: s.name, customId: s.customId, createTime: s.createTime }))
      .flatMap(s => {
        // Find PR
        let prObservable;
        if (program.nonPr) {
          let query = {
            subscriberId: s._id
          //, deviceId: { '$exists' : true, '$ne' : ''}
          }
          let findPrRx = Observable.bindNodeCallback((q, cb) => prCol.find(q).toArray(cb));
          prObservable = findPrRx(query);
        } else {
          prObservable = Observable.of([]);
        }
        return Observable.zip(Observable.of(s), prObservable, (s, p) => ({ sub: s, prs: p }));
      })
      .flatMap(s => {
        // Do delete
        let sub = s.sub;
        let prs = s.prs;

        console.log(JSON.stringify(sub));
        if (!program.delete) {
          // no delete flag
          return Observable.of({ sub: sub });
        }
        if (program.nonPr) {
          if (program.debug) console.log('Subscriber ' + sub._id + ' PR number: ' + prs.length)
          if (prs.length > 0) {
            // has pr
	    return Observable.of({ sub: sub });
          }
        }
	//TEST: return Observable.of({ sub: sub });

        let _url = program.acsUrl + '/cc/subscriber/' + sub._id;
        let _headers = { 'appid': 'Cleanup' };
        let _opt = { url: _url, headers: _headers };
        let deleteRx = Observable.bindNodeCallback(request.delete);

	if (program.debug) console.log('Subscriber delete: ' + JSON.stringify(_opt))
	//TEST: return Observable.of({ sub: sub, resp: { statusCode: 200 } });
        return deleteRx(_opt).flatMap(r => {
          return Observable.of({ sub: sub, resp: r[0] });
        });
        //return Observable.zip(Observable.of(sub), deleteRx(_opt), (s, r) => ({ sub: s, resp: r[0] }));
      })
      .concat(Observable.of({ _end: true }))
      .subscribe(r => {
        if (r._end) {
          console.log("--------------------------------------------------");
          console.log(_total + " subscriber(s) processed, " + _deleted + " deleted");
          quit(0);
        }
        //if (program.debug) console.log('Result > ' + JSON.stringify(r));

        // Result
        let sub = r.sub;
        let resp = r.resp;
        _total++;

        if (resp != null && resp.statusCode == 200) {
          console.info('Deleted ' + sub._id);
          _deleted++;
        }
      });
  },
  err => {
    console.error('Failed to query, ' + err);
    quit(2)
  });
},
err => {
  console.error('Failed to connect MongoDB, ' + err);
  process.exit(1);
});

