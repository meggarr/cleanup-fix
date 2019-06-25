#!/usr/bin/env node
const DEBUG = true;

const program = require('commander');
const req = require('sync-request');
const Mongo = require('mongodb').MongoClient;
const assert = require('assert');
const Rx = require('rxjs/Rx');
const Observable = require('rxjs/Observable').Observable;

program.version('0.2.0', '-v, --version')
  .option('-m, --mongo-url <url>', 'MongoDB connection URL')
  .option('-d, --db-name [value]', 'Database name', 'sxa')
  .option('-e, --delete', 'Delete subscribers in CC+')
  .option('-a, --acs-url [value]', 'ACS URL, e.g. http://gcs:8081', 'none')
  .option('-o, --org-id <id>', 'Organization ID')
  .option('-t, --last-date <value>', 'Using rule that the subscriber was created (ISO 8601 format) before the last date, e.g. 2018-06-01T00:00:00Z')
  .option('-p, --pattern <pattern>', 'Using rule that the valid or invalid pattern of customId in subscriber, e.g. .*:.*')
  .option('-v, --valid', 'Using rule that the pattern is for valid customId or not', true)
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

console.log('Using MongoDB - ' + program.mongoUrl); 
console.log('Database name - ' + program.dbName); 
console.log('Organization ID - ' + program.orgId);
console.log('Delete subscriber - ' + program.delete);
console.log('ACS URL - ' + program.acsUrl);
console.log('Rule: Subscriber created before the last date - ' + program.lastDate);
console.log('Rule: Subscriber customId is not in pattern - ' + program.pattern);
console.log('Rule: Subscriber pattern is for valid customId - ' + program.valid);
console.log('Rule: Subscriber has no device - ' + program.nonDevice);
console.log('Rule: Subscriber has no provisioning record - ' + program.nonPr);
console.log('');

// process.exit(1);
const doDelete = function (sub) {
  console.log(JSON.stringify(sub));
  if (!program.delete) return;
  if (typeof program.acsUrl === 'undefined') {
    console.log('Missing ACS URL')
    return;
  }

  let _url = program.acsUrl + '/cc/subscriber/' + sub._id;
  let resp = req('DELETE', _url, {
    headers: { 'appid': 'Cleanup' }
  });
  if (resp.statusCode == 200) {
    console.info('Deleted ' + sub._id);
  }
}

const tryDelete = function (sub, prCol) {
  return Observable.create(observer => {
    if (program.nonPr) {
      let query = {
        subscriberId: sub._id
      //, deviceId: { '$exists' : true, '$ne' : ''}
      }
      let findRx = Observable.bindNodeCallback((q, cb) => prCol.find(q).toArray(cb));
      findRx(query).subscribe(prs => {
        if (prs.length == 0) {
          if (program.debug) console.log('Subscriber has no provisioning record, ' + sub._id)
          observer.next(sub);
        } else {
          if (program.debug) console.log('Subscriber has provisioning record, ' + sub._id)
        }
        observer.complete();
      });
    } else {
      observer.next(sub);
      observer.complete();
    }
  });
}

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

    Observable.create(observer => {
      observer.count = subscribers.length;
      Observable.from(subscribers).subscribe({
        next(s) {
          if (program.debug) console.log('Subscriber, ' + s._id);
          let sub = {
            _id: s._id,
            name: s.name,
            customId: s.customId,
            createTime: s.createTime
          };

          let delObserver = {
            sub: null,
            next(s) { this.sub = s; },
            complete() {
              if (this.sub != null) {
                observer.next(this.sub)
              }
              observer.count--;
              if (program.debug) console.log('Subscribers left: ' + observer.count);
              if (observer.count == 0) {
                observer.complete();
              }
            }
          };
          tryDelete(sub, prCol).subscribe(delObserver);
        },
        complete() {
          if (program.debug) {
            console.log('Scanned ' + subscribers.length + ' subscribers');
            console.log('----------------------------------------------');
          }
        }
      });
    }).subscribe({
      sum: 0,
      next(sub) {
        this.sum++;
        doDelete(sub);
      },
      complete() {
        console.log('--------------------------------------------------');
        console.log(this.sum + ' subscribers processed');
        quit(0);
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

