// --------------------------------------------------------------------------------------------------------------------
//
// statto-backend-leveldb/index.js
// 
// Copyright 2015 Tynio Ltd.
//
// --------------------------------------------------------------------------------------------------------------------

// core
var crypto = require('crypto')

// npm
var async = require('async')

// --------------------------------------------------------------------------------------------------------------------

var queue = async.queue()
var FIELD_SEP = '~'
var FIELD_END = '\xff'

function StattoBackendLevelDB(db) {
  this.db = db
  this.queue = async.queue(this.process.bind(this), 1)
}

StattoBackendLevelDB.prototype.stats = function stats(stats) {
  var self = this

  console.log('=== %s ===', stats.ts)
  console.log(JSON.stringify(stats, null, '  '))

  // Let's use async to store these things up, just in case it takes longer to process than
  // before the next one comes in ... in which case, you're in trouble!!!
  this.queue.push(stats)
}

StattoBackendLevelDB.prototype.process = function process(stats, done) {
  var self = this

  console.log('Processing:', JSON.stringify(stats, null, '  '))

  // figure out the SHA1 of these stats (should be unique due to 'ts' and 'info.pid' and 'info.host'
  var str = JSON.stringify(stats)
  var hash = crypto.createHash('sha1').update(str).digest('hex')

  // firstly, see if this file has already been started (ie. check the hash)
  var fileKeyBeg = makeFileKey(stats.ts, hash, 'beg')
  var fileKeyDat = makeFileKey(stats.ts, hash, 'dat')
  var fileKeyEnd = makeFileKey(stats.ts, hash, 'end')
  self.db.get(fileKeyBeg, function(err, val) {
    if (err) {
      if ( err.type !== 'NotFoundError' ) {
        return done(err)
      }
    }

    if (!err) {
      console.log('This file has already been processed')
      done()
    }

    // process this file
    console.log('Processing Stats ...')

    var ops = [
      { type : 'put', key : fileKeyBeg, value : (new Date()).toISOString() },
      { type : 'put', key : fileKeyDat, value : JSON.stringify(stats) },
    ]
    self.db.batch(ops, function(err) {
      if (err) return done(err)
      console.log('Set started on this file')

      // process the counters first
      async.parallel(
        [
          self.processCounters.bind(self, stats.ts, stats.counters),
          self.processTimers.bind(self, stats.ts, stats.timers),
          self.processGauges.bind(self, stats.ts, stats.gauges),
        ],
        function(err) {
          console.log('Finished Processing Stats')

          // let's record that this has finished
          self.db.put(fileKeyEnd, (new Date()).toISOString(), function(err) {
            if (err) return done(err)
            done()
          })
        }
      )
    })
  })
}

StattoBackendLevelDB.prototype.processCounters = function processCounters(ts, counters, done) {
  var self = this

  console.log('Processing counters ...')

  var keys = Object.keys(counters)
  async.each(
    keys,
    function(name, done) {
      var key = makeCounterKey(name, ts)
      self.db.get(key, function(err, val) {
        if (err) {
          if (err.type == 'NotFoundError') {
            return self.db.put(key, counters[name], done)
          }
          return done(err)
        }
        return self.db.put(key, val + counters[name], done)
      })
    },
    done
  )
}

StattoBackendLevelDB.prototype.processTimers = function processTimers(ts, timers, done) {
  console.log('Processing timers ...')
  console.log(timers)

  var keys = Object.keys(timers)

  process.nextTick(function() {
    console.log('Done timers')
    done()
  })
}

StattoBackendLevelDB.prototype.processGauges = function processGauges(ts, gauges, done) {
  var self = this

  console.log('Processing gauges ...')

  // If we have processed a file from a different machine already, then we don't care if a gauge gets overwritten with
  // this info or not. ie. we don't care which one is processed last in this period, since it really shouldn't affect
  // things. This is true of any collector sending a gauge to a daemon anyway, the last measurement wins.

  var keys = Object.keys(gauges)
  async.each(
    keys,
    function(name, done) {
      var key = makeGaugeKey(name, ts)
      self.db.put(key, gauges[name], done)
    },
    done
  )
}

// --------------------------------------------------------------------------------------------------------------------
// utility functions

function makeFileKey(ts, hash, type) {
  return 'f' + FIELD_SEP + ts + FIELD_SEP + hash + FIELD_SEP + type
}

function makeCounterKey(name, ts) {
  return 'c' + FIELD_SEP + name + FIELD_SEP + ts
}

function makeGaugeKey(name, ts) {
  return 'g' + FIELD_SEP + name + FIELD_SEP + ts
}

// --------------------------------------------------------------------------------------------------------------------

module.exports = function backend(db, opts) {
  if ( !db ) {
    throw new Error('Required: db in statto-backend-leveldb')
  }

  return new StattoBackendLevelDB(db)
}

// --------------------------------------------------------------------------------------------------------------------
