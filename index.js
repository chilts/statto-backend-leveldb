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
var stattoMerge = require('statto-merge')
var stattoProcess = require('statto-process')

// --------------------------------------------------------------------------------------------------------------------

var queue = async.queue()
var FIELD_SEP = '!'
var FIELD_END = '~'

function StattoBackendLevelDB(db) {
  this.db = db
  this.queue = async.queue(this.store.bind(this), 1)
}

StattoBackendLevelDB.prototype.stats = function stats(stats) {
  var self = this

  console.log('=== %s ===', stats.ts)
  console.log(JSON.stringify(stats, null, '  '))

  // Let's use async to store these things up, just in case it takes longer to process than
  // before the next one comes in ... in which case, you're in trouble!!!
  this.queue.push(stats)
}

StattoBackendLevelDB.prototype.store = function store(stats, done) {
  var self = this

  console.log('Processing:', JSON.stringify(stats, null, '  '))

  // figure out the SHA1 of these stats (should be unique due to 'ts' and 'info.pid' and 'info.host'
  var str = JSON.stringify(stats)
  var hash = crypto.createHash('sha1').update(str).digest('hex')

  // Store this file (check if something already there, but not need for locking since if we
  // overwrite it, it'll have exactly the same content anyway).
  var fileKey = makeFileKey(stats.ts, hash)
  self.db.get(fileKey, function(err, val) {
    if (err) {
      if ( err.type !== 'NotFoundError' ) {
        return done(err)
      }
    }
    self.db.put(fileKey, stats, function(err) {
      if (err) return done(err)
      done()
    })
  })
}

StattoBackendLevelDB.prototype.get = function get(date, callback) {
  var self = this

  var ts = date.toISOString()

  console.log('Getting all of the stats for ' + ts)

  // start streaming all the files for this timestamp
  var begin = 'f' + FIELD_SEP + ts + FIELD_SEP
  var end   = 'f' + FIELD_SEP + ts + FIELD_SEP + FIELD_END
  console.log('begin=%s', begin)
  console.log('end=%s', end)

  var stats

  var count = 0
  self.db
    .createReadStream({ gt : begin, lt : end })
    .on('data', function (data) {
      count++
      console.log('Got some data, key=' + data.key)
      console.log('             count=' + count)
      // for the first set of data
      if ( count === 1 ) {
        // we don't need to do anything (just remember it)
        stats = data.value
      }
      else {
        // all subsequent data should be merged
        stats = stattoMerge({ merged : count }, stats, data.value)
      }
    })
    .on('error', function (err) {
      console.log('Oh my!', err)
      callback(err)
    })
    .on('close', function () {
      console.log('Stream Closed')
    })
    .on('end', function () {
      console.log('Stream Ended')
      if ( !stats ) {
        return callback() // no problem, but no stats either
      }
      callback(null, stats)
    })
  ;
}

StattoBackendLevelDB.prototype.process = function process(date, callback) {
  var self = this

  var ts = date.toISOString()
  console.log('Processing for ts=' + ts)

  self.get(date, function(err, stats) {
    if (err) return callback(err)

    // got the stats
    console.log('merged stats:', stats)

    // if there are no stats, don't do anything
    if ( !stats ) {
      console.log('No stats for ' + ts)
      return
    }

    // process the stats
    stats = stattoProcess(stats)

    // let's store these processed stats
    var key = 's' + FIELD_SEP + ts
    self.db.put(key, stats, function(err) {
      if (err) return callback(err)

      // here, we could denormalise the stats to leveldb ... but perhaps later
      callback(null, stats)
    })
  })
}

StattoBackendLevelDB.prototype.denormalise = function denormalise(stats, callback) {
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
  var self = this

  console.log('Processing timers ...')
  console.log(timers)

  var keys = Object.keys(timers)
  async.each(
    keys,
    function(name, done) {
      var key = makeTimerKey(name, ts)
      self.db.put(key, timers[name], done)
    },
    done
  )
}

StattoBackendLevelDB.prototype.processGauges = function processGauges(ts, gauges, done) {
  var self = this

  console.log('Processing gauges ...')
  console.log(gauges)

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

function makeFileKey(ts, hash) {
  return 'f' + FIELD_SEP + ts + FIELD_SEP + hash
}

function makeCounterKey(name, ts) {
  return 'c' + FIELD_SEP + name + FIELD_SEP + ts
}

function makeGaugeKey(name, ts) {
  return 'g' + FIELD_SEP + name + FIELD_SEP + ts
}

function makeTimerKey(name, ts) {
  return 't' + FIELD_SEP + name + FIELD_SEP + ts
}

// --------------------------------------------------------------------------------------------------------------------

module.exports = function backend(db, opts) {
  if ( !db ) {
    throw new Error('Required: db in statto-backend-leveldb')
  }

  return new StattoBackendLevelDB(db)
}

// --------------------------------------------------------------------------------------------------------------------
