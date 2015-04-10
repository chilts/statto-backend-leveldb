// --------------------------------------------------------------------------------------------------------------------
//
// statto-backend-leveldb/index.js
// 
// Copyright 2015 Tynio Ltd.
//
// --------------------------------------------------------------------------------------------------------------------

// core
var util = require('util')

// npm
var stattoMerge = require('statto-merge')
var stattoProcess = require('statto-process')
var stattoBackend = require('statto-backend')

// --------------------------------------------------------------------------------------------------------------------
// module level

function noop(){}
var FIELD_SEP = '!'
var FIELD_END = '~'

// --------------------------------------------------------------------------------------------------------------------
// constructor

function StattoBackendLevelDB(db, opts) {
  // default the opts to nothing
  this.opts = opts || {}

  // store the db and set up the queue
  this.db = db

  // set some default options
  this.opts.denormalise = this.opts.denormalise || false
}
util.inherits(StattoBackendLevelDB, stattoBackend.StattoBackendAbstract)

// --------------------------------------------------------------------------------------------------------------------
// methods

StattoBackendLevelDB.prototype.addRaw = function addRaw(raw, callback) {
  var self = this
  callback = callback || noop

  // store these stats straight into LevelDB
  var hash = self._getHash(raw)
  var ts = raw.ts

  // Store these raw stats (check if something already there, but no need for locking since if we
  // overwrite it, it'll have exactly the same content anyway).
  var rawKey = makeRawKey(raw.ts, hash)

  self.db.get(rawKey, function(err, val) {
    if (err) {
      if ( err.type !== 'NotFoundError' ) {
        callback(err)
        return self.emit('error', err)
      }
      // This was a 'NotFoundError', so just carry on
    }
    self.db.put(rawKey, raw, function(err) {
      if (err) return self.emit('error', err)
      self.emit('stored')
      callback()
    })
  })
}

StattoBackendLevelDB.prototype.getRaws = function getStats(date, callback) {
  var self = this

  date = self._datify(date)
  if ( !date ) {
    return process.nextTick(function() {
      callback(new Error('Unknown date type : ' + typeof date))
    })
  }
  var ts = date.toISOString()

  var raws = []

  var begin = makeRawKey(ts)
  var end   = makeRawKey(ts) + FIELD_END
  self.db
    .createReadStream({ gt : begin, lt : end })
    .on('data', function (data) {
      raws.push(data.value)
    })
    .on('error', function (err) {
      callback(err)
    })
    .on('close', function () {
      // nothing
    })
    .on('end', function () {
      callback(null, raws)
    })
  ;
}

StattoBackendLevelDB.prototype.setStats = function setStats(stats, callback) {
  var self = this

  // Store this file (check if something already there, but no need for locking since if we
  // overwrite it, it'll have exactly the same content anyway).
  var statsKey = makeStatsKey(stats.ts)

  self.db.get(statsKey, function(err, val) {
    if (err) {
      if ( err.type !== 'NotFoundError' ) {
        self.error(err)
        return callback(err)
      }
    }
    self.db.put(statsKey, stats, callback)
  })
}

StattoBackendLevelDB.prototype.getStats = function get(date, callback) {
  var self = this
  callback = callback || noop

  date = self._datify(date)
  if ( !date ) {
    return process.nextTick(function() {
      callback(new Error('Unknown date type : ' + typeof date))
    })
  }
  var ts = date.toISOString()

  // make the stats key
  var statsKey = makeStatsKey(ts)

  self.db.get(statsKey, function(err, val) {
    if (err) {
      if ( err.type !== 'NotFoundError' ) {
        return callback()
      }
      return callback(err)
    }
    callback(null, val)
  })
}

StattoBackendLevelDB.prototype.createStatsReadStream = function createStatsReadStream(from, to, callback) {
  // * from - greater than or equal to (always included)
  // * to - less than (therefore never included)
  var self = this

  from = self._datify(from)
  to   = self._datify(to)
  // if ( !from ) {
  //   return process.nextTick(function() {
  //     callback(new Error("Unknown 'from' type : " + typeof from))
  //   })
  // }
  // if ( !to ) {
  //   return process.nextTick(function() {
  //     callback(new Error("Unknown 'to' type : " + typeof to))
  //   })
  // }
  var ts1 = from.toISOString()
  var ts2 = to.toISOString()

  // start streaming all the files for this timestamp
  var begin = makeStatsKey(ts1)
  var end   = makeStatsKey(ts2)

  return self.db.createValueStream({ gte : begin, lt : end })
}




























StattoBackendLevelDB.prototype.getFilesAndMerge = function getFilesAndMerge(date, callback) {
  var self = this

  var ts
  if ( typeof date === 'string' ) {
    ts = date
    date = new Date(date)
  }
  else if ( date instanceof Date ) {
    ts = date.toISOString()
  }
  else {
    return process.nextTick(function() {
      callback(new Error('Unknown date type : ' + typeof date))
    })
  }

  // console.log('Getting all of the files for ' + ts)

  // start streaming all the files for this timestamp
  var begin = makeRawKey(ts)
  var end   = makeRawKey(ts) + FIELD_END

  var stats

  var count = 0
  self.db
    .createReadStream({ gt : begin, lt : end })
    .on('data', function (data) {
      count++
      // console.log('Got some data, key=' + data.key)
      // console.log('             count=' + count)
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
      // console.log('Oh my!', err)
      callback(err)
    })
    .on('close', function () {
      // console.log('Stream Closed')
    })
    .on('end', function () {
      // console.log('Stream Ended')
      if ( !stats ) {
        return callback() // no problem, but no stats either
      }
      callback(null, stats)
    })
  ;
}

StattoBackendLevelDB.prototype.getCounterRange = function getCounterRange(name, from, to, interval, callback) {
  var self = this

  // IGNORE INTERVAL FOR NOW, JUST RETURN THE TIMESTAMPS AS WE HAVE THEM CURRENTLY STORED

  if ( self.opts.denormalise ) {
    // grab each denormalised counter
    throw new Error('Not yet implemented')
  }
  else {
    // just grab each complete set of stats and extract what we need
    var periods = []

    // start streaming all the stats for this timestamp
    var start = makeStatsKey(from.toISOString())
    var end   = makeStatsKey(to.toISOString())

    self.db
      .createReadStream({ gte : start, lt : end })
      .on('data', function (data) {
        console.log('data:', data)
        if ( data.value.counters[name] ) {
          periods.push({
            ts : data.value.ts,
            v  : data.value.counters[name],
          })
        }
        // else, don't add this to the array
      })
      .on('error', function (err) {
        callback(err)
      })
      .on('end', function () {
        callback(null, periods)
      })
  }
}

StattoBackendLevelDB.prototype.range = function(range) {
  var self = this
  return 
}

StattoBackendLevelDB.prototype.getTimerRange = function getTimerRange(name, from, to, interval, callback) {
  var self = this

  // IGNORE INTERVAL FOR NOW, JUST RETURN THE TIMESTAMPS AS WE HAVE THEM CURRENTLY STORED

  if ( self.opts.denormalise ) {
    // grab each denormalised counter
    throw new Error('Not yet implemented')
  }
  else {
    // just grab each complete set of stats and extract what we need
    var periods = []

    // start streaming all the stats for this timestamp
    var start = makeStatsKey(from.toISOString())
    var end   = makeStatsKey(to.toISOString())

    self.db
      .createReadStream({ gte : start, lt : end })
      .on('data', function (data) {
        if ( data.value.timers[name] ) {
          periods.push({
            ts : data.value.ts,
            v  : data.value.timers[name],
          })
        }
        // else, don't add this to the array
      })
      .on('error', function (err) {
        callback(err)
      })
      .on('end', function () {
        callback(null, periods)
      })
  }
}

StattoBackendLevelDB.prototype.getGaugeRange = function getGaugeRange(name, from, to, interval, callback) {
  var self = this

  // IGNORE INTERVAL FOR NOW, JUST RETURN THE TIMESTAMPS AS WE HAVE THEM CURRENTLY STORED

  if ( self.opts.denormalise ) {
    // grab each denormalised counter
    throw new Error('Not yet implemented')
  }
  else {
    // just grab each complete set of stats and extract what we need
    var periods = []

    // start streaming all the stats for this timestamp
    var start = makeStatsKey(from.toISOString())
    var end   = makeStatsKey(to.toISOString())

    self.db
      .createReadStream({ gte : start, lt : end })
      .on('data', function (data) {
        if ( data.value.gauges[name] ) {
          periods.push({
            ts : data.value.ts,
            v  : data.value.gauges[name],
          })
        }
        // else, don't add this to the array
      })
      .on('error', function (err) {
        callback(err)
      })
      .on('end', function () {
        callback(null, periods)
      })
  }
}

StattoBackendLevelDB.prototype.processFilesIntoStats = function processFilesIntoStats(date, callback) {
  var self = this

  var ts
  if ( typeof date === 'string' ) {
    ts = date
    date = new Date(date)
  }
  else if ( date instanceof Date ) {
    ts = date.toISOString()
  }
  else {
    return process.nextTick(function() {
      callback(new Error('Unknown date type : ' + typeof date))
    })
  }

  // console.log('=== Processing for ts=%s ===', ts)

  self.getFilesAndMerge(date, function(err, stats) {
    if (err) return callback(err)

    // got the stats
    // console.log('merged stats:', stats)

    // if there are no stats, don't do anything
    if ( !stats ) {
      // console.log('No stats for ' + ts)
      return callback()
    }

    // process the stats
    stats = stattoProcess(stats)

    // let's store these processed stats
    var key = makeStatsKey(ts)
    self.db.put(key, stats, function(err) {
      if (err) return callback(err)

      // here, we could denormalise the stats to leveldb ... but perhaps later
      callback(null, stats)
    })
  })
}

// --------------------------------------------------------------------------------------------------------------------
// utility functions

function makeRawKey(ts, hash) {
  var key = 'r' + FIELD_SEP + ts
  if ( hash ) {
    key += FIELD_SEP + hash
  }
  return key
}

function makeStatsKey(ts) {
  return 'm' + FIELD_SEP + ts
}

// --------------------------------------------------------------------------------------------------------------------

module.exports = function backend(db, opts) {
  if ( !db ) {
    throw new Error('Required: db in statto-backend-leveldb')
  }

  return new StattoBackendLevelDB(db, opts)
}

// --------------------------------------------------------------------------------------------------------------------
