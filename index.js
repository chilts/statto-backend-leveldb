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

function StattoBackendLevelDB(db, opts) {
  // default the opts to nothing
  this.opts = opts || {}

  // store the db and set up the queue
  this.db = db
  this.queue = async.queue(this.store.bind(this), 1)

  // set some default options
  this.opts.denormalise = this.opts.denormalise || false
}

StattoBackendLevelDB.prototype.stats = function stats(stats) {
  var self = this

  // console.log('=== %s ===', stats.ts)
  // console.log(JSON.stringify(stats, null, '  '))

  // Let's use async to store these things up, just in case it takes longer to process than
  // before the next one comes in ... in which case, you're in trouble!!!
  this.queue.push(stats)
}

StattoBackendLevelDB.prototype.store = function store(stats, done) {
  var self = this

  // console.log('Processing:', JSON.stringify(stats, null, '  '))

  // figure out the SHA1 of these stats (should be unique due to 'ts' and 'info.pid' and 'info.host'
  var str = JSON.stringify(stats)
  var hash = crypto.createHash('sha1').update(str).digest('hex')

  // Store this file (check if something already there, but not need for locking since if we
  // overwrite it, it'll have exactly the same content anyway).
  var fileKey = makeFileKey(stats.ts, hash)
  // console.log('Storing under fileKey=%s', fileKey)
  self.db.get(fileKey, function(err, val) {
    if (err) {
      if ( err.type !== 'NotFoundError' ) {
        return done(err)
      }
    }
    self.db.put(fileKey, stats, function(err) {
      if (err) return done(err)

      // now process the new stats
      self.processFilesIntoStats(stats.ts, function(err, stats) {
        if ( err ) {
          // console.log('error processing stats:', err)
          return done(err)
        }
        // console.log('Stats processed correctly')
        done()
      })
    })
  })
}

StattoBackendLevelDB.prototype.getStats = function get(date, callback) {
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

  // console.log('Getting all of the stats for ' + ts)

  // start streaming all the files for this timestamp
  var key = 's' + FIELD_SEP + ts
  // console.log('Getting all of the stats for ' + key)

  self.db.get(key, function(err, value) {
    if (err) return callback(err)
    callback(null, value)
  })
}

StattoBackendLevelDB.prototype.getFilesAndMerge = function get(date, callback) {
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
  var begin = makeFileKey(ts)
  var end   = makeFileKey(ts) + FIELD_END

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

  console.log('here', from, to)

  // IGNORE INTERVAL FOR NOW, JUST RETURN THE TIMESTAMPS AS WE HAVE THEM CURRENTLY STORED

  if ( self.opts.denormalise ) {
    // grab each denormalised counter
    throw new Error('Not yet implemented')
  }
  else {
    // just grab each complete set of stats and extract what we need
    var periods = []

    // start streaming all the stats for this timestamp
    var start = makeStatKey(from.toISOString())
    var end   = makeStatKey(to.toISOString())

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
    var start = makeStatKey(from.toISOString())
    var end   = makeStatKey(to.toISOString())

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
    var start = makeStatKey(from.toISOString())
    var end   = makeStatKey(to.toISOString())

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
    var key = makeStatKey(ts)
    self.db.put(key, stats, function(err) {
      if (err) return callback(err)

      // here, we could denormalise the stats to leveldb ... but perhaps later
      callback(null, stats)
    })
  })
}


// --------------------------------------------------------------------------------------------------------------------
// utility functions

function makeFileKey(ts, hash) {
  var key = 'f' + FIELD_SEP + ts
  if ( hash ) {
    key += FIELD_SEP + hash
  }
  return key
}

function makeStatKey(ts) {
  return 'm' + FIELD_SEP + ts
}

// --------------------------------------------------------------------------------------------------------------------

module.exports = function backend(db, opts) {
  if ( !db ) {
    throw new Error('Required: db in statto-backend-leveldb')
  }

  return new StattoBackendLevelDB(db)
}

// --------------------------------------------------------------------------------------------------------------------
