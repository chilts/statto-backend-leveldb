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
  self.db.get('file!' + stats.ts + '!' + hash + '!beg' , function(err, val) {
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
      { type : 'put', key : 'file!' + stats.ts + '!' + hash + '!beg', value : (new Date()).toISOString() },
      { type : 'put', key : 'file!' + stats.ts + '!' + hash + '!dat', value : JSON.stringify(stats) },
    ]
    self.db.batch(ops, function(err) {
      if (err) return done(err)
      console.log('Set started on this file')

      // process the counters first
      async.parallel(
        [
          self.processCounters.bind(self, stats.ts, stats.counters),
          self.processTimers.bind(self, stats.ts, stats.timers),
        ],
        function(err) {
          console.log('Finished Processing Stats')

          // let's record that this has finished
          var endKey = 'file!' + stats.ts + '!' + hash + '!end'
          self.db.put(endKey, (new Date()).toISOString(), function(err) {
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
      var key = 'c!' + name + '!' + ts
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
  process.nextTick(function() {
    console.log('Done timers')
    done()
  })
}

StattoBackendLevelDB.prototype.dump = function dump() {
  var self = this
}

// --------------------------------------------------------------------------------------------------------------------

module.exports = function backend(db, opts) {
  if ( !db ) {
    throw new Error('Required: db in statto-backend-leveldb')
  }

  return new StattoBackendLevelDB(db)
}

// --------------------------------------------------------------------------------------------------------------------
