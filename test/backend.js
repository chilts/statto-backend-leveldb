// --------------------------------------------------------------------------------------------------------------------
//
// statto-backend-leveldb/test/backend.js
// 
// Copyright 2015 Tynio Ltd.
//
// --------------------------------------------------------------------------------------------------------------------

// npm
var test = require('tape')
var stattoBackendTest = require('statto-backend/test/backend.js')
var levelup = require('levelup')

// local
var stattoBackendLevelDB = require('../')

// --------------------------------------------------------------------------------------------------------------------

// create a backend
var db = levelup('test-db', { valueEncoding : 'json' })
var backend = stattoBackendLevelDB(db)

// now run the tests
stattoBackendTest(backend, test)

// --------------------------------------------------------------------------------------------------------------------
