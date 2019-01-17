#!/usr/bin/env node

var fs       = require('fs')
var path     = require('path')
var fork     = require('child_process').fork

var through  = require('through')
var csv      = require('fast-csv')
var tmp      = require('tmp')
var duplexer = require('duplexer')
var concat   = require('concat-stream')

module.exports = function (options, transform) {

  var read = through()
  var duplex

  var filename = tmp.tmpNameSync()

  var forkArgs = []

  if (options) {
    options.sheet && forkArgs.push('--sheet') && forkArgs.push(options.sheet) && delete options.sheet
    options.sheetIndex && forkArgs.push('--sheet-index') && forkArgs.push(options.sheetIndex) && delete options.sheetIndex
  }

  forkArgs.push(filename)

  var write = fs.createWriteStream(filename)
    .on('close', function () {
      var child = fork(require.resolve('j/bin/j.njs'), forkArgs, {silent: true})
      child.stdout
        .pipe(transform ? csv(options).transform(transform) : csv(options))
        .pipe(through(function (data) {
          var _data = {}
          for(var k in data) {
            var value = data[k].trim()
            _data[k.trim()] = (isNaN(value) || value === '') ? value : +value
          }
          this.queue(_data)
        }))
        .pipe(read)
      child.on('exit', function(code, sig) {
        if(code === null || code !== 0) {
          child.stderr.pipe(concat(function(errstr) {
            duplex.emit('error', new Error(errstr))
          }))
        }
      })
    })

  return (duplex = duplexer(write, read))

}


if(!module.parent) {
  var JSONStream = require('JSONStream')
  var args = require('minimist')(process.argv.slice(2))
  process.stdin
    .pipe(module.exports())
    .pipe(args.lines || args.newlines
      ? JSONStream.stringify('', '\n', '\n', 0)
      : JSONStream.stringify()
    )
    .pipe(process.stdout)
}
