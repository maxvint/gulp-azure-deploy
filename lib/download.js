'use strict';
var gutil = require('gulp-util');
var assign = require('object-assign');
var File = require('vinyl');
var ProgressBar = require('progress');
var azure = require('azure-storage');
var queue = require('queue');
var es = require('event-stream');
var delayed = require('delayed-stream');

var PLUGIN_NAME = 'gulp-azure-deploy';

function collectEntries(service, opts, callback) {
  var entries = [];
  function loop(token) {
    service.listBlobsSegmentedWithPrefix(opts.container, opts.prefix, token, {include: 'metadata'}, function (err, result) {
      if (err) {
        return callback(err);
      }

      entries.push.apply(entries, result.entries);

      if (result.continuationToken) {
        loop(result.continuationToken);
      } else {
        callback(null, entries);
      }
    });
  }
  loop(null);
}


module.exports = function(options) {
  options = assign({}, options);
  options.verbose = options.verbose || (process.argv.indexOf('--verbose') !== -1);

  if (options.account === undefined) {
    throw new gutil.PluginError(PLUGIN_NAME, '`account` required');
  }

  if (options.key === undefined) {
    throw new gutil.PluginError(PLUGIN_NAME, '`key` required');
  }

  if (options.container === undefined) {
    throw new gutil.PluginError(PLUGIN_NAME, '`container` required');
  }

  if (options.container.length < 3 || options.container.length > 63) {
    throw new gutil.PluginError(PLUGIN_NAME, 'Container name must be between 3 and 63 characters long');
  }

  var fileCount = 0;

  var blobService = azure.createBlobService(options.account, options.key, options.host);
  blobService = blobService.withFilter(new azure.LinearRetryPolicyFilter());

  return es.readable(function (count, callback) {
    var _self = this;

    collectEntries(blobService, options, function (err, entries) {
      var q = queue({ concurrency: 4, timeout: 1000 * 60 * 2 });
      if (!options.quiet) {
        var bar = new ProgressBar(options.format || 'Downloading [:bar] :percent :etas', { total: entries.length });
        bar.tick(0);
      }

      entries.forEach(function (entry) {
        if (options.buffer) {
          q.push(function (callback) {
            var buffer = new Buffer(Number(entry.properties['content-length']));
            var position = 0;
            var stream = service.createReadStream(options.container, entry.name);

            stream.on('error', callback);
            stream.on('data', function (chunk) {
              chunk.copy(buffer, position);
              position += chunk.length;
            });

            stream.on('end', function () {
              _self.emit('data', new File({
                cwd: '.',
                base: options.prefix ? options.prefix + '/' : '',
                path: entry.name,
                contents: buffer,
                stat: {
                  mode: Number(entry.metadata.fsmode)
                }
              }));
              callback();
            });
          });
        } else {
          q.push(function (callback) {
            var stream = blobService.createReadStream(options.container, entry.name);
            var delayedStream = delayed.create(stream, {
              maxDataSize: 1024 * 1024 * 5
            });

            delayedStream.on('error', callback);
            delayedStream.on('end', callback);

            _self.emit('data', new File({
              cwd: '.',
              base: options.prefix ? options.prefix + '/' : '',
              path: entry.name,
              contents: delayedStream
            }));
            
          });
        }
      });

      if (!options.quiet) {
        q.on('success', function () {
          bar.tick();
        });
      }

      q.on('error', function (err) {
        _self.emit('error', err);
      });
      q.start(function () {
        _self.emit('end');
      })
    });
  });
};