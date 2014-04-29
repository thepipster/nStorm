/**
 * @see https://github.com/ajlopez/SimpleMessages/blob/master/lib/simplemessages.js
 * @type {*}
 */

var Logger = require('arsenic-logger');

var net = require('net'),
    util = require('util'),
    os = require('./../ObjectStream.js');

function Server(fn)
{
    var server;

    server = net.createServer(function(socket) {
        fn(os.createStream(socket));
    });

    this.listen = function(port, domain) {
        server.listen(port, domain);
    };

    this.close = function() { server.close(); }
}

exports.createServer = function(fn) {
    return new Server(fn);
}

exports.createClient = function(port, host, connect)
{
    var socket;

    if (typeof port === 'object') {
        socket = port;
        port = undefined;
    }

    if (typeof host === 'function') {
        connect = host;
        host = undefined;
    }

    if (!socket)
        socket = net.connect(port, host);

    var stream = os.createStream(socket);

    if (connect)
        socket.on('connect', connect);

    return stream;
}
