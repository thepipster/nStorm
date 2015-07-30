/**
 *
 * Main entry point for FoxHollow web app
 *
 */

//
// Determine if we're in production or development environment
//

if (typeof process.env.NODE_ENV == 'undefined'){
    process.env.NODE_ENV = 'local';
}

if (typeof process.env.NODE_API_PORT == 'undefined'){
    process.env.NODE_API_PORT = 4000;
}

//
// Module dependencies.
//

var express = require('express');
var bodyParser = require('body-parser');
var fs = require('fs');
var http = require('http');
//var https = require('https');
var path = require('path');
var Logger = require('arsenic-logger');
var LightningMQ = require('./routes/LightningMQ.js');

var pjson = require('../package.json');
Logger.info(">>> Version " + pjson.version);
Logger.info(">>> Starting in " + process.env.NODE_ENV + " mode");

var app = express();

//CORS middleware
var allowCrossDomain = function(req, res, next) {
    res.setHeader('Access-Control-Allow-Origin', "*");
    res.setHeader('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
    res.setHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, x-kurrent-timestamp, x-kurrent-udid, x-kurrent-id, Authorization");
    //res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    next();
}

var utf8 = require('utf8');

function getRawBody(req, res, next) {

    //req.setEncoding('utf8');
    req.rawBody = '';
    req.on('data', function(chunk) {
        //req.rawBody += utf8.encode(chunk);
        req.rawBody += chunk;
    });
    req.on('end', function(){
        req.rawBody = utf8.encode(req.rawBody);
    });

    next();

}

app.set('port', process.env.NODE_API_PORT);
app.use(allowCrossDomain);
app.use(getRawBody);
app.use(bodyParser());


//
// API Commands......
//

app.post('/v1/topo', LightningMQ.registerTopology);

app.post('/v1/block/:topoId', LightningMQ.getTopo, LightningMQ.registerBlock);
app.put('/v1/block/subscribe/:topoId', LightningMQ.getBlock, LightningMQ.addSubscriber);
app.post('/v1/block/message/:topoId/:blockId', LightningMQ.getBlock, LightningMQ.emit);
app.delete('/v1/block/message/:topoId/:msgId', LightningMQ.getBlock, LightningMQ.ack);

//
// Start server.....
//

process.on('SIGTERM', function () {
    Logger.info("Closing"); app.close();
});

http.createServer(app).listen(app.get('port'), function(){
    Logger.info("Express HTTP server listening on port " + app.get('port'));
});




