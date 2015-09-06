/**
 * Global settings
 */

var mongoose = require('mongoose');
var Logger = require('arsenic-logger');
var sys = require('sys')
var path = require('path');
var redisEmitter = require('redis-eventemitter');
var redis = require("redis");

var Settings = {

    /** Default time zone */
    timeZone : 'America/New_York',

    /** Mongo connection */
    mongoDb : null,

    /** Redis connection */
    redisClient : null,

    /** Redis PubSub connection */
    redisPubSub : null,

    envDefaults : {
        "NODE_ENV": "local",
        "LOG_LEVEL": "debug",
        "MONGO_HOST_URL": "mongodb://localhost/nStorm",
        "REDIS_PORT": 6379,
        "REDIS_HOST": '127.0.0.1',
        "REDIS_MQ_PREFIX": 'nstorm-pubsub:',
    },

    /**
     * Initialize any connections
     */
    init : function(){

        // Check for environment variables, or take defaults

        for (var key in Settings.envDefaults) {
            if (Settings.envDefaults.hasOwnProperty(key)){
                if (typeof process.env[key] == 'undefined'){
                    process.env[key] = Settings.envDefaults[key];
                    Logger.warnX("Settings", "Environment variable '" + key + "'' is not defined, so using default \""+Settings.envDefaults[key]+"\"");
                }                
            }
        };


        Logger.setLevel(process.env.LOG_LEVEL);
        //Logger.catchExceptions();


        // Connect to Redis....
        var redisSettings = {
            prefix: process.env.REDIS_MQ_PREFIX,
            host: process.env.REDIS_HOST,
            port: process.env.REDIS_PORT
        }
        
        Settings.redisClient = redis.createClient(process.env.REDIS_PORT, process.env.REDIS_HOST, {prefix: process.env.REDIS_MQ_PREFIX});            
        Settings.redisPubSub = redisEmitter(redisSettings);

        // Connect to Mongo....

        Settings.mongoDb = mongoose.connect(process.env.MONGO_HOST_URL, function(err) {
            if (err) {
                Logger.fatalX("Settings", "Error connecting to Mongo! Is Mongo running?");
                if (callback) { 
                    callback(err)
                }
                else {
                    throw err;                
                }
            }
        });

        mongoose.connection.on('open', function () {            
            Logger.debugX("Settings", "Connected to MongoDB!");
        });

    }
}

Logger.info("Setting up connection to database, using " + process.env.NODE_ENV + " environment");

Settings.init();

module.exports = Settings;
