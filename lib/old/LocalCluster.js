/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */

var Logger = require('arsenic-logger');
var amqp = require('amqp');
var lodash = require('lodash');
var redis = require('redis-eventemitter');

LocalCluster = (function(){

    // class function a.k.a. constructor
    return function()
    {
        // Private instances variables
        var self = this; // Grab a reference to myself
        var workers = [];
        var topologies = [];

        // ////////////////////////////////////////////////////////////////////////////

        /**
         * Config conf = new Config();
         * conf.setDebug(true);
         * conf.setNumWorkers(2);
         * @param name
         * @param config
         * @param topology
         */
        this.submitTopology = function (name, options, topology) {


            topologies[name] = topology;

            var defaults = {
                mongoUrl : 'mongodb://127.0.0.1:27017/foxhollow',    
            };

            var settings = lodash.defaults(options, defaults);

            topology.start();

        };

        // ////////////////////////////////////////////////////////////////////////////

        /**
         * Kill a specific topology
         * @param name
         */
        this.killTopology = function(name){
            topologies[name].stop();
        };

        // ////////////////////////////////////////////////////////////////////////////

        /**
         * Shut down all currently running topologies
         */
        this.shutdown = function(){
            for(name in topologies){
                self.killTopology(name);
            }
        };
    }


})();

module.exports = LocalCluster;


