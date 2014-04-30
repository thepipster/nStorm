/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */


var Topology = require('./Topology.js');
var Logger = require('arsenic-logger');
//var events = require('events');
var redisEmitter = require('redis-eventemitter');
var redis = require("redis");

var getMicroSeconds = function(){
    var hrTime = process.hrtime()
    return hrTime[0] * 1000 + hrTime[1] / 1000;
}

TopologyBuilder = (function(){

    // class function a.k.a. constructor
    return function(){

        // Private instance fields
        var self = this; // Grab a reference to myself
        var spouts = {};
        var bolts = {};
        var inputDeclarers = {};

        // ////////////////////////////////////////////////////////////////////////////

        /**
         * Add a spout to the topology
         * @param name
         * @param spout
         */
        this.setSpout = function(name, spout) {
            if (typeof spouts[name] !== 'undefined'){
                throw "A spout already exists with this name. Bolt names must be unique";
            }
            spouts[name] = spout;
        }

        // ////////////////////////////////////////////////////////////////////////////

        /**
         * Get a spout by its name
         * @param name
         * @returns {*}
         */
        this.getSpout = function(name) {
            return spouts[name];
        }

        // ////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Add a bolt to the topology
         *
         * Example usage;
         *
         * var builder = new TopologyBuilder();
         *
         * builder.setSpout("spout", spout);
         * builder.setBolt("filter", filter).shuffleGrouping("spout");
         *
         * Where in this case, shuffleGrouping("spout") indicates to randomly take input
         * from connected spouts (there may be only one single spout defined for this topology
         * but there maybe be multiple instances of that spout if you're using more than one
         * worker).
         *
         * @param name The name of this bolt, use a unique name
         * @param bolt The implentation of the bolt
         * @param limit maximum number of bolt functions that can run at one time *per* worker
         * @returns {InputDeclarer}
         */
        this.setBolt = function(name, bolt, limit) {

            if (typeof bolts[name] !== 'undefined'){
                throw "A bolt already exists with this name. Bolt names must be unique";
            }

            if (!limit) limit = -1;

            bolt.inputs = [];
            bolts[name] = bolt;
            bolts[name].limit = limit;
            bolts[name].numberThreads = 0;

            var declarer = new InputDeclarer(name);
            inputDeclarers[name] = declarer;
            return declarer;
        };

        // ////////////////////////////////////////////////////////////////////////////////////////

        this.getBolt = function(name) {
            return bolts[name];
        };

        // ////////////////////////////////////////////////////////////////////////////////////////

        /**
         * {private} This function sets up the input for a bolt. Part of defining a topology is specifying for each
         * bolt which streams it should receive as input. 
         *
         * Loosely based on Twitter Storm, but adapted for simplicity
         * @see https://github.com/nathanmarz/storm/wiki/Concepts
         *
         * @param boltName
         * @param filterOptions filtering options 
         * @constructor
         */
        function InputDeclarer(boltName, filterOptions) {

            var inputs = [];

            this.input = function(sourceName, filterOptions) {
                inputs.push({sourceName: sourceName, filter: filterOptions});
                return this;
            }

            this.getInputs = function() {
                return inputs;
            }

        }

        // ////////////////////////////////////////////////////////////////////////////////////////

        this.createTopology = function() {

            var outputs = {};
            var inputs = {};
            
            for (var name in spouts) {
                var collector = new OutputCollector(name, true);
                outputs[name] = collector;
            }
            
            for (var name in bolts) {
                var collector = new OutputCollector(name, false);
                outputs[name] = collector;
            }
            
            for (var name in inputDeclarers)
            {
                var inputDeclarer = inputDeclarers[name];
                var bolt = bolts[name];
                var output = outputs[name];
                var inputCollector = new InputCollector(name, bolt, output);
                
                inputs[name] = inputCollector;

                inputDeclarer.getInputs().forEach(function(input) {
                    //Logger.debug(">>>> Block " + name + " listening to block " + input.sourceName);
                    outputs[input.sourceName].addSubscriber(name, input.filter);
                });
            }
            
            return new Topology(spouts, bolts, outputs, inputs); 
        }

        // ////////////////////////////////////////////////////////////////////////////

        function OutputCollector(blockName, isSpout) {

            this.name = blockName;
            this.numberThreads = 0;

            var os = require("os");
            var workerHost = os.hostname();
            //this.workerId = blockName + '-' + Math.random().toString(16).slice(2);

            //Logger.error("[OutputCollector] >>>>>>>>> " + blockName + " <<<<<<<<<<<<<");

            var subscribers = [];


            var sentMessages = [];

            var self = this;

            var client = redis.createClient({
                port: 6379,
                host: '127.0.0.1'
            });

            var pubsub = redisEmitter({
                port: 6379,
                host: '127.0.0.1',
                prefix: 'nStorm:'
            });

            /**
            * Emit a message, bnut store a copy to memory/database until an ack is recieved
            */  
            this.emit = function(msg) {

                for (var i=0; i<subscribers.length; i++){

                    // Check filters
                    if (_isFiltered(msg, subscribers[i].filter)){

                        // Store local copy of message before we send it.
                        // NOTE: there may be more than on subsciber to each message, we
                        // generate a seperate message for each subscriber from the original
                        // message.

                        var message = {};

                        msg.id = _generateID();
                        //msg.id = sentMessages.length;
                        msg.target = subscribers[i].name;
                        msg.source = self.name;

                        //sentMessages.push(msg);
                        //client.hmset("msg-"+msg.id, msg);
                        client.lpush("msg-"+msg.target, JSON.stringify(msg));

                        //Logger.debug("[" + blockName + "] Sending message to " + subscribers[i].name);
                        pubsub.emit("nStorm:"+subscribers[i].name, msg);

                    }

                }

            }

            /**
            * Check to see if any filter options have been passed. If so, check to seee
            * if the message has a corresponding key and it is of the correct value.
            */
            function _isFiltered(msg, filter){

                if (!filter) return true;

                for (var key in filter){
                    for (var msgKey in msg){
                        if (msgKey == key){
                            if (filter[key] == msg[msgKey]) {
                                return true;
                            }
                            else {
                                return false;
                            }
                        }
                    }
                }

                return true;
            }

            /**
            * Add subscribers, i.e. blocks that are listening to messages from this one.
            * @param subscriberName the subscribing block name
            * @param filterOptions custom filter options          
            */
            this.addSubscriber = function(subscriberName, filterOptions){
                subscribers.push({name: subscriberName, filter: filterOptions});
            }  


            /**
             * Acknowledges a message that this block recieved from another block. 
             * Note: this is called on the block's output collector that recieved the message
             * NOT the block that sent the message.
             */
            this.ack = function(msg){

                //Logger.info(">>>>>>>>> Message ack! " + msg.id + " " + sentMessages.length);
                //Logger.info("[" + this.name + "]", msg);

                Logger.debug("[" + this.name + "]", self.numberThreads);
                self.numberThreads--;

                // Remove this message from my list of messages as its been processed
                client.lrem("msg-"+this.name, -1, JSON.stringify(msg));

            };

        }

        // ////////////////////////////////////////////////////////////////////////////

        function _generateID(){
            return 'xxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                return v.toString(16);
            });                         
        }

        // ////////////////////////////////////////////////////////////////////////////

        function _generateUID(){
            return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                return v.toString(16);
            });                      
        }

        // ////////////////////////////////////////////////////////////////////////////

        function InputCollector(blockName, bolt, outputCollector){

            this.name = blockName;

            var self = this;

            /*
            var os = require("os");
            this.workerHost = os.hostname();
            this.workerId = blockName + '-' + Math.random().toString(16).slice(2);
            */

            var client = redis.createClient({
                port: 6379,
                host: '127.0.0.1'
            });            

            var pubsub = redisEmitter({
                port: 6379,
                host: '127.0.0.1',
                prefix: 'nStorm:'
            });

            setInterval(_processLostMessages, 1000);

            /**
            * List for messages sent to the block
            */
            pubsub.on("nStorm:"+blockName, function(channel, msg) {
                Logger.debug("["+blockName+"] Got message", msg);
                self.process(msg);
            });

            /**
             * Add a listener for any messages sent to this block, and pass to the
             * bolt's process method
             *
             * @param listener A InputCollector
             * @param options Filter/advanced options
             */
            this.process = function(msg) {

                Logger.debug("[" + this.name + "] threads = " + outputCollector.numberThreads + " limit = " + bolt.limit);

                // Set a timeout to release this thread after a long long time, just in case something went wrong
                setTimeout(function(){outputCollector.numberThreads--}, 5*60000);

                if (bolt.limit == -1 || outputCollector.numberThreads < bolt.limit){
                    
                    outputCollector.numberThreads++;

                    try {
                        bolt.process(msg, outputCollector);
                    }
                    catch(err){
                        Logger.error(err);
                        outputCollector.numberThreads--;
                    }

                }
                else {
                    Logger.warn("[" + this.name + "] Overloaded! Can't process this message so rejecting!");
                }

            }

            /**
            * Periodically check for any messages that haven't been processed yet, which includes any
            * messages passed into the process method that were not acknowledged!
            */
            function _processLostMessages(){

                client.llen("msg-"+self.name, function(err, noMessages){
                    
                    Logger.debug("[" + self.name + "] " + noMessages + " unprocessed messages in the queue");                    

                    if (noMessages > 0 && (bolt.limit == -1 || outputCollector.numberThreads <= bolt.limit)){
                        // Process the message in the head of the list (which will be the olders)
                        client.lpop("msg-"+self.name, function(err, msgString){
                            if (err) Logger.error(err);
                            if (msgString){
                                var msg = JSON.parse(msgString);
                                //Logger.warn("[" + self.name + "] Re-processing old message ", msg);
                                self.process(msg);                                
                            }
                        });                        
                    }

                });

                //client.keys("msg-*");
            }

        }

    }

})();

module.exports = TopologyBuilder;