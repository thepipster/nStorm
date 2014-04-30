/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
var Logger = require('arsenic-logger');
var redisEmitter = require('redis-eventemitter');
var redis = require("redis");
var async = require("async");


nStorm = (function() {

    // class function a.k.a. constructor
    return function() {

    /** Static instance of blocks so the worker threads can access */
    var blocks = {};

        // Private instance fields
        var self = this; // Grab a reference to myself
        var inputDeclarers = {};

        // Flag to indicate if nSTorm should use Node.js cluster 
        // and place each worker in its own child process
        var useCluster = false;

       // var blocks = {};

        var client = redis.createClient({
            port: 6379,
            host: '127.0.0.1'
        });            

        var pubsub = redisEmitter({
            port: 6379,
            host: '127.0.0.1',
            prefix: 'nStorm:'
        });

        // ////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Add a block to the topology
         *
         * Example usage;
         *
         * var builder = new TopologyBuilder();
         *
         * builder.setSpout("spout", spout);
         * builder.setblock("filter", filter).shuffleGrouping("spout");
         *
         * Where in this case, shuffleGrouping("spout") indicates to randomly take input
         * from connected spouts (there may be only one single spout defined for this topology
         * but there maybe be multiple instances of that spout if you're using more than one
         * worker).
         *
         * @param name The name of this block, use a unique name
         * @param block The implentation of the block
         * @param limit maximum number of block functions that can run at one time *per* worker
         * @returns {InputDeclarer}
         */
        this.addBlock = function(name, block, limit) {

            if (typeof blocks[name] !== 'undefined'){
                throw "A block already exists with this name. Block names must be unique";
            }

            if (!limit) limit = -1;

            block.name = name;
            block.subscribers = [];
            block.limit = limit;
            block.threads = [];

            // Add required functions
            block.emit = function(msg){
                //Logger.debug('Block ' + block.name + ' emitting ', msg);
                _emit(block.name, msg);
            }

            block.ack = function(msg){
                _ack(block.name, msg);                
            }

            blocks[name] = block;

            var declarer = new InputDeclarer(name);
            inputDeclarers[name] = declarer;
            return declarer;
        };

        /**
         * {private} This function sets up the input for a block. Part of defining a topology is specifying for each
         * block which streams it should receive as input. 
         *
         * Loosely based on Twitter Storm, but adapted for simplicity
         * @see https://github.com/nathanmarz/storm/wiki/Concepts
         *
         * @param blockName
         * @param filterOptions filtering options 
         * @constructor
         */
        function InputDeclarer(blockName) {

            this.input = function(sourceName, filterOptions) {
                _addSubscriber(sourceName, blockName, filterOptions);
                return this;
            }

        }

        // ////////////////////////////////////////////////////////////////////////////////////////

        this.getBlock = function(name) {
            return blocks[name];
        };


        // ////////////////////////////////////////////////////////////////////////////

        this.start = function() {

            if (useCluster){

                var cluster = require('cluster');

                if (cluster.isMaster) {

                    var workerNames = {};

                    function forkWorker(blockName){
                        // Spin off worker thread for block....
                        Logger.info(blockName);
                        var worker = cluster.fork({ name: blockName});
                        workerNames[worker.id] = blockName;
                    }

                    // Fork all the blocks to seperate processes...
                    for (var name in blocks) {
                        forkWorker(name);
                    }

                    // If a block dies, restart it....
                    cluster.on('exit', function(worker, code, signal) {
                        var blockName = workerNames[worker.id];
                        Logger.error('worker ' + blockName + ' died', code, signal);
                        delete workerNames[worker.id];
                        forkWorker(blockName);
                    });

                } 
                else {

                    var blockName = process.env.name;
                    _startBlock(blockName);

                }
            }
            else {

                for (var name in blocks) {
                    _startBlock(name);
                }

            }

        };

        // ////////////////////////////////////////////////////////////////////////////

        function _startBlock(blockName){

            var block = blocks[blockName];

            Logger.info("Starting child process for " + blockName);

            block.threads = [];

            if (typeof block.start !== 'undefined'){
               block.start(block);
            }

            if (typeof block.process !== 'undefined'){

                var workerPubSub = redisEmitter({
                    port: 6379,
                    host: '127.0.0.1',
                    prefix: 'nStorm:'
                });

                // List for messages sent to the block
                workerPubSub.on("nStorm:"+blockName, function(channel, msg) {
                    //Logger.debug("["+blockName+"] Got message", msg);
                    _doProcess(blockName, msg);
                });

                setInterval(function(){_checkThreads(blockName)}, 1000);
                setInterval(function(){_processLostMessages(blockName)}, 1000);
            }

            function _doProcess(blockName, msg){

                var block = blocks[blockName];

                if (!block){
                    Logger.error("block not defined!?");
                    return;
                }

                //Logger.debug("[" + blockName + "] threads = " + block.threads.length + " limit = " + block.limit);

                // Set a timeout to release this thread after a long long time, just in case something went wrong

                if (block.limit == -1 || block.threads.length < block.limit){

                    block.threads.push(msg.id);                        

                    try {
                        block.process(msg, block);
                    }
                    catch(err){
                        Logger.error(err.message, err.stack);
                        _clearThread(block, msg.id);
                    }

                }
                else {
                    Logger.warn("[" + this.name + "] Overloaded! Can't process this message so rejecting!");
                }

            }

            /**
            * Periodically check if the threads are still being used, we do this by checking to 
            * see if the message for that thread is still on the stack
            */
            function _checkThreads(blockName){
                
                if (!blockName){
                    Logger.error("block not defined!?");
                    return;
                }

                var block = blocks[blockName];

                if (!('threads' in block)) block.threads = [];

                if (block.threads.length == 0) return;

                async.map(
                    block.threads,
                    function(msgId, callback){
                        client.exists(msgId, function(err, val){
                            if (!val) {
                                _clearThread(block, msgId);
                            }
                            callback();
                        });

                    }, 
                    function(err, results){
                    }
                );

            }

            /**
            * Periodically check for any messages that haven't been processed yet, which includes any
            * messages passed into the process method that were not acknowledged!
            */
            function _processLostMessages(blockName){

                if (!blockName){
                    Logger.error("block not defined!?");
                    return;
                }

                var block = blocks[blockName];

                client.llen("msg-ids-"+blockName, function(err, noMessages){
                    
                    if (noMessages && noMessages > 0){
                        Logger.warn("[" + blockName + "] " + noMessages + " unprocessed messages in the queue");                                                
                    }

                    if (noMessages > 0 && (block.limit == -1 || block.threads.length <= block.limit)){
                        
                        // Process the message in the head of the list (which will be the olders)
                        client.lpop("msg-ids-"+blockName, function(err, msgId){
                            
                            if (err) Logger.error(err);

                            if (msgId){

                                client.get("msg-"+msgId, function(err, msgString){

                                    if (err) Logger.error(err);

                                    if (msgString){
                                        var msg = JSON.parse(msgString);
                                        Logger.warn("[" + blockName + "] Re-processing old message ");
                                        _doProcess(blockName, msg);                         
                                    }

                                });
                            }

                        });

                    }


                });

                //client.keys("msg-*");
            }
        }

        // ////////////////////////////////////////////////////////////////////////////

        this.stop = function() {

            if (server){
                server.end();
            }

            if (listener){
                listener.close();
            }

            workers.forEach(function(worker) {
                worker.end();
            });
        };

        // ////////////////////////////////////////////////////////////////////////////////////////

        /**
        * Add subscribers, i.e. blocks that are listening to messages from this one.
        * @param subscriberName the subscribing block name
        * @param filterOptions custom filter options          
        */
        function _addSubscriber(blockName, subscriberName, filterOptions){
            //Logger.info("Block '"+subscriberName+"' is subsribing to block '"+blockName+"'");
            blocks[blockName].subscribers.push({name: subscriberName, filter: filterOptions});
        }  

        // ////////////////////////////////////////////////////////////////////////////////////////

        /**
        * Emit a message from block 'blockName', bnut store a copy to memory/database until an ack is recieved
        */  
        function _emit(blockName, msg){

            var block = blocks[blockName];

            for (var i=0; i<block.subscribers.length; i++){

                // Check filters
                if (_isFiltered(msg, block.subscribers[i].filter)){

                    // Store local copy of message before we send it.
                    // NOTE: there may be more than on subsciber to each message, we
                    // generate a seperate message for each subscriber from the original
                    // message.

                    var message = {};

                    msg.id = _generateID();
                    msg.target = block.subscribers[i].name;
                    msg.source = blockName;

                    client.set("msg-"+msg.id, JSON.stringify(msg))
                    client.lpush("msg-ids-"+msg.target, msg.id);

                    pubsub.emit("nStorm:"+msg.target, msg);

                }

            }        

        }
    
        // ////////////////////////////////////////////////////////////////////////////

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

        // ////////////////////////////////////////////////////////////////////////////

        /**
         * Acknowledges a message that this block recieved from another block. 
         * Note: this is called on the block's output collector that recieved the message
         * NOT the block that sent the message.
         */
        function _ack(blockName, msg){

            var block = blocks[blockName];

            //Logger.info(">>>>>>>>> Message ack! " + msg.id );
            //Logger.info("[" + this.name + "]", msg);

            _clearThread(block, msg.id);

            // Remove this message from my list of messages as its been processed
            client.lrem("msg-ids-"+blockName, -1, msg.id);
            client.del("msg-"+msg.id);

        };        

        // ////////////////////////////////////////////////////////////////////////////

        function _generateID(){
            
            var os = require("os");

            var uid = 'xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                return v.toString(16);
            });      

            return os.hostname() + "-" + uid;                   
        }

        // ////////////////////////////////////////////////////////////////////////////

        /**
        * Clear a specific thread (based on message id associated with it)
        */
        function _clearThread(block, msgId){
            var index = block.threads.indexOf(msgId);
            if (index > -1) {
                block.threads.splice(index, 1);
            }                    
        }

    }


})();

module.exports = nStorm;
