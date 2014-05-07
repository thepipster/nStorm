/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
var Logger = require('arsenic-logger');
var redisEmitter = require('redis-eventemitter');
var redis = require("redis");
var async = require("async");
var _ = require("lodash");
var os = require("os");

nStorm = (function() {

    // class function a.k.a. constructor
    function cls(options){

        /** Static instance of blocks so the worker threads can access */
        var blocks = {};

        // Private instance fields
        var self = this; // Grab a reference to myself
        var inputDeclarers = {};

        // ////////////////////////////////////////////////////////////////////////////////////////
        //
        // Process user options.....
        //
        // ////////////////////////////////////////////////////////////////////////////////////////

        var defaults = {
            cloudName: "stormcloud",
            metaLogging: true,
            useCluster: true,
            debug: false,
            redis: {
                port: 6379,
                host: '127.0.0.1',
                prefix: 'nstorm-pubsub:'
            },
            replay: true,
            replayLimit: 3,
            replayTime: 300            
        };

        var settings = _.merge(defaults, options);

        userCluster = settings.useCluster;

        // ////////////////////////////////////////////////////////////////////////////////////////
        //
        // Connect to Redis
        //
        // ////////////////////////////////////////////////////////////////////////////////////////

        var client = redis.createClient(settings.redis);            

        var pubsub = redisEmitter(settings.redis);

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

            // If meta data logging is turned on, create an empty stats object for this block            
            if (settings.metaLogging){
                _initBlockMeta(name);
            }

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

            if (settings.useCluster){

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

                    setTimeout(function(){
                        _startBlock(blockName);
                    }, 250);

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

                var workerPubSub = redisEmitter(settings.redis);

                // List for messages sent to the block
                workerPubSub.on(getPubSubHashId(blockName), function(channel, msg) {
                    if (settings.debug) Logger.debug("["+blockName+"] Got message " + msg.id);
                    _doProcess(blockName, msg);
                });

                setInterval(function(){_checkThreads(blockName)}, 200);

                if (settings.replay){
                    setInterval(function(){_processLostMessages(blockName)}, settings.replayTime);                    
                }
            }

            function _doProcess(blockName, msg, isReprocessed){

                var block = blocks[blockName];

                if (!block){
                    Logger.error("block not defined!?");
                    return;
                }

                if (settings.debug) Logger.debug("[" + blockName + "] threads = " + block.threads.length + " limit = " + block.limit);

                // Set a timeout to release this thread after a long long time, just in case something went wrong

                if (block.limit == -1 || block.threads.length < block.limit){

                    block.threads.push(msg.id);                        

                    try {

                        if (settings.debug) {
                            if (isReprocessed){
                                Logger.warn("[" + blockName + "] Re-processing message " + msg.id + ". Replay count = " + msg.replays, block.threads);
                            }
                            else {
                                Logger.debug("[" + blockName + "] Processing message " + msg.id + ". Replay count = " + msg.replays, block.threads);
                            }                            
                        }

                        block.process(msg, block);
                    }
                    catch(err){
                        Logger.error(err.message, err.stack);
                        _clearThread(block, msg.id);
                    }

                }
                else {
                    if (settings.debug) Logger.warn("[" + blockName + "] Overloaded! Can't process this message so rejecting! " + msg.id);
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

                client.llen(getMsgQueueHashId(blockName), function(err, noMessages){
                    
                    var block = blocks[blockName];

                    //if (noMessages && noMessages > 0){
                    //    Logger.warn("[" + blockName + "] " + noMessages + " unprocessed messages in the queue");                                                
                    //}

                    if (noMessages > 0 && (block.limit == -1 || block.threads.length <= block.limit)){
                        
                        // First get all the items in the message list, and starting from the end of the list
                        // look for any messages that are not in the thread list (i.e. aren't currently being processed)
                        client.lrange(getMsgQueueHashId(blockName), 0, -1, function(err, messageList){

                            if (messageList){

                                // Find the lost messages, which will be the msgId's in the message List 
                                // that are not in the threads list
                                var lost = _.difference(messageList, block.threads);

                                if (settings.debug) Logger.debug("Lost messages = ", lost);
                                if (settings.debug) Logger.debug("messageList = ", messageList);
                                if (settings.debug) Logger.debug("block.threads = ", block.threads);

                                if (lost && lost.length > 0){
                                    
                                    var oldestMsgId = lost[lost.length-1];

                                    // Remove this message from the message list and reprocess
                                    client.lrem(getMsgQueueHashId(blockName), -1, oldestMsgId, function(err){
                                        
                                        if (err) {
                                            Logger.error(err);
                                        }
                                        else {

                                            client.get(getMsgHashId(oldestMsgId), function(err, msgString){

                                                if (err) Logger.error(err);

                                                if (msgString){

                                                    var msg = JSON.parse(msgString);
                                                    msg.replays++;

                                                    if (msg.replays < settings.replayLimit){

                                                        // Add back into message queue and attempt to process
                                                        client.lpush(getMsgQueueHashId(msg.target), msg.id);

                                                        // Update (msg.replays changed)
                                                        client.set(getMsgHashId(msg.id), JSON.stringify(msg))

                                                        // Now try to re-process
                                                        _doProcess(blockName, msg, true);                                                                                 
                                                    }
                                                    else {

                                                        // Delete this message
                                                        client.del(getMsgHashId(msg.id));

                                                        if (settings.debug) Logger.error("Message " + msg.id + " has reached replay limit, discarding");   
                                                    }
                                                }

                                            });
                                        }

                                    });

                                }
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

            if (!(blockName in blocks)) {
                Logger.error("Block '"+subscriberName+"' tried to subsribe to block '"+blockName+"', but no such block exists?");
                return;
            }
            if (settings.debug) Logger.info("Block '"+subscriberName+"' is subsribing to block '"+blockName+"'");
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
                    msg.replays = 0;
                    msg.sentTime = process.hrtime();

                    client.set(getMsgHashId(msg.id), JSON.stringify(msg))
                    client.lpush(getMsgQueueHashId(msg.target), msg.id);

                    pubsub.emit(getPubSubHashId(msg.target), msg);

                    if (settings.metaLogging) {
                        _updateBlockMeta(blockName, 'emit');
                    }

                }

            }        

        }

        // ////////////////////////////////////////////////////////////////////////////


        function getPubSubHashId(blockName){
            return "nstorm-"+settings.cloudName+"-pubsub:"+blockName;
        }
    
        function getMsgQueueHashId(blockName){
            return "nstorm-"+settings.cloudName+":msg-ids-"+blockName
        }

        function getMsgHashId(msgId){
            return "nstorm-"+settings.cloudName+":msg-"+msgId;
        }

        function getBlockMetaHashId(blockName){
            return "nstorm-meta-"+settings.cloudName+":"+blockName;
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

            _clearThread(block, msg.id);

            var processTime = process.hrtime(msg.sentTime);

            var nanos = processTime[0] * 1000000000 + processTime[1];
            var ms = nanos / 1000000;

            if (settings.debug) Logger.info("["+blockName+"] Message ack, took " + ms + " ms " + msg.id, block.threads);

            // Remove this message from my list of messages as its been processed
            client.lrem(getMsgQueueHashId(blockName), -1, msg.id);
            client.del(getMsgHashId(msg.id));

            // If meta data logging is turned on, update the stats on this block            
            if (settings.metaLogging) {
                _updateBlockMeta(blockName, 'ack', {processTime:ms});
            }
        };        

        // ////////////////////////////////////////////////////////////////////////////

        function _generateID(){
            
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

        // ////////////////////////////////////////////////////////////////////////////

        /**
        * Initialize the block meta data, which contains meta data about performance of each block
        */
        function _initBlockMeta(blockName){

            client.get(getBlockMetaHashId(blockName), function(err, metaString){

                var meta = null;

                if (!metaString){
                    meta = {};
                    meta.inputs = [];
                    meta.outputs = [];
                    meta.hosts = [];
                    meta.processFinishCt = 0;
                    meta.processStartCt = 0;
                    meta.processTimes = [];
                }
                else {
                    meta = JSON.parse(metaString);
                }

                // Add this host to the list
                meta.hosts.push(os.hostname());
                meta.hosts = _.uniq(meta.hosts);

                client.set(getBlockMetaHashId(blockName), JSON.stringify(meta));
            });

        }

        // ////////////////////////////////////////////////////////////////////////////

        /**
        * Update the block meta data, which contains meta data about performance of each block
        */
        function _updateBlockMeta(blockName, type, data){

            client.get(getBlockMetaHashId(blockName), function(err, metaString){

                if (!metaString){
                    Logger.error("Meta data object not setup for block " + blockName);
                    return;
                }

                meta = JSON.parse(metaString);

                if (type == 'ack'){

                    meta.processFinishCt++;
                    meta.lastAckTime = Date.now();
                    bufferInsert(meta.processTimes, data.processTime);

                    client.set(getBlockMetaHashId(blockName), JSON.stringify(meta));
                }
                else if (type == 'emit'){
                    meta.lastEmitTime = Date.now();
                    meta.processStartCt++;
                }

                client.set(getBlockMetaHashId(blockName), JSON.stringify(meta));
            });

            /**
            * Make an array act like a FILO buffer with limited size
            */
            function bufferInsert(list, val){
                list.push(val);
                if (list.length > 100){
                    list.shift();
                }
            }

        }

        // ////////////////////////////////////////////////////////////////////////////        

    }

    return cls;

})();

module.exports = nStorm;
