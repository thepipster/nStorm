/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
var Logger = require('arsenic-logger');
var Block = require('../models/Block.js')
var Topology = require('../models/Topology.js')
var Settings = require('../Settings.js')
var async = require("async");
var _ = require("lodash");
var os = require("os");

/*

General Error Codes:

2XX: Error passing paras
3XX: Mongo database errors

Error Codes:

201: Requires a topology name, not found
202: Requires a block name, not found
203: Requires a topology id, not found

301: Error loading topology
302: Error saving topology
303: Error loading block
304: Error saving block
305: Block not found
*/


// ///////////////////////////////////////////////////////////////////////////////////////
//
// Helper middle-ware
//
// ///////////////////////////////////////////////////////////////////////////////////////

function sendError(res, errorCode, errorMessage, err){
    if (err){
        Logger.error(errorMessage, err);
        res.status(500).send({code: errorCode, message: errorMessage, error: err});
    }
    else {
        Logger.error(errorMessage);
        res.status(500).send({code: errorCode, message: errorMessage});
    }
    return;
}

// ///////////////////////////////////////////////////////////////////////////////////////

/**
* Check for a topo Id, if found pass to next 
*/
exports.getTopo = function(req, res, next){

    if (!('topoId' in req.params) || !req.params.topoId){
        return sendError(res, 203, "Requires a topology id, not found");    
    }

    Logger.infoX("LightningMQ", "Found topo, " + req.params.topoId);
    req.topoId = req.params.topoId;

    return next();
}

// ///////////////////////////////////////////////////////////////////////////////////////

/**
* Check for a topo Id and block name, if found pass to next 
*/
exports.getBlock = function(req, res, next){

    if (!('name' in req.body)){
        return sendError(res, 202, "Requires a block name, not found");    
    }

    if (!('topoId' in req.params)){
        return sendError(res, 203, "Requires a topology id, not found");    
    }

    Block.loadFromName(req.params.topoId, req.body.name, function(err, block){
        if (err){
            sendError(res, 303, "Error loading block", err);
        }
        else if (block){
            req.topoId = req.params.topoId;
            req.block = block;
            return next();
        }
        else {
            sendError(res, 305, "Block '"+req.body.name+"' not found in topology " + req.params.topoId);
        }
    });

}

// ///////////////////////////////////////////////////////////////////////////////////////
//
// Main services
//
// ///////////////////////////////////////////////////////////////////////////////////////

/**
 * Register a topology and return the topo id
 */
exports.registerTopology = function(req, res, next){

    if (!req.body.name){
        return sendError(res, 201, "Requires a topology name, none given");    
    }

    var defaults = {
        debug: false,
        reset: false, 
        replay: true,
        replayLimit: 3,
        replayTime: 300            
    };

    if (!req.body.options) req.body.options = {};
    var settings = _.merge(defaults, req.body.options);

    /**
    * Delete all entries in the database for this cloud, use with caution!!
    */ 
    var clearDatabase = function(topoName, topoId){

        /*
        Meta data moved to Mongo.........

        var prefix = "nsmeta-"+topoName+":*";

        Settings.redisCient.keys(prefix, function(err, keys){
            if (keys){
                for (var i=0; i<keys.length; i++){
                    client.del(keys[i]);
                }                    
            }
        });
        */

        var prefix = "ns-"+topoName+"*";

        Settings.redisCient.keys(prefix, function(err, keys){
            if (keys){
                for (var i=0; i<keys.length; i++){
                    client.del(keys[i]);
                }
            }
        });

        Block.clearStats(topoId)
    };

    // Check if this topo exists, if not then create
    Topology.loadFromName(req.body.name, function(err, topo){

        if (err){
            sendError(req, 301, "Error loading topology", err);   
        }

        if (!topo){
            Logger.infoX("LightningMQ", "Creating new topo");
            topo = Topology.create();
            topo.name = req.body.name;
        }

        topo.debug = settings.debug; 
        topo.replay = settings.replay; 
        topo.replayTime = settings.replayTime; 
        topo.replayLimit = settings.replayLimit; 
        topo.modified = new Date();

        topo.save(function(saveErr, savedTopo){

            if (saveErr){
                sendError(req, 302, "Error creating or updating topology", err);   
            }
            else {

                if (settings.reset){
                    clearDatabase(topo.name, topo._id);
                }

                res.send({'topoId': topo._id});   
            }
        });

    });

}

// ///////////////////////////////////////////////////////////////////////////////////////

/**
 * Register a block and return it's ID
 * Requires topoId (use getTopo)
 */
exports.registerBlock = function(req, res, next){

    Logger.info("Registering block with topo " + req.topoId);

    // Check if this block exists, if not create one
    Block.loadFromName(req.topoId, req.body.name, function(err, block){

        if (err){
            sendError(req, 303, "Error loading block", err);   
        }

        if (!block){
            block = Block.create();            
        }

        block.name = req.body.name;
        if (req.body.limit) block.limit = req.body.limit;
        block.modified = new Date();
        block.topoId = req.topoId;

        block.save(function(saveErr, savedBlock){
            if (saveErr){
                sendError(req, 304, "Error creating new block", saveErr);   
            }
            else {
                res.send({'block': savedBlock});                       
            }
        });

        setTimeout(function(){
            res.send({'block': block});   
        }, 5000);
    });

}

// ////////////////////////////////////////////////////////////////////////////////////////

/**
* Add subscribers, i.e. blocks that are listening to messages from this one.
* @param cloudName the cloud name
* @param targetBlockName the name of the block we want to subscribe to
* @param subscriberBlockName the subscribing block name
* @param filterOptions custom filter options          
* Requires topoId (use checkTopoId)
*/
exports.addSubscriber = function(req, res, next){

    Logger.info("Adding subscriber");

    var findSubscriberIndex = function(subscribers, name){
        for (var i=0; i<subscribers.length; i++){
            if (subscribers[i].name == req.block.name){
                return i;
            }
        }   
        return -1;     
    }

    // By the time we get here, we should already have a block on the req object

    var filterOptions = req.body.filterOptions;
    var targetBlockName = req.body.targetName;
    //var subscriberBlockName = req.body.subscriberBlockName;

    // Try to load target block
    Block.loadFromName(req.topoId, targetBlockName, function(err, targetBlock){
        
        if (err || !targetBlock){
            return sendError(req, 303, "Block tried to subscribe to non-existent block '"+targetBlockName+"'", err);     
        }

        if (!targetBlock.subscribers) targetBlock.subscribers = [];

        // See if we already have this subscriber
        var ind = findSubscriberIndex(targetBlock.subscribers, req.block.name);
        
        if (ind != -1){
            targetBlock.subscribers[ind].name = req.block.name;
            targetBlock.subscribers[ind].filter = filterOptions;
        }
        else {
            targetBlock.subscribers.push({name: req.block.name, filter: filterOptions});              
        }

        targetBlock.save(function(saveErr, savedBlock){
            if (saveErr){
                sendError(req, 304, "Error saving new block", saveErr);   
            }
            else {
                Logger.debug(savedBlock.subscribers);
                res.send({'block': savedBlock});      
            }
        });
    });


}

// ////////////////////////////////////////////////////////////////////////////////////////

/**
* Emit a message from block 'blockName', bnut store a copy to memory/database until an ack is recieved
*/  
exports.emit = function(req, res, next){

    if (!req.block.subscribers || req.block.subscribers.length == 0) {
        Logger.info("Block has no subscribers!");
        return res.send({'result': 'fail', 'message':'no subscribers'});     
    }

    /**
    * Check to see if any filter options have been passed. If so, check to seee
    * if the message has a corresponding key and it is of the correct value.
    */
    var isFiltered = function(msg, filter){

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

    for (var i=0; i<req.block.subscribers.length; i++){

        // Check filters
        if (isFiltered(msg, block.subscribers[i].filter)){

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

            Settings.redisClient.set(getMsgHashId(msg.id), JSON.stringify(msg))
            Settings.redisClient.lpush(getMsgQueueHashId(msg.target), msg.id);

            Settings.redisPubSub.emit(getPubSubHashId(msg.target), msg);

            // Update meta data for block, but don't wait
            Block.incStartCt(block._id, 1);

            return res.send({'result': 'ok'});   
        }

    }   

}

// ////////////////////////////////////////////////////////////////////////////

/**
 * Acknowledges a message that this block recieved from another block. 
 * Note: this is called on the block's output collector that recieved the message
 * NOT the block that sent the message.
 */
exports.ack = function(req, res, next){
}

// ////////////////////////////////////////////////////////////////////////////

