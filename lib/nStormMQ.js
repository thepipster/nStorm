/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
var Logger = require('arsenic-logger');
var Block = require('./models/Block.js')
var Topology = require('./models/Topology.js')
var Settings = require('./Settings.js')
var async = require("async");
var _ = require("lodash");
var os = require("os");

/*
var nohm = require('nohm').Nohm;
var redis = require('redis').createClient();

nohm.setClient(redis);
nohm.setPrefix('nstorm');

nohm.model('Block', {
    properties: {
        topoId: {
            type: 'string', 
            validations: ['notEmpty'] 
        },
        // Full name = cloudName + '-' + blockName
        name: {
            type: 'string', 
            unique: true,
            validations: ['notEmpty'] 
        },
        lastAckTime: {
            type: 'timestamp' 
        },
        finishCt: {
            type: function incrFinishCtBy(value, key, old) {
                return old + value;
            },
            defaultValue: 0
        }
        startCt: {
            type: function incrStartCtBy(value, key, old) {
                return old + value;
            },
            defaultValue: 0
        }
    }
});

nohm.model('Topology', {
    properties: {
        name: {
            type: 'string', 
            validations: ['notEmpty'] 
        },
    }
});

// http://maritz.github.io/nohm/#models
Nohm.model('Block', {});
Nohm.model('Topology', {});
*/

var nStormMQ = {

    // ////////////////////////////////////////////////////////////////////////////////////////

    registerTopology: function(cloudName, options){

        var defaults = {
            replay: true,
            replayLimit: 3,
            replayTime: 300,
            filter: null
        }

        var opts = _.merge(defaults, options);

    },

    // ////////////////////////////////////////////////////////////////////////////////////////

    /**
    * Add subscribers, i.e. blocks that are listening to messages from this one.
    * @param cloudName the cloud name
    * @param targetBlockName the name of the block we want to subscribe to
    * @param subscriberBlockName the subscribing block name
    * @param filterOptions custom filter options          
    */
    addSubscriber: function(cloudName, targetBlockName, subscriberBlockName, filterOptions){




        if (!(blockName in blocks)) {
            Logger.error("Block '"+subscriberName+"' tried to subsribe to block '"+blockName+"', but no such block exists?");
            return;
        }

        if (nStormMQ.settings.debug) Logger.infoX(blockName, "Block '"+subscriberName+"' is subsribing to block '"+blockName+"'");

        blocks[blockName].subscribers.push({name: subscriberName, filter: filterOptions});        
    },  

    // ////////////////////////////////////////////////////////////////////////////////////////

    /**
    * Emit a message from block 'blockName', bnut store a copy to memory/database until an ack is recieved
    */  
    emit : function(cloudName, fromBlockName, msg){

        var block = Nohm.factory('Block');

        // Get block
        block.find({name:cloudName+'-'+fromBlockName}, function(err, ids){
            
            // ids = array of all instances that have (somestring === 'hurg' && someBoolean === false)

            user.load(ids[0], function (err, properties) {
                if (err) {
                    // err may be a redis error or "not found" if the id was not found in the db.
                } 
                else {
                    console.log(properties);
                    // you could use this.allProperties() instead, which also gives you the 'id' property
                }
            });            
        });


        // Iterate over subscribing blocks

        //     Check filters, and emit


/*

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

                if (nStormMQ.settings.metaLogging) {
                    _updateBlockMeta(blockName, 'emit');
                }

            }

        }  
*/
    },

    // ////////////////////////////////////////////////////////////////////////////

    /**
     * Acknowledges a message that this block recieved from another block. 
     * Note: this is called on the block's output collector that recieved the message
     * NOT the block that sent the message.
     */
    ack : function(blockName, msg){
    },        

    // ////////////////////////////////////////////////////////////////////////////
    //
    // 
    //
    // ////////////////////////////////////////////////////////////////////////////


    // ////////////////////////////////////////////////////////////////////////////////////////

    /**
    * Delete all entries in the database for this cloud, use with caution!!
    */ 
    _clearDatabase : function(){

        var prefix = "nsmeta-"+nStormMQ.settings.cloudName+":*";

        client.keys(prefix, function(err, keys){
            if (keys){
                for (var i=0; i<keys.length; i++){
                    client.del(keys[i]);
                }                    
            }
        });

        var prefix = "ns-"+nStormMQ.settings.cloudName+"*";

        client.keys(prefix, function(err, keys){
            if (keys){
                for (var i=0; i<keys.length; i++){
                    client.del(keys[i]);
                }
            }
        });
    },

    // ////////////////////////////////////////////////////////////////////////////

    /**
    * Check to see if any filter options have been passed. If so, check to seee
    * if the message has a corresponding key and it is of the correct value.
    */
    _isFiltered : function(msg, filter){

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
    }, 


};

module.exports = nStorm;
