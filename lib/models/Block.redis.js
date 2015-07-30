/**
 * @author: mike@arsenicsoup.com
 * @since: September 15th, 2014
 */
var Logger = require('arsenic-logger');
var redisEmitter = require('redis-eventemitter');
var redis = require("redis").createClient();

/*
var nohm = require('nohm').Nohm;
var redis = require('redis').createClient();

nohm.setClient(redis);

nohm.model('User', {
    properties: {
        topoId: {
            type: 'string', 
            validations: ['notEmpty'] 
        },
        name: {
            type: 'string', 
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
*/

var Block = (function() {

    // class function a.k.a. constructor
    function cls(options){

        /** Block id (internal use) */
        this.id = null;

        /** Block name */
        this.name = "";

        /** Number of messages processed, i.e. that were ack'd */
        this.finishCt = 0;

        /** Number of messages recieved, i.e. number of process starts */
        this.startCt = 0;

        /** Time of last ack, in epoch */
        this.lastAckTime = null;

        /** Array of processing times */
        this.ptimes = [];

        // ////////////////////////////////////////////////////////////////////////////

        this.create = function(cloudName, blockName){

            if (!id){
                id = _generateID();
            }

            var prefix = _getBlockMetaHashId(cloudName, blockName);

            client.set(prefix + ":finishCt", 0);
            client.set(prefix + ":startCt", 0);
        }

        // ////////////////////////////////////////////////////////////////////////////

        this.load = function(id){

        }
    


        // ////////////////////////////////////////////////////////////////////////////

        /**
        * Update the block meta data, which contains meta data about performance of each block
        */
        _updateBlockMeta : function(cloudName, blockName, type, data){

            

            if (type == 'ack'){



            }
            else if (type == 'emit'){
                //meta.lastEmitTime = Date.now();
                //meta.processStartCt++;
                client.incrby(prefix+":startCt", 1);
            }
            else if (type == 'input'){
                //meta.inputs.push(data.blockName);
                //meta.inputs = _.uniq(meta.inputs);
            }
            else if (type == 'output'){
                //meta.outputs.push(data.blockName);
                //meta.outputs = _.uniq(meta.outputs);
            }

        },

    // ////////////////////////////////////////////////////////////////////////////

    /**
    * Initialize the block meta data, which contains meta data about performance of each block
    */                
    _initBlockMeta : function(blockName){

        var prefix = nSTormMQ.getBlockMetaHashId(blockName);

        if (!client.exists(prefix + ":finishCt")) client.set(prefix + ":finishCt", 0);
        if (!client.exists(prefix + ":startCt")) client.set(prefix + ":startCt", 0);

/*
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

            if (!meta.processTimes[os.hostname()])
                meta.processTimes[os.hostname()]= [];

            // Add this host to the list
            meta.hosts.push(os.hostname());
            meta.hosts = _.uniq(meta.hosts);

            client.set(getBlockMetaHashId(blockName), JSON.stringify(meta));
        });
*/

    }

    // //////////////////////////////////////////////////////////////////////////////////////////
    //
    // Private static methods
    //
    // //////////////////////////////////////////////////////////////////////////////////////////

    function _getPubSubHashId(blockName){
        return "ns-"+nStormMQ.settings.cloudName+"-pubsub:"+blockName;
    };

    // //////////////////////////////////////////////////////////////////////////////////////////

    function _getMsgQueueHashId(blockName){
        return "ns-"+nStormMQ.settings.cloudName+":msg-ids-"+blockName
    };

    // //////////////////////////////////////////////////////////////////////////////////////////

    function _getMsgHashId(msgId){
        return "ns-"+nStormMQ.settings.cloudName+":msg-"+msgId;
    };

    // //////////////////////////////////////////////////////////////////////////////////////////

    function _getBlockMetaHashId(cloudName, blockName){
        return "nsmeta-"+cloudName+":"+blockName;
    };

    // //////////////////////////////////////////////////////////////////////////////////////////

    function _generateID(){
        
        var uid = 'xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
            return v.toString(16);
        });      

        return os.hostname() + "-" + uid;                   
    };

    // ////////////////////////////////////////////////////////////////////////////

    this.registerAck = function(cloudName, blockName){

        var prefix = _getBlockMetaHashId(cloudName, blockName);

        client.incrby(prefix+":finishCt", 1);
        client.set(prefix+":lastAckTime", Date.now());

        // Add the processing time, and remove oldest value if buffer is too big
        var plistName = prefix+":ptimes"; //getBlockProcessingTimesHashId(blockName);
        client.lpush(plistName, data.processTime);
        client.llen(plistName, function(err, len){
            if (len > 100){
                client.rpop(plistName);
            }
        });
    }
        
    return cls;

})();

module.exports = Block;