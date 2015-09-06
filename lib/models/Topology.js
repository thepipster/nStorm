/**
 * @author: mike@arsenicsoup.com
 * @since: September 15th, 2014
 */
var mongoose = require('mongoose');
var Logger = require('arsenic-logger');
var Schema = mongoose.Schema;
var Settings = require('../Settings.js');


var Topology = {

    /** Mongoose Model for this class */
    Model : null,

    // //////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Static initializer method
     */
    init : function(){

        // private static field
        var TopoSchema = new Schema({
            name: { type: String, required: true, index: true },
            debug: { type: Boolean, required: false, default: false }, // Flag to turn on debug message
            replay: { type: Boolean, required: false, default: true }, // Flag to determing if lost/error'd messages should be replayed
            replayTime: { type: Number, required: false, default: 300 }, // Time between message replay
            replayLimit: { type: Number, required: false, default: 3 }, // Number of times to attempt message replay
            modified:  {type: Date, default: Date.now, index: true}
        });

        //
        // Setup instance and static methods here
        //

        // Setup the mongoose model, which we'll re-use
        this.Model = Settings.mongoDb.model('Topology', TopoSchema);
    },

    // //////////////////////////////////////////////////////////////////////////////////////////////

    create : function(){return new this.Model();},

    getModel : function(){return this.Model;},

    load : function(topoId, callback){ this.Model.findOne({'_id': topoId}, callback);},

    loadFromName : function(name, callback){ this.Model.findOne({'name': name}, callback);},
/*
    registerAck : function(blockId, blockName){

        this.Model.update({"_id": blockId}, {"$inc": { "finishCt": 1 }}).exec(callback);
        this.Model.update({"_id": id}, {"$set": { "lastAckTime": Date.now()}}).exec(callback);
        this.Model.update({"_id": id}, {"$push": { "processTimes": processTime}}).exec(callback);

        // Add the processing time, and remove oldest value if buffer is too big
        var plistName = prefix+":ptimes"; //getBlockProcessingTimesHashId(blockName);
        client.lpush(plistName, data.processTime);
        client.llen(plistName, function(err, len){
            if (len > 100){
                client.rpop(plistName);
            }
        });
    },
*/
    delete: function(id, callback){ this.Model.remove({'_id': id}, callback);}    
}

// Initalize the static variables
Topology.init();

// node.js module export
module.exports = Topology;