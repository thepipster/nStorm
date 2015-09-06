/**
 * @author: mike@arsenicsoup.com
 * @since: September 15th, 2014
 */
var mongoose = require('mongoose');
var Logger = require('arsenic-logger');
var Schema = mongoose.Schema;
var Settings = require('../Settings.js');

var ObjectId = mongoose.Schema.Types.ObjectId;

var Block = {

    /** Mongoose Model for this class */
    Model : null,

    // //////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Static initializer method
     */
    init : function(){

        var SubscriberSchema = new mongoose.Schema({
            name: { type: String, required: true, index: true },
            filter: [mongoose.Schema.Types.Mixed],
        });

        var BlockSchema = new mongoose.Schema({
            name: { type: String, required: true, index: true },
            topoId: { type: ObjectId, required: true },
            limit: { type: Number, required: false },
            finishCt: { type: Number, required: false },
            startCt: { type: Number, required: false },
            subscribers: [SubscriberSchema],
            lastAckTime: {type: Date, default: Date.now},
            modified: {type: Date, default: Date.now}
        });

        // Turn off autoIndex in production
        if (process.env.NODE_ENV == 'production'){
            BlockSchema.set('autoIndex', false);
        }

        //
        // Setup instance and static methods here
        //

        // Setup the mongoose model, which we'll re-use
        this.Model = Settings.mongoDb.model('Block', BlockSchema, 'Blocks');
    },

    // //////////////////////////////////////////////////////////////////////////////////////////////

    create : function(){
        return new this.Model();
    },

    getModel : function(){return this.Model;},

    load : function(id, callback){ this.Model.findOne({'_id': id}, callback);},

    loadFromName : function(topoId, name, callback){ this.Model.findOne({'topoId': topoId, 'name':name}, callback);},

}

// Initalize the static variables
Block.init();

// node.js module export
module.exports = Block;