/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
var Logger = require('arsenic-logger');
var async = require("async");
var _ = require("lodash");
var os = require("os");

var nStormMQ =  {

    // class function a.k.a. constructor
    function cls(options){

        // Private instance fields
        var self = this; // Grab a reference to myself

        // ////////////////////////////////////////////////////////////////////////////////////////
        //
        // Process user options.....
        //
        // ////////////////////////////////////////////////////////////////////////////////////////

        var defaults = {
            port: 6379,
            host: '127.0.0.1',
            prefix: 'nstorm-pubsub:'
            replay: true,
            replayLimit: 3,
            replayTime: 300            
        };

        var settings = _.merge(defaults, options);

        // ////////////////////////////////////////////////////////////////////////////////////////

        /**
        * Emit a message from block 'blockName', bnut store a copy to memory/database until an ack is recieved
        */  
        function emit(fromBlockName, msg){
    
        }

        // ////////////////////////////////////////////////////////////////////////////

        /**
         * Acknowledges a message that this block recieved from another block. 
         * Note: this is called on the block's output collector that recieved the message
         * NOT the block that sent the message.
         */
        function ack(blockName, msg){
        };        

        // ////////////////////////////////////////////////////////////////////////////

        /**
        * Add subscribers, i.e. blocks that are listening to messages from this one.
        * @param subscriberName the subscribing block name
        * @param filterOptions custom filter options          
        */
        function _addSubscriber(blockName, subscriberName, filterOptions){
        }  


    }

    return cls;

})();

module.exports = nStorm;
