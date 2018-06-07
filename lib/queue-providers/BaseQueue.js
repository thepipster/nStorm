const Logger = require('../Logger.js')

/**
 * Base class for a queue provider
 */
class BaseQueue {

    /**
     * Called by the nStorm constructor, and is passed the nStorm settings
     * @param {object} opts The nStorm settings
     */
    constructor(opts){
        this.settings = opts
    }

    /**
     * Hook called by nStorm when a block is registered in the topology
     * spouts is called
     * @param {object} block The block
     * @return a Promise
     */
    registerBlock(block){
    }

    /**
     * Hook called by nStorm when the topology is setup but before the start() method of the 
     * spouts is called
     * @return a Promise
     */
    start(){
    }

    /**
     * Called by a Spout/Bolt internally to send a message to a specific block
     * @param {string} blockName Target block name
     * @param {object} message The message
     * @param {object} opts Options
     * @return a Promise
     */
    emit(blockName, message, opts){
    }

    /**
     * Called internally by a block whenever a message is recieved
     * @param {string} blockName Target block name
     * @param {number} concurrency The concurency, defaults to 1
     * @return a Promise
     */
    process(blockName, concurrency){
    }

    /**
     * Called internally by nStorm to clear out any residual queue data (reset the queue)
     * @return a Promise
     */
    clear(){
        return 
    }


}

module.exports = BaseQueue