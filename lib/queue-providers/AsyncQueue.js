const Logger = require('../Logger.js')
const async = require("async")
const BaseQueue = require('./BaseQueue')

class AsyncQueue extends BaseQueue {

    constructor(opts){
        super(opts)
        this.prefix = opts.cloudName
        this.queues = {}
    }

    /**
     * Called by a Spout/Bolt internally to send a message to a specific block
     * @param {string} blockName Target block name
     * @param {object} message The message
     * @param {object} opts Options
     * @return a Promise
     */
    emit(blockName, message, opts){
        
        let q = this.queues[this.prefix+'-'+blockName]
        
        if (!q){
            throw new Error(`Queue ${this.prefix+'-'+blockName} is not defined!`)
        }

        q.push(message, function(err) {
            //console.log('finished processing foo');
        });        
    }

    /**
     * Called internally by a block whenever a message is recieved
     * @param {string} blockName Target block name
     * @param {number} concurrency The concurency, defaults to 1
     */
    process(blockName, concurrency, callback){
        this.queues[this.prefix+'-'+blockName] = async.queue( (job, done)=>{
            try {
                callback(job, done)
            }
            catch(e){
                done(e);
                //Logger.error("Error in process block; ", e)
            }
        }, concurrency)
        Logger.debug(this.prefix+'-'+blockName + ' setup!')
    }
}

module.exports = AsyncQueue