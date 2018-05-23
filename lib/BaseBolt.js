const Logger = require('./Logger.js');
const kue = require('kue')
const queue = kue.createQueue()
const _ = require('lodash')
const TTL = 300000

/**
 * The base class for a bolt
 */
class BaseBolt {

    constructor() {    
        this.inputs = []
        this.subscribers = []
        this.limit = null
        this.threads = [];                 
    }

    setup(opts, context){
        
        this.name = (opts && opts.name) ? opts.name : ''
        this.context = context

        queue.process(this.name, (job, done)=>{
            this.process(job.data, done)
        });
    }

    delay(ms){
        //return new Promise(res => setTimeout(res, ms))
        return new Promise((resolve, reject) => {            
            setTimeout(()=>{
                return resolve('done')
            }, ms)
        })         
    }

    /**
    * Check to see if any filter options have been passed. If so, check to seee
    * if the message has a corresponding key and it is of the correct value.
    */
    _isFiltered(msg, filter){

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

    /**
     * Send a message to anyone subscribe to this block
     * @param {*} message 
     */
    emit(message){

        //Logger.debug(`${this.name} has ${this.subscribers.length} subscribers`, this.subscribers)

        for (let i=0; i<this.subscribers.length; i+=1){

            // Check filters
            let filter = this.subscribers[i].options.filter
            let skip = false

            if (filter){
                if (!this._isFiltered(message, filter)){
                    skip = true
                }
            }

            if (!skip){
                Logger.debug(`"${this.name}" sending message to "${this.subscribers[i].name}"`, message.coin)
                let job = queue
                    .create(this.subscribers[i].name, message)
                    .attempts(3)
                    .backoff( {type:'exponential'} )
                    .ttl(TTL)
                    .save(function(err){
                        if (err){
                            Logger.error(err)
                        }
                        //else {
                        //    Logger.debug(`Job ${job.id} created`)
                        //}
                    })
            }

        }

    }

    async ack(message){

    }

    /**
     * Do some processing, this should be defined on the child bolt
     */
    async process(message, done){

    }

    async start(){
        
    }

    /**
     * This function sets up the input for a block. Part of defining a topology is specifying for each
     * block which streams it should receive as input.
     *
     * Loosely based on Twitter Storm, but adapted for simplicity
     * @see https://github.com/nathanmarz/storm/wiki/Concepts
     *
     * @param sourceName
     * @param options filtering options
     * @constructor
     */    
    input(sourceName, options) {

        Logger.debug(`Adding input ${sourceName}`)
        //_addSubscriber(sourceName, blockName, filterOptions);

        this.inputs.push({
            name: sourceName,
            options: options
        })

        return this;
    }


}

module.exports = BaseBolt