const Logger = require('./Logger.js');
const _ = require('lodash')



/**
 * The base class for a bolt
 */
class BaseBolt {

    constructor() {    
        this.inputs = []
        this.subscribers = []
        this.limit = null
        this.threads = [] 
        this.context = null      
        this.debug = false         
    }

    setup(opts, context){        
        
        this.name = (opts && opts.name) ? opts.name : ''
        this.context = context
        this.limit = (opts && opts.limit) ? opts.limit : 1
        this.debug =  (opts && opts.debug) ? true : false      

        //this.queue.setup(this.name, opts)
    }

    _init(){
        
        this.context.queue.process(this.name, this.limit, (job, done)=>{

            try {
                this.process(job, done)
            }
            catch(e){
                Logger.error("Error in process block; ", e)
                done(e);
            }
            
        })
        
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
            let skip = false

            if (this.subscribers[i].options && this.subscribers[i].options.filter){
                if (!this._isFiltered(message, this.subscribers[i].options.filter)){
                    skip = true
                }
            }

            if (!skip){
                this.context.queue.emit(this.subscribers[i].name, message, this.context.settings)
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
        
        //Logger.debug(`Adding input ${sourceName}`)
        //_addSubscriber(sourceName, blockName, filterOptions);

        this.inputs.push({
            name: sourceName,
            options: options
        })

        this.inputs = _.uniq(this.inputs)

        return this;
    }


}

module.exports = BaseBolt