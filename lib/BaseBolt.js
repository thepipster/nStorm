const Logger = require('./Logger.js');
const kue = require('kue')
const queue = kue.createQueue()

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
            //email(job.data.to, done);
            this.process(job.data.to, done)
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
     * Send a message to anyone subscribe to this block
     * @param {*} message 
     */
    emit(message){

        Logger.debug(`${this.name} has ${this.subscribers.length} subscribers`, this.subscribers)
        for (let i=0; i<this.subscribers.length; i+=1){
            Logger.debug(`${this.name} sending message to ${this.subscribers[i].name}`, message)
            queue
                .create(this.subscribers[i].name, message)
                .attempts(3)
                .backoff( {type:'exponential'} )
        }

    }

    async ack(message){

    }

    /**
     * Do some processing, this should be defined on the child bolt
     */
    async process(){

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