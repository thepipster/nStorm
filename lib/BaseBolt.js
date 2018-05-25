const Logger = require('./Logger.js');
const kue = require('kue')
const _ = require('lodash')
const TTL = 1 * 60 * 60 * 1000
const cluster = require('cluster')

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
    }

    setup(opts, context){
        
        this.name = (opts && opts.name) ? opts.name : ''
        this.context = context
        this.limit = (opts && opts.limit) ? opts.limit : 1

        var clusterWorkerSize = require('os').cpus().length;

        if (this.context.settings.useCluster) {

            if (cluster.isMaster) {
                //kue.app.listen(3000);
                for (var i = 0; i < clusterWorkerSize; i++) {
                    cluster.fork();
                }
            } 
            else {                
                this.context.queue.process(this.name, this.limit, (job, done)=>{
                    try {
                        this.process(job.data, done)
                    }
                    catch(e){
                        done(e);
                        Logger.error("Error in process block; ", e)
                    }
                });
            }

        }
        else {
            this.context.queue.process(this.name, this.limit, (job, done)=>{
                try {
                    this.process(job.data, done)
                }
                catch(e){
                    done(e);
                    //Logger.error("Error in process block; ", e)
                }
            });
        }
        
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

            /*
        var defaults = {
            cloudName: "stormcloud",
            metaLogging: true,
            useCluster: false,
            debug: false,
            redis: {
                port: 6379,
                host: '127.0.0.1',
                prefix: 'nstorm-pubsub:'
            },
            reset: false,
            replay: true,
            replayLimit: 3,
            replayTime: 300
        };

            */
            if (!skip){
                //Logger.debug(`"${this.name}" sending message to "${this.subscribers[i].name}"`, message.coin)
                let job = this.context.queue
                    .create(this.subscribers[i].name, message)
                    .attempts(this.context.settings.replayLimit)
                    .backoff( {delay: this.context.settings.replayTime, type:'fixed'})
                    .ttl(TTL)
                    .removeOnComplete(true)
                    .save(function(err){
                        if (err){
                            Logger.error(err)
                        }
                        //else {
                        //    Logger.debug(`Job ${job.id} created`)
                        //}
                    })

                    job
                        .on('complete', function(result){
                            //Logger.debug('Job completed with data ', result);
                        })
                        .on('failed attempt', function(errorMessage, doneAttempts){
                            Logger.error(`Job failed, ${doneAttempts} doneAttempts. Error = ${errorMessage}`);
                        })
                        .on('failed', function(errorMessage){
                            Logger.error(`Job failed final attempt. Error = ${errorMessage}`);
                        })
                        .on('progress', function(progress, data){
                            Logger.debug(`job #${job.id} ${progress}% complete with data `, data );
                        });                    
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