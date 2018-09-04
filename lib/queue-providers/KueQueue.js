const Logger = require('../Logger.js');
const Promise = require("bluebird");
const redis = require('redis')
// https://github.com/Automattic/kue#processing-jobs
const kue = require('kue')
const BaseQueue = require('./BaseQueue')
const process = require('process')

// Use bluebird to Promisfy redis, see https://github.com/NodeRedis/node_redis
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

//const TTL = 1 * 60 * 60 * 1000
const TTL = 10 * 60 * 1000

class KueQueue extends BaseQueue {

    constructor(opts){

        super(opts)
        
        if (this.settings.debug){
            Logger.info("Using Kue")
        }
        
        // Get a direct connection to redis
        this.client = redis.createClient(opts.redis);

        this.settings.prefix = opts.cloudName

        this.queue = kue.createQueue({
            prefix: this.settings.prefix,
            redis: opts.redis          
        });
    }

    start(){

        // Listen to the queue
        this.queue
            //.on('job enqueue', function(id, type){
            //    Logger.info(`Job ${id} got queued of type ${type}`);          
            //})
            .on('job complete', (id, result)=>{
                
                kue.Job.get(id, (err, job)=>{
                    
                    if (err) {
                        //Logger.error(err.toString())
                        return;
                    }

                    job.remove((err)=>{
                        
                        if (err) {
                            Logger.error(err)
                        }

                        if (this.settings.debug){
                            Logger.info(`removed completed job ${job.id}`);
                        }
                    })

                })
        }) 

        this.queue.watchStuckJobs(1000)

        this.queue.on( 'error', function( err ) {
            Logger.error(err);
          });        
        
        // Setup a graceful shutdown
        process.once( 'SIGTERM', (sig)=>{
            this.queue.shutdown( 5000, (err)=>{
                if (this.settings.debug){
                    Logger.warn( 'Kue shutdown: ', err||'' );
                }
                process.exit( 0 );
            })
        })    
        
        //kue.app.listen(3000);

    }

    emit(blockName, message, opts){   

        //Logger.debug(`"${this.name}" sending message to "${blockName}"`, message.coin)
        let job = this.queue
            .create(blockName, message)
            .attempts(opts.replayLimit)
            .backoff( {delay: opts.replayTime, type:'fixed'})
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

            if (this.settings.debug){

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

    process(blockName, concurrency, callback){
        this.queue.process(blockName, concurrency, async (job, done)=>{

            try {
                await callback(job.data, done)
            }
            catch (err){
                if (err){
                    done(err)
                    Logger.error("Error in process block; ", e)
                }                
            }

            /*
            try {
                callback(job.data, done)
            }
            catch(e){
                //done(e);
                Logger.error("Error in process block; ", e)
            }
            */
        });        
    }

    async clear(){

        let keys = await this.client.keysAsync(this.settings.prefix+':job:*')

        if (this.settings.debug){
            Logger.debug('Clearing queue. Deleting ' + keys.length)
        }

        return Promise.map(keys, async (key)=>{
            await this.client.delAsync(key)      
            return      
        }, {concurrency:100})               
    }

}

module.exports = KueQueue