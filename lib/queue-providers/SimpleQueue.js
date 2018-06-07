const Logger = require('../Logger.js')
const BaseQueue = require('./BaseQueue')
const FJQ = require('featureless-job-queue')
const redis = require('redis')
const bluebird = require("bluebird")

// Use bluebird to Promisfy redis, see https://github.com/NodeRedis/node_redis
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

// https://www.npmjs.com/package/featureless-job-queue
//var FJQ = require('featureless-job-queue')
//var fjq = new FJQ({redisUrl: redisUrl})

class SimpleQueue extends BaseQueue {

    constructor(opts){

        super(opts)

        this.prefix = opts.cloudName
        this.queues = {}

        if (!opts.redis.user){
            opts.redis.user = ''
        }

        if (!opts.redis.password){
            opts.redis.password = ''
        }
        
        // Get a direct connection to redis
        this.client = redis.createClient(opts.redis);

        // [redis:]//[[user][:password@]][host][:port][/db-number][?db=db-number[&password=bar[&option=value]]]

        this.redisUrl = `redis://${opts.redis.host}:${opts.redis.port}/1`

        if (opts.redis.user){
            this.redisUrl = `redis://${opts.redis.user}:${opts.redis.password}@${opts.redis.host}:${opts.redis.port}/0`
        }

    }

    registerBlock(block){
        this.queues[this.prefix+'-'+block.name] = new FJQ({
            redisKey: block.name+':sq-jobs',
            redisUrl: this.redisUrl
        })
    }

    start(){

        // Setup a graceful shutdown
        process.once('SIGTERM', (sig)=>{
            this.queue.shutdown(() =>{
                if (this.settings.debug){
                    Logger.warn( 'Queue shutdown: ', err || '' );
                }
                process.exit( 0 );
            })
        })    

    }

    emit(blockName, message, opts){   

        let q = this.queues[this.prefix+'-'+blockName]
        
        if (!q){
            throw new Error(`Queue ${this.prefix+'-'+blockName} is not defined!`)
        }

        q.create(message, function(err) {
            //console.log('finished processing foo');
        });    

    }

    process(blockName, concurrency, callback){

        let q = this.queues[this.prefix+'-'+blockName]
        
        if (!q){
            throw new Error(`Queue ${this.prefix+'-'+blockName} is not defined!`)
        }

        q.process((job, done)=>{
            try {
                callback(job, done)
            }
            catch(e){
                done(e);
            }
        }, concurrency)        
  
    }

    async clear(){

        let keys = await this.client.keysAsync('*:sq-jobs:*')

        if (this.settings.debug){
            Logger.debug('Clearing queue. Deleting ' + keys.length)
        }

        for (let i=0; i<keys.length; i+=1){
            Logger.info("Deleting " + keys[i])
            await this.client.delAsync(keys[i])            
        }

        if (this.settings.debug){
            Logger.debug('Done clearing queue')
        }

        return 
/*
        return new Promise((resolve, reject) => {       

            async.map(this.queues,
                (q, cb)=>{
                    q.clearAll(cb)       
                },
                (err, results)=>{
                    if (err){
                        return reject(err)    
                    }
                    return resolve()
                })

        }) 

*/
    }
}

module.exports = SimpleQueue