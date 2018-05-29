/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
const Logger = require('./Logger.js');
const async = require("async");
const _ = require("lodash");
const os = require("os");
const util = require('util');
const redis = require("redis");
const bluebird = require("bluebird");
// Use bluebird to Promisfy redis, see https://github.com/NodeRedis/node_redis
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

// https://github.com/Automattic/kue#processing-jobs
const kue = require('kue')
//const queue = kue.createQueue()

class nStorm {

    constructor(options) {   

        var defaults = {
            cloudName: "stormcloud",
            metaLogging: true,
            debug: false,
            redis: {
                port: 6379,
                host: '127.0.0.1'
            },
            reset: false,
            replay: true,
            replayLimit: 3,
            replayTime: 300
        };

        this.settings = _.merge(defaults, options);

        // Set the prefix to the cloud name, so you can run multiple 'clouds' in parallel
        this.settings.prefix = this.settings.cloudName+':'

        this.queue = kue.createQueue({
            prefix: this.settings.prefix,
            redis: this.settings.redis            
        });
        
        this.blocks = []  

        // Get a direct connection to redis
        this.client = redis.createClient(this.settings.redis);
    }

    start(){
    
        // Setup subscribers...
        for (let i=0; i<this.blocks.length; i+=1){
            for (let j=0; j<this.blocks[i].inputs.length; j+=1){

                let inputBlock = this._findBlock(this.blocks[i].inputs[j].name)

                if (!inputBlock){
                    throw new Error(`For block ${this.blocks[i].name}, could not find it's source block ${inputBlock.name}`)
                }

                inputBlock.subscribers.push({
                    name: this.blocks[i].name,
                    options: this.blocks[i].inputs[j].options
                })
            }                   
        }

        this._clearQueue()
            .then(()=>{

                for (let i=0; i<this.blocks.length; i+=1){
                    this.blocks[i].start().catch((err) => {
                        Logger.error(err)
                    })
                }
                
            })
            .catch(err=>{
                Logger.error(err)
            })


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

    /**
     * Add a block to the topology
     *
     * Example usage;
     *
     * ar cloud = new nStorm()
     *
     * cloud.addBlock("coindTossSpout", coinTossSpout);
     * cloud.addBlock("headsBolt", headsBolt).input("coindTossSpout");
     *
     * Where in this case, shuffleGrouping("spout") indicates to randomly take input
     * from connected spouts (there may be only one single spout defined for this topology
     * but there maybe be multiple instances of that spout if you're using more than one
     * worker).
     *
     * @param name The name of this block, use a unique name
     * @param block The implentation of the block
     * @param limit maximum number of block functions that can run at one time *per* worker
     * @returns {InputDeclarer}
     */
    addBlock(name, block, limit) {

        if (typeof this.blocks[name] !== 'undefined'){
            throw "A block already exists with this name. Block names must be unique";
        }

        // Make sure the block is using it's name
        block.setup({name:name, limit:limit}, this)

        this.blocks.push(block)
        
        return block
    }

    async getStatus(){

        return new Promise((resolve, reject) => {            

            async.parallel(
                {
                failed: (cb)=>{this.queue.failedCount(cb)},
                active: (cb)=>{this.queue.activeCount(cb)},
                complete: (cb)=>{this.queue.completeCount(cb)},
                delayed: (cb)=>{this.queue.delayedCount(cb)},
                inactive: (cb)=>{this.queue.inactiveCount(cb)}
                },
                (err, results)=>{
                    if (err){
                        return reject(err)
                    }
                    return resolve(results)
                }
            )

        })  

    }

    _findBlock(name){
        for (let i=0; i<this.blocks.length; i+=1){
            if (this.blocks[i].name == name){
                return this.blocks[i]
            }
        }
        return null
    }
    
    async _clearQueue(){

        let keys = await this.client.keysAsync(this.settings.prefix)

        for (let i=0; i<keys.length; i+=1){
            Logger.info("Deleting " + keys[i])
            await this.client.delAsync(keys[i])            
        }

        return 

        /*

        // House keeping, remove completed jobs
        kue.Job.rangeByState('complete', 0, 10000, 'asc', (err, jobs)=>{
            jobs.forEach((job)=>{
                job.remove(()=>{
                    if (this.settings.debug){
                        Logger.debug('removed ', job.id);
                    }
                });
            });
        });

        */

    }    
}

module.exports = nStorm;
