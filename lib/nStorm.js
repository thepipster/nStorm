/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
const Logger = require('./Logger.js');
const async = require("async");
const _ = require("lodash");
const os = require("os");
const cluster = require('cluster')

// https://github.com/Automattic/kue#processing-jobs
const kue = require('kue')
//const queue = kue.createQueue()

class nStorm {

    constructor(options) {   

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

        this.settings = _.merge(defaults, options);

        this.queue = kue.createQueue({
            prefix: this.settings.redis.prefix,
            redis: this.settings.redis            
        });
        
        this.blocks = []  
        this._clearQueue()
    }

    start(){
        
        if (this.settings.useCluster && !cluster.isMaster) {
            return
        }

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

        for (let i=0; i<this.blocks.length; i+=1){
            this.blocks[i].start().catch((err) => {
                Logger.error(err)
            })
        }

        // Listen to the queue
        this.queue
            //.on('job enqueue', function(id, type){
            //    Logger.info(`Job ${id} got queued of type ${type}`);          
            //})
            .on('job complete', function(id, result){
                kue.Job.get(id, function(err, job){
                    if (err) return;
                    job.remove(function(err){
                        if (err) throw err;
                        Logger.info('removed completed job #%d', job.id);
                    });
                });
        });   

        this.queue.on( 'error', function( err ) {
            Logger.error( 'Oops... ', err );
          });        
        
        // Setup a graceful shutdown
        process.once( 'SIGTERM', function(sig) {
            this.queue.shutdown( 5000, function(err) {
                Logger.warn( 'Kue shutdown: ', err||'' );
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
        block.name = name
        block.limit = (limit) ? limit : 1
        
        block.setup({name:name}, this)

        this.blocks.push(block)
        
        return block
    }

    _findBlock(name){
        for (let i=0; i<this.blocks.length; i+=1){
            if (this.blocks[i].name == name){
                return this.blocks[i]
            }
        }
        return null
    }
    
    _clearQueue(){
        // House keeping, remove completed jobs
        kue.Job.rangeByState('complete', 0, 10000, 'asc', function (err, jobs) {
            jobs.forEach(function (job) {
                job.remove(function () {
                    Logger.debug('removed ', job.id);
                });
            });
        });
    }    
}

module.exports = nStorm;
