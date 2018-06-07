/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
const Logger = require('./Logger.js');
const async = require("async");
const _ = require("lodash");
const os = require("os");
const util = require('util');

const AsyncQueue = require('./queue-providers/AsyncQueue')
const KueQueue = require('./queue-providers/KueQueue')
const SimpleQueue = require('./queue-providers/SimpleQueue')

//const process = require('process')
//process.setMaxListeners(Infinity)

class nStorm {

    constructor(options) {   

        var defaults = {
            cloudName: "stormcloud",
            queue: 'kue', // kue, async, featureless
            metaLogging: true,
            debug: false,
            redis: {
                port: 6379,
                host: '127.0.0.1'
            },
            replay: true,
            replayLimit: 3,
            replayTime: 300
        };

        this.settings = _.merge(defaults, options);

        this.blocks = []  

        /*
        this.queue = kue.createQueue({
            prefix: this.settings.prefix,
            redis: this.settings.redis            
        });
        */

        switch(this.settings.queue){
            case 'simple': this.queue = new SimpleQueue(this.settings); break;
            case 'kue': this.queue = new KueQueue(this.settings); break;
            default: this.queue = new AsyncQueue(this.settings); break;
        }
        
    }

    async setupTopology(){
    
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

        // Initialize the blocks and register the block with the queue provider callback
        for (let i=0; i<this.blocks.length; i+=1){
            await this.queue.registerBlock(this.blocks[i])
            await this.blocks[i]._init()
        }        

        return 
        
    }

    async start(){
    
        if (this.settings.debug){
            Logger.debug('Starting cloud')
        }

        // Call the queue providers start hook
        await this.queue.start()

        // Now call the start methods of all the blocks to get things rolling
        for (let i=0; i<this.blocks.length; i+=1){   
            await this.blocks[i].start()
        }

        if (this.settings.debug){
            Logger.debug('Starting cloud')
        }
        
        return
    }

    async reset(){    
        this.queue.clear()
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
        block.setup({name:name, limit:limit, debug:this.settings.debug}, this)

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

}

module.exports = nStorm;
