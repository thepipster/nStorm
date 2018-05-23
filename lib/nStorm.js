/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
const Logger = require('./Logger.js');
const async = require("async");
const _ = require("lodash");
const os = require("os");

// https://github.com/Automattic/kue#processing-jobs
const kue = require('kue')
const queue = kue.createQueue()

class nStorm {

    constructor() {   
        this.blocks = []  
    }

    _findBlock(name){
        for (let i=0; i<this.blocks.length; i+=1){
            if (this.blocks[i].name == name){
                return this.blocks[i]
            }
        }
        return null
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

        for (let i=0; i<this.blocks.length; i+=1){
            this.blocks[i].start().catch((err) => {
                Logger.error(err)
            })
        }
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
}

module.exports = nStorm;
