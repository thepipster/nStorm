/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */
var Logger = require('./Logger.js');
//var redisEmitter = require('redis-eventemitter');
//var redis = require("redis");
var async = require("async");
var _ = require("lodash");
var os = require("os");

class nStorm {

    constructor() {   
        this.blocks = []  
    }

    start(){
        for (let i=0; i<this.blocks.length; i+=1){
            this.blocks[i].block
                .start()
                .catch((err) => {
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

        this.blocks.push({
            name: name,
            block: block,
            limit: (limit) ? limit : 1
        })
        
        return block
    }
}

module.exports = nStorm;
