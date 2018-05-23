var Logger = require('./Logger.js');

/**
 * The base class for a bolt
 */
class BaseBolt {

    constructor(opts) {    
        this.name = (opts && opts.name) ? opts.name : ''
        this.subscribers = []
        this.limit = null
        this.threads = [];         
    }

    delay(ms){
        //return new Promise(res => setTimeout(res, ms))
        return new Promise((resolve, reject) => {            
            setTimeout(()=>{
                return resolve('done')
            }, ms)
        })         
    }

    async emit(message){

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

    input(sourceName, filterOptions) {

        Logger.debug(`Adding ${sourceName}`)
        //_addSubscriber(sourceName, blockName, filterOptions);

        return this;
    }

    /**
     * {private} This function sets up the input for a block. Part of defining a topology is specifying for each
     * block which streams it should receive as input.
     *
     * Loosely based on Twitter Storm, but adapted for simplicity
     * @see https://github.com/nathanmarz/storm/wiki/Concepts
     *
     * @param blockName
     * @param filterOptions filtering options
     * @constructor
     */
    __inputDeclarer(blockName) {

        this.input = function(sourceName, filterOptions) {

            if (settings.metaLogging) {
                _updateBlockMeta(blockName, 'input', {blockName: blockName});
            }

            _addSubscriber(sourceName, blockName, filterOptions);

            return this;
        }

    }

}

module.exports = BaseBolt