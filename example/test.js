var Logger = require('../index.js').Logger
var nStorm = require('../index.js').nStorm
var BaseBolt = require('../index.js').BaseBolt

const startTime = new Date()

/**
 * Returns a random integer between min and max
 * Using Math.round() will give you a non-uniform distribution!
 */
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getDelta(){
    //return Math.round( ( (new Date()) - startTime) / 1000)
    return (new Date()) - startTime
}

/**
 * Demo spout that randomly creates 'coins', either heads or tails
 */
class CoinSpout extends BaseBolt {

    async start() {

        Logger.info("Starting...");

        for (var i=0; i<1000; i++){

            var test = getRandomInt(0,100);

            var toss = 'tails';
            if (test < 50){
                toss = 'heads';
            }

            var row = {coin: toss, count: i, time: Date.now(), deltaSpout: getDelta()}

            //Logger.debug("Emitting", row);

            //await this.delay(3000)

            this.emit(row);
            
        }

        return

    }


}

class HeadsBolt extends BaseBolt {

    process(message, done) {

        message.deltaHeads = Date.now() - message.time
        //Logger.info(`Heads >>>>> ${message.coin} - message.count = ${message.count}`);

        // Pass data along
        this.emit(message);

        // Acknowledge
        done()

    }

}

class TailsBolt extends BaseBolt {

    process(message, done) {

        message.deltaTails = Date.now() - message.time
        //Logger.info(`Tails >>>>> ${message.coin} - message.count = ${message.count}`);

        // Pass data along
        this.emit(message);

        // Acknowledge
        done()
    }

}

class ResultsBolt extends BaseBolt {

    constructor() {   
        super()
        this.count = 0
    }

    process(message, done) {

        message.deltaResults = Date.now() - message.time

        this.count++
        Logger.info(`Results >>>>> ${this.count} coins - message.count = ${message.count}`, message);

        // Randomly throw an expection
        var test = getRandomInt(0,100);

        if (test < 33){
          //  throw new Error("Test error!!!");
        //    return;
        }

        // Pass data along
        this.emit(message);

        // Acknowledge
        done()

        
    }

}



// Spout and Bolt implementation
var coinTossSpout = new CoinSpout();
var headsBolt = new HeadsBolt();
var tailsBolt = new TailsBolt();
var resultsBolt = new ResultsBolt();

var cloud = new nStorm({useCluster:false, debug:false});

// Setting up topology using the topology builder
cloud.addBlock("coindTossSpout", coinTossSpout);
cloud.addBlock("tailsBolt", tailsBolt, 3).input("coindTossSpout", {filter:{coin: "tails"}});
cloud.addBlock("headsBolt", headsBolt, 3).input("coindTossSpout", {filter:{coin: "heads"}});
cloud.addBlock("resultsBolt", resultsBolt, 6).input("tailsBolt").input("headsBolt");

// Setup cluster, and run topology...
cloud.start();

