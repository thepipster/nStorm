var Logger = require('../index.js').Logger
var nStorm = require('../index.js').nStorm
var BaseBolt = require('../index.js').BaseBolt

/**
 * Returns a random integer between min and max
 * Using Math.round() will give you a non-uniform distribution!
 */
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Demo spout that generates random data
 */
class CoinSpout extends BaseBolt {

    async start() {

        Logger.info("Starting...");

        for (var i=0; i<200; i++){

            var test = getRandomInt(0,100);

            var toss = 'tails';
            if (test < 50){
                toss = 'heads';
            }

            var row = {coin: toss, time: Date.now()}

            //Logger.debug("Emitting", row);

            this.emit(row);

            await this.delay(1000)
        }

        return

    }


}

class HeadsBolt extends BaseBolt {

    process(message, context) {

        Logger.info("Heads ", message.coin);

        // Acknowledge
        context.ack(message);

        // Pass data along
        context.emit(message);

    }

}

class TailsBolt extends BaseBolt {

    process(message, done) {

        Logger.info("Tails >>>> ", message.coin);

        // Pass data along
        //this.emit(message);

        // Acknowledge
        done()
    }

}

class ResultsBolt extends BaseBolt {

    process(message, done) {

  //      Logger.info("Results Message ", message.coin);

        // Randomly throw an expection
        var test = getRandomInt(0,100);

        if (test < 33){
            throw new Error("Test error!!!");
            return;
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
cloud.addBlock("tailsBolt", tailsBolt, 1).input("coindTossSpout", {filter:{coin: "tails"}});
//cloud.addBlock("headsBolt", headsBolt, 1).input("coindTossSpout", {coin: "heads"});
//cloud.addBlock("resultsBolt", resultsBolt, 1).input("tailsBolt").input("headsBolt");

// Setup cluster, and run topology...
cloud.start();

