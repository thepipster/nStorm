var Logger = require('arsenic-logger');
var nStorm = require('../index.js');

// Spout and Bolt implementation
var coinTossSpout = new CoinSpout();
var headsBolt = new HeadsBolt();
var tailsBolt = new TailsBolt();
var resultsBolt = new ResultsBolt();

var cloud = new nStorm({debug:false}, function(){

    // Setting up topology using the topology builder
    cloud.addBlock("coindTossSpout", coinTossSpout);

    cloud.addBlock("tailsBolt", tailsBolt, 1).input("coindTossSpout", {keys:{coin: "tails"}});
    cloud.addBlock("headsBolt", headsBolt, 1).input("coindTossSpout", {keys:{coin: "heads"}});
    cloud.addBlock("resultsBolt", resultsBolt, 1).input("tailsBolt").input("headsBolt");

    // Setup cluster, and run topology...
    //cloud.start();

});



/**
 * Demo spout that generates random data
 */
function CoinSpout() {

    var test = 56;
    this.temp = 47;

    this.start = function(context) {

        Logger.info("Starting...");
        sendData();

        function sendData(){
            
            for (var i=0; i<10; i++){

                var test = getRandomInt(0,100);

                var toss = 'tails';
                if (test < 50){
                    toss = 'heads';
                }

                var row = {coin: toss, time: Date.now()}

                Logger.debug("Emitting", row);

                context.emit(row);
            }

            setTimeout(function(){
                sendData();
            }, 1000);

        }

    }


}

function HeadsBolt() {

    this.process = function(message, context) {

        Logger.info("Heads ", message.coin);

        // Acknowledge
        context.ack(message);

        // Pass data along
        context.emit(message);

    }

}

function TailsBolt() {

    this.process = function(message, context) {

        Logger.info("Tails ", message.coin);

        // Acknowledge
        context.ack(message);

        // Pass data along
        context.emit(message);
    }

}


function ResultsBolt() {

    this.process = function(message, context) {

  //      Logger.info("Results Message ", message.coin);

        // Randomly throw an expection
        var test = getRandomInt(0,100);

        if (test < 33){
            throw new Error("Test error!!!");
            return;
        }

        // Acknowledge
        context.ack(message);

        // Pass data along
        context.emit(message);
    }

}

/**
 * Returns a random integer between min and max
 * Using Math.round() will give you a non-uniform distribution!
 */
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
