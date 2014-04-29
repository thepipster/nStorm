var Logger = require('arsenic-logger');
var nStorm = require('../index.js');

var builder = new nStorm.TopologyBuilder();

// Spout and Bolt implementation
var coinTossSpout = new CoinSpout();
var headsBolt = new HeadsBolt();
var tailsBolt = new TailsBolt();
var resultsBolt = new ResultsBolt();

// Setting up topology using the topology builder
builder.setSpout("coindTossSpout", coinTossSpout);
builder.setBolt("tailsBolt", tailsBolt, 1).input("coindTossSpout", {coin: "tails"});
builder.setBolt("headsBolt", headsBolt, 1).input("coindTossSpout", {coin: "heads"});
builder.setBolt("resultsBolt", resultsBolt, 1).input("tailsBolt").input("headsBolt");

// Setup cluster, and run topology...

var cluster = new LocalCluster();

var clusterConfig = {
    debug: true,
    numWorkers: 2
}

//var topo = builder.createTopology();

cluster.submitTopology("test", clusterConfig, builder.createTopology());

// Give it 10 seconds, then kill
//setTimeout(function(){
//    cluster.killTopology("test");
//    cluster.shutdown();
//}, 10000);



/**
 * Demo spout that generates random data
 */
function CoinSpout() {

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
            }, 60000);

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

        Logger.info("Results Message ", message.coin);

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
