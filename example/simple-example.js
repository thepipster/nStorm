var Logger = require('arsenic-logger');
var nStorm = require('../index.js');

/**
 * Demo spout that generates random data
 */
function CoinSpout() {

    this.start = function(context) {

        sendData();

        function sendData(){
            
            var test = Math.floor(Math.random() * 101);

            var toss = 'tails';
            if (test < 50){
                toss = 'heads';
            }

            var row = {coin: toss, time: Date.now()}

            // 'Emit' the data, which is passed to any blocks listening to
            // (subscribed) to this blocks messages
            context.emit(row);

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

var builder = new TopologyBuilder();

// Spout and Bolt implementation
var coinTossSpout = new CoinSpout();
var headsBolt = new HeadsBolt();

// Setting up topology using the topology builder
builder.setSpout("coindTossSpout", coinTossSpout);
builder.setBolt("headsBolt", headsBolt).input("coindTossSpout");

var cluster = new LocalCluster();
cluster.submitTopology("test", {}, builder.createTopology());